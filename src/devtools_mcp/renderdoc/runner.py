"""Drive RenderDoc: renderdoccmd for capture/thumb, qrenderdoc --python for replay.

Replay analysis cannot import RenderDoc's python module in-process (official
builds only embed it in qrenderdoc, pinned to Python 3.6), so the replay verbs
spawn `qrenderdoc --python scripts/bridge.py`. Parameters travel via a request
JSON file referenced by env vars (the embedded interpreter has no sys.argv);
the bridge writes a response JSON and sys.exit()s before the UI opens.

Verbs:
  capture   binary=<exe>  — launch + inject; default mode `targetcontrol`
            (auto-trigger via the target-control API), `--mode launch-wait`
            falls back to renderdoccmd capture + in-app F12.
  analyze   binary=<rdc>  — action/drawcall tree (no counters: fast).
  counters  binary=<rdc>  — + GPU counter fetch (replays per pass: slow).
  resources binary=<rdc>  — + texture/buffer/shader inventory.
  thumb     binary=<rdc>  — renderdoccmd thumb (no GPU replay needed).
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import shutil
import struct
import tempfile
import time

from devtools_mcp.models import create_run_base
from devtools_mcp.renderdoc.models import (
    RenderdocCaptureResult,
    RenderdocReplayResult,
    RenderdocThumbResult,
)
from devtools_mcp.renderdoc.parsers import (
    bridge_to_replay_result,
    classify_bridge_error,
    find_new_rdcs,
    parse_bridge_json,
    parse_renderdoccmd_version,
)

TOOLS = ("capture", "analyze", "counters", "resources", "thumb")
REPLAY_TOOLS = ("analyze", "counters", "resources")

_LOG_TAIL_CHARS = 2_000
_WINDOWS_DIR = r"C:\Program Files\RenderDoc"
_POSIX_BINS = ("/usr/bin", "/usr/local/bin")


def find_renderdoccmd() -> str | None:
    """$DEVTOOLS_RENDERDOCCMD -> PATH -> default install dirs."""
    return _find("DEVTOOLS_RENDERDOCCMD", "renderdoccmd")


def find_qrenderdoc() -> str | None:
    """$DEVTOOLS_QRENDERDOC -> PATH -> default install dirs."""
    return _find("DEVTOOLS_QRENDERDOC", "qrenderdoc")


def _find(env_var: str, name: str) -> str | None:
    env = os.environ.get(env_var)
    if env and pathlib.Path(env).exists():
        return env
    on_path = shutil.which(name)
    if on_path:
        return on_path
    candidates = [os.path.join(_WINDOWS_DIR, name + ".exe")]
    candidates += [os.path.join(d, name) for d in _POSIX_BINS]
    for candidate in candidates:  # bounded: 3 candidates
        if pathlib.Path(candidate).exists():
            return candidate
    return None


async def check_renderdoc() -> dict[str, str]:
    """Detect renderdoccmd/qrenderdoc and the RenderDoc version."""
    cmd = find_renderdoccmd()
    qrd = find_qrenderdoc()
    if not cmd and not qrd:
        return {
            "installed": "false",
            "version": "",
            "path": "renderdoccmd",
            "error": (
                'RenderDoc not found. devtools_install(suite="renderdoc") shows install '
                "commands (winget install BaldurKarlsson.RenderDoc / apt install renderdoc), "
                "or set $DEVTOOLS_RENDERDOCCMD."
            ),
        }
    version = ""
    if cmd:
        code, out, _ = await _exec([cmd, "version"], timeout=30)
        if code == 0:
            version = parse_renderdoccmd_version(out)
    return {
        "installed": "true",
        "version": version,
        "path": cmd or qrd or "",
        "qrenderdoc": qrd or "",
        "renderdoccmd": cmd or "",
    }


async def _exec(argv: list[str], timeout: int, cwd: str | None = None) -> tuple[int, str, str]:
    """One subprocess invocation; returns (exit_code, stdout, stderr)."""
    assert argv, "empty argv"
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, "", f"{pathlib.Path(argv[0]).name} timed out after {timeout}s"
    return proc.returncode or 0, out.decode("utf-8", errors="replace"), err.decode("utf-8", errors="replace")


def _bridge_script() -> str:
    """Filesystem path of the packaged bridge script."""
    path = pathlib.Path(__file__).parent / "scripts" / "bridge.py"
    assert path.is_file(), f"bridge script missing from package: {path}"
    return str(path)


async def _run_bridge(request: dict, timeout: int) -> tuple[str | None, dict | None, str]:
    """Spawn qrenderdoc --python bridge.py; returns (err, payload, output_path)."""
    qrd = find_qrenderdoc()
    if not qrd:
        return (await check_renderdoc()).get("error", "qrenderdoc not found"), None, ""
    work_dir = tempfile.mkdtemp(prefix="devtools-rdoc-")
    request_path = os.path.join(work_dir, "request.json")
    output_path = os.path.join(work_dir, "output.json")
    request = dict(request, schema_version=1)
    pathlib.Path(request_path).write_text(json.dumps(request), encoding="utf-8")

    env = dict(os.environ)
    env["DEVTOOLS_RDOC_REQUEST"] = request_path
    env["DEVTOOLS_RDOC_OUTPUT"] = output_path
    proc = await asyncio.create_subprocess_exec(
        qrd,
        "--python",
        _bridge_script(),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        _, err_bytes = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return (
            f"renderdoc bridge timed out after {timeout}s — counters on large captures can take "
            "minutes; raise timeout",
            None,
            output_path,
        )
    stderr_tail = err_bytes.decode("utf-8", errors="replace")[-_LOG_TAIL_CHARS:]

    out_file = pathlib.Path(output_path)
    if not out_file.is_file():
        return (
            f"renderdoc bridge produced no output (qrenderdoc exit {proc.returncode}).\n{stderr_tail}",
            None,
            output_path,
        )
    try:
        payload = parse_bridge_json(out_file.read_text(encoding="utf-8", errors="replace"))
    except ValueError as exc:
        return f"renderdoc bridge output unreadable: {exc}\n{stderr_tail}", None, output_path
    if not payload.get("ok"):
        return classify_bridge_error(payload, stderr_tail), None, output_path
    return None, payload, output_path


def _opt(extra_args: list[str] | None, flag: str) -> str | None:
    """Value following `flag` in extra_args, if present."""
    if not extra_args or flag not in extra_args:
        return None
    i = extra_args.index(flag)
    return extra_args[i + 1] if i + 1 < len(extra_args) else None


async def run_renderdoc(
    tool: str = "analyze",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: object,
) -> tuple[str | None, RenderdocReplayResult | RenderdocCaptureResult | RenderdocThumbResult | None, str]:
    """Dispatch one renderdoc verb."""
    if tool not in TOOLS:
        return f"Unknown renderdoc tool: {tool} (one of: {', '.join(TOOLS)})", None, ""
    if not binary or not pathlib.Path(binary).exists():
        return f"target not found: {binary!r}", None, ""
    if tool == "capture":
        if binary.lower().endswith(".rdc"):
            return f"tool 'capture' takes binary=<path to .exe>; {binary} is a capture — use tool='analyze'", None, ""
        return await _run_capture(binary, args, extra_args, timeout)
    if not binary.lower().endswith(".rdc"):
        return (
            f"tool '{tool}' takes binary=<path to .rdc>; run tool='capture' first to produce one",
            None,
            "",
        )
    if tool == "thumb":
        return await _run_thumb(binary, extra_args, timeout)
    return await _run_replay(tool, binary, extra_args, timeout)


async def _run_replay(
    tool: str, rdc_path: str, extra_args: list[str] | None, timeout: int
) -> tuple[str | None, RenderdocReplayResult | None, str]:
    """analyze / counters / resources via the bridge."""
    assert tool in REPLAY_TOOLS, f"bad replay tool {tool!r}"
    max_actions = int(_opt(extra_args, "--max-actions") or 50_000)
    counter_names = [a for i, a in enumerate(extra_args or []) if i > 0 and (extra_args or [])[i - 1] == "--counter"]
    request = {
        "op": "replay",
        "rdc_path": os.path.abspath(rdc_path),
        "max_actions": max_actions,
        "want_resources": tool in ("resources", "counters"),
        "want_counters": tool == "counters",
        "counter_names": counter_names,
    }
    if tool == "counters" and timeout < 600:
        timeout = 600  # counter fetch replays the frame per pass
    start = time.monotonic()
    err, payload, output_path = await _run_bridge(request, timeout)
    if err:
        return err, None, output_path
    assert payload is not None, "bridge returned neither error nor payload"
    result = bridge_to_replay_result(payload, tool=tool, rdc_path=rdc_path, duration_seconds=time.monotonic() - start)
    return None, result, output_path


async def _run_capture(
    binary: str, args: list[str] | None, extra_args: list[str] | None, timeout: int
) -> tuple[str | None, RenderdocCaptureResult | None, str]:
    """capture via target-control bridge (default) or renderdoccmd launch-wait."""
    mode = (_opt(extra_args, "--mode") or "targetcontrol").lower()
    if mode not in ("targetcontrol", "launch-wait"):
        return f"--mode must be targetcontrol or launch-wait, got {mode!r}", None, ""
    out_dir = _opt(extra_args, "--out") or tempfile.mkdtemp(prefix="devtools-rdoc-cap-")
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    template = os.path.join(out_dir, pathlib.Path(binary).stem)
    working_dir = _opt(extra_args, "--working-dir") or str(pathlib.Path(binary).parent)
    start = time.monotonic()

    if mode == "targetcontrol":
        frame = _opt(extra_args, "--frame")
        request = {
            "op": "capture",
            "exe": os.path.abspath(binary),
            "working_dir": working_dir,
            "cmdline": " ".join(map(str, args or [])),
            "capture_file": template,
            "warmup_s": float(_opt(extra_args, "--warmup") or 3.0),
            "max_wait_s": float(_opt(extra_args, "--max-wait") or 60.0),
            "frame": int(frame) if frame is not None else None,
        }
        err, payload, output_path = await _run_bridge(request, timeout)
        if err:
            return err, None, output_path
        assert payload is not None, "bridge returned neither error nor payload"
        base = create_run_base(
            suite="renderdoc",
            tool="capture",
            binary=binary,
            args=list(map(str, args or [])),
            duration_seconds=time.monotonic() - start,
        )
        frame_captured = payload.get("frame")
        result = RenderdocCaptureResult(
            **base.model_dump(),
            mode=mode,
            rdc_paths=[str(p) for p in payload.get("rdc_paths") or []],
            frame_captured=int(frame_captured) if frame_captured is not None else None,
        )
        return None, result, output_path

    # launch-wait: renderdoccmd capture, user triggers via F12 / in-app API.
    cmd = find_renderdoccmd()
    if not cmd:
        return (await check_renderdoc()).get("error", "renderdoccmd not found"), None, ""
    launch_time = time.time()
    argv = [cmd, "capture", "-w", "-c", template, "-d", working_dir, binary, *map(str, args or [])]
    max_wait = int(_opt(extra_args, "--max-wait") or timeout)
    code, out, err_text = await _exec(argv, timeout=max_wait)
    rdcs = find_new_rdcs(out_dir, launch_time)
    base = create_run_base(
        suite="renderdoc",
        tool="capture",
        binary=binary,
        args=list(map(str, args or [])),
        duration_seconds=time.monotonic() - start,
        exit_code=code,
    )
    log_tail = (out + err_text)[-_LOG_TAIL_CHARS:].strip()
    result = RenderdocCaptureResult(
        **base.model_dump(),
        mode=mode,
        rdc_paths=[str(p) for p in rdcs],
        app_exit_code=code,
        capture_log=log_tail,
    )
    return None, result, ""


async def _run_thumb(
    rdc_path: str, extra_args: list[str] | None, timeout: int
) -> tuple[str | None, RenderdocThumbResult | None, str]:
    """thumbnail via renderdoccmd thumb — no GPU replay required."""
    cmd = find_renderdoccmd()
    if not cmd:
        return (await check_renderdoc()).get("error", "renderdoccmd not found"), None, ""
    fmt = (_opt(extra_args, "--format") or "png").lower()
    if fmt not in ("png", "jpg", "bmp", "tga"):
        return f"--format must be png/jpg/bmp/tga, got {fmt!r}", None, ""
    out_path = _opt(extra_args, "--out") or str(pathlib.Path(rdc_path).with_suffix("." + fmt))
    code, out, err_text = await _exec([cmd, "thumb", "-f", fmt, "-o", out_path, rdc_path], timeout=timeout)
    if code != 0 or not pathlib.Path(out_path).is_file():
        tail = (out + err_text).strip().splitlines()[-3:]
        return f"renderdoccmd thumb failed (exit {code}): " + " | ".join(tail), None, ""
    width, height = _png_dimensions(out_path) if fmt == "png" else (0, 0)
    base = create_run_base(suite="renderdoc", tool="thumb", binary=rdc_path)
    result = RenderdocThumbResult(
        **base.model_dump(), rdc_path=rdc_path, thumb_path=out_path, width=width, height=height
    )
    return None, result, out_path


def _png_dimensions(path: str) -> tuple[int, int]:
    """Width/height from a PNG IHDR header; (0, 0) if unreadable."""
    try:
        with open(path, "rb") as f:
            header = f.read(24)
    except OSError:
        return 0, 0
    if len(header) < 24 or header[:8] != b"\x89PNG\r\n\x1a\n":
        return 0, 0
    width, height = struct.unpack(">II", header[16:24])
    assert width < 1_000_000 and height < 1_000_000, "implausible PNG dimensions"
    return int(width), int(height)
