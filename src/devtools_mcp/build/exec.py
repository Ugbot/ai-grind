"""Subprocess execution + raw-output helpers shared by build runners.

Golden rule: never hand the full console dump back to the model. Runners persist
the raw output to a temp file (`write_raw`) and keep only a bounded `tail` preview
in the result; the parsed frame is the queryable surface.
"""

from __future__ import annotations

import asyncio
import tempfile
import uuid
from pathlib import Path

MAX_OUTPUT_CHARS = 8 << 20  # 8 MiB cap on captured output
TAIL_LINES = 200  # bounded preview kept in the result
TIMEOUT_RC = 124  # conventional "timed out" exit code


async def run_capture(cmd: list[str], cwd: str, timeout: int) -> tuple[int, str]:
    """Run `cmd` in `cwd`, merging stdout+stderr; return (returncode, text).

    Never raises on a non-zero exit or a timeout — the caller decides what a
    failure means for that tool. Output is capped so a pathological build can't
    blow up memory.
    """
    assert cmd, "empty command"
    assert timeout > 0, f"timeout must be positive: {timeout}"
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return TIMEOUT_RC, f"[devtools] command timed out after {timeout}s: {' '.join(cmd)}"
    text = out.decode("utf-8", "replace") if out else ""
    if len(text) > MAX_OUTPUT_CHARS:
        text = text[:MAX_OUTPUT_CHARS] + "\n[devtools] output truncated"
    rc = proc.returncode if proc.returncode is not None else -1
    return rc, text


def tail(text: str, lines: int = TAIL_LINES) -> str:
    """Last `lines` lines of `text` — a bounded preview for the result object."""
    assert isinstance(text, str), "text must be str"
    assert lines > 0, f"lines must be positive: {lines}"
    parts = text.splitlines()
    if len(parts) <= lines:
        return text
    return "\n".join(parts[-lines:])


def write_raw(prefix: str, text: str) -> str:
    """Persist full raw output to a temp file; return its path ("" if empty)."""
    assert isinstance(prefix, str) and prefix, "prefix required"
    assert isinstance(text, str), "text must be str"
    if not text:
        return ""
    path = Path(tempfile.gettempdir()) / f"{prefix}{uuid.uuid4().hex}.txt"
    path.write_text(text, encoding="utf-8")
    return str(path)
