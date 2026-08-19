"""lldb-dap adapter: native C/C++/Rust/Zig debugging via LLVM's lldb-dap.

Spawns the `lldb-dap` binary over stdio. Discovery order: the
DEVTOOLS_LLDB_DAP env var, PATH, `xcrun -f lldb-dap` on macOS (Xcode CLT),
then versioned Linux installs (/usr/bin/lldb-dap-NN, /usr/lib/llvm-NN/bin).
Attach supports PID and waitFor natively; host:port goes through a
`gdb-remote` attachCommands sequence because plain lldb-dap has no socket
attach of its own.
"""

from __future__ import annotations

import asyncio
import glob
import os
import re
import shutil
import sys

from devtools_mcp.debug.adapters.base import (
    AdapterQuirks,
    AdapterSpec,
    is_native_binary,
    register_adapter,
)
from devtools_mcp.debug.models import AttachConfig, LaunchConfig
from devtools_mcp.debug.protocol import StdioTransport
from devtools_mcp.registry import InstalledTool, InstallSpec, InstallStep

_VERSION_TIMEOUT = 10.0
_LINUX_GLOBS = ("/usr/bin/lldb-dap-*", "/usr/lib/llvm-*/bin/lldb-dap")
_VERSION_RE = re.compile(r"version\s+([\d][\w.\-]*)", re.IGNORECASE)


def _versioned_linux_binary() -> str:
    """Newest versioned lldb-dap from the usual Linux LLVM install spots."""
    candidates: list[str] = []
    for pattern in _LINUX_GLOBS:
        candidates.extend(path for path in glob.glob(pattern) if os.access(path, os.X_OK))

    def _version_key(path: str) -> int:
        digits = re.findall(r"\d+", path)
        return int(digits[-1]) if digits else 0

    candidates.sort(key=_version_key, reverse=True)
    return candidates[0] if candidates else ""


async def find_lldb_dap() -> str:
    """Resolve the lldb-dap binary: env override > PATH > xcrun > versioned."""
    explicit = os.environ.get("DEVTOOLS_LLDB_DAP", "")
    if explicit:
        return explicit
    on_path = shutil.which("lldb-dap")
    if on_path:
        return on_path
    if sys.platform == "darwin":
        try:
            proc = await asyncio.create_subprocess_exec(
                "xcrun",
                "-f",
                "lldb-dap",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=_VERSION_TIMEOUT)
            if proc.returncode == 0 and out.decode().strip():
                return out.decode().strip()
        except (OSError, TimeoutError):
            pass
    if sys.platform.startswith("linux"):
        return _versioned_linux_binary()
    return ""


async def _lldb_dap_version(path: str) -> str:
    """Version string from `lldb-dap --version`, or '' if it won't run.

    Output shape varies: Apple/Homebrew builds print a banner line then
    '  LLVM version 21.0.0'; some builds print 'lldb version N' first.
    Scan for the first line carrying a version, fall back to line one.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            path,
            "--version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=_VERSION_TIMEOUT)
    except (OSError, TimeoutError):
        return ""
    if proc.returncode != 0:
        return ""
    lines = [line.strip() for line in out.decode(errors="replace").splitlines() if line.strip()]
    for line in lines:
        match = _VERSION_RE.search(line)
        if match:
            return match.group(1)
    return lines[0] if lines else ""


async def make_transport(config: LaunchConfig | AttachConfig) -> StdioTransport:
    path = await find_lldb_dap()
    if not path:
        raise RuntimeError(
            "lldb-dap not found. Install LLVM/lldb (devtools_install suite='debug' shows "
            "per-OS commands) or point DEVTOOLS_LLDB_DAP at the binary."
        )
    transport = StdioTransport([path])
    await transport.start()
    return transport


def launch_template(config: LaunchConfig) -> dict:
    args: dict[str, object] = {
        "name": "devtools-mcp",
        "type": "lldb-dap",
        "request": "launch",
        "program": config.program,
        "args": config.args,
        "stopOnEntry": config.stop_on_entry,
    }
    if config.cwd:
        args["cwd"] = config.cwd
    if config.env:
        # lldb-dap documents env as a list of "VAR=VALUE" strings.
        args["env"] = [f"{key}={value}" for key, value in config.env.items()]
    for key, alias in (("initCommands", "init_commands"), ("preRunCommands", "pre_run_commands")):
        commands = config.extra.get(key) or config.extra.get(alias)
        if commands:
            args[key] = commands
    return args


def attach_template(config: AttachConfig) -> dict:
    args: dict[str, object] = {
        "name": "devtools-mcp",
        "type": "lldb-dap",
        "request": "attach",
    }
    if config.program:
        args["program"] = config.program  # helps lldb resolve breakpoints
    if config.pid is not None:
        args["pid"] = config.pid
    elif config.port is not None:
        # Plain lldb-dap has no host:port attach; route through gdb-remote.
        host = config.host or "127.0.0.1"
        args["attachCommands"] = [f"gdb-remote {host}:{config.port}"]
    elif config.extra.get("wait_for"):
        if not config.program:
            raise ValueError("lldb-dap waitFor attach needs program= (the binary to wait for)")
        args["waitFor"] = True
    else:
        raise ValueError(
            "lldb-dap attach needs pid=, host=/port= (gdb-remote), " "or program= with extra={'wait_for': True}"
        )
    return args


async def detect() -> InstalledTool:
    path = await find_lldb_dap()
    version = await _lldb_dap_version(path) if path else ""
    return InstalledTool(
        suite="debug",
        name="lldb-dap",
        path=path if version else "",
        version=version,
        available=bool(version),
    )


def sniff(program: str) -> int:
    if is_native_binary(program):
        return 80
    return 0


_INSTALL = InstallSpec(
    platforms={
        "darwin": [
            InstallStep(
                kind="shell",
                argv=["xcode-select", "--install"],
                description="Xcode CLT ships lldb-dap (Xcode 16+)",
            ),
            InstallStep(
                kind="brew",
                argv=["brew", "install", "llvm"],
                description="Homebrew LLVM (lldb-dap lives in $(brew --prefix llvm)/bin, add it to PATH)",
            ),
        ],
        "linux": [
            InstallStep(
                kind="apt",
                argv=["apt-get", "install", "-y", "lldb"],
                description="LLDB suite (binary may be versioned, e.g. lldb-dap-18)",
                elevation=True,
            ),
        ],
        "windows": [
            InstallStep(
                kind="winget",
                argv=["winget", "install", "LLVM.LLVM"],
                description="LLVM toolchain (includes lldb-dap)",
            ),
        ],
    },
    note="If discovery misses your install, set DEVTOOLS_LLDB_DAP to the lldb-dap binary path.",
    url="https://github.com/llvm/llvm-project/blob/main/lldb/tools/lldb-dap/README.md",
)


def _register() -> None:
    register_adapter(
        AdapterSpec(
            name="lldb-dap",
            languages=("c", "cpp", "c++", "rust", "zig", "native"),
            transport=make_transport,
            launch_template=launch_template,
            attach_template=attach_template,
            detect=detect,
            sniff=sniff,
            install=_INSTALL,
            quirks=AdapterQuirks(supports_attach_pid=True),
            description="C/C++/Rust/Zig native debugging via lldb-dap (LLVM)",
        )
    )


_register()
