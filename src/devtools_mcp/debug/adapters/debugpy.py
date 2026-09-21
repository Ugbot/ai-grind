"""debugpy adapter: Python debugging.

Spawns `<python> -m debugpy.adapter` over stdio. The interpreter matters
twice: it runs the adapter AND must be able to import debugpy, and for
launch, debugpy injects itself into the same interpreter that runs the
program. We therefore resolve the target project's interpreter (venv
layouts, then the server's own) and require debugpy importable there.
"""

from __future__ import annotations

import asyncio
import os
import sys

from devtools_mcp.debug.adapters.base import (
    AdapterQuirks,
    AdapterSpec,
    find_project_file,
    register_adapter,
)
from devtools_mcp.debug.models import AttachConfig, LaunchConfig
from devtools_mcp.debug.protocol import StdioTransport
from devtools_mcp.registry import InstalledTool, InstallSpec, InstallStep

_VENV_BIN = "Scripts" if sys.platform == "win32" else "bin"
_VENV_PY = "python.exe" if sys.platform == "win32" else "python"


def resolve_python(config: LaunchConfig | AttachConfig) -> str:
    """Interpreter for the adapter/debuggee: explicit > project venv > server's."""
    explicit = str(config.extra.get("python", ""))
    if explicit:
        return explicit
    anchor = config.cwd if isinstance(config, LaunchConfig) and config.cwd else ""
    if not anchor and isinstance(config, LaunchConfig):
        anchor = config.program
    if anchor:
        for venv_dir in (".venv", "venv"):
            marker = find_project_file(anchor, (os.path.join(venv_dir, _VENV_BIN, _VENV_PY),))
            if marker:
                return marker
    return sys.executable


async def _debugpy_version(python: str) -> str:
    """debugpy version importable by `python`, or '' if absent."""
    try:
        proc = await asyncio.create_subprocess_exec(
            python,
            "-c",
            "import debugpy; print(debugpy.__version__)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=15.0)
    except (OSError, TimeoutError):
        return ""
    return out.decode().strip() if proc.returncode == 0 else ""


async def make_transport(config: LaunchConfig | AttachConfig) -> StdioTransport:
    python = resolve_python(config)
    version = await _debugpy_version(python)
    if not version:
        raise RuntimeError(
            f"debugpy is not importable by {python}. Install it into that interpreter: "
            f"`{python} -m pip install debugpy` (the adapter and the debuggee share it)."
        )
    transport = StdioTransport([python, "-m", "debugpy.adapter"])
    await transport.start()
    return transport


def launch_template(config: LaunchConfig) -> dict:
    args: dict[str, object] = {
        "type": "python",
        "request": "launch",
        "name": "devtools-mcp",
        "program": config.program,
        "args": config.args,
        "console": "internalConsole",  # avoids the runInTerminal dance
        "justMyCode": bool(config.extra.get("just_my_code", False)),
        "redirectOutput": True,
        "stopOnEntry": config.stop_on_entry,
    }
    if config.cwd:
        args["cwd"] = config.cwd
    if config.env:
        args["env"] = config.env
    python = resolve_python(config)
    if python:
        args["python"] = python
    if config.extra.get("module"):
        # Debug `python -m <module>` instead of a file.
        args.pop("program")
        args["module"] = config.extra["module"]
    return args


def attach_template(config: AttachConfig) -> dict:
    args: dict[str, object] = {
        "type": "python",
        "request": "attach",
        "name": "devtools-mcp",
        "justMyCode": bool(config.extra.get("just_my_code", False)),
    }
    if config.port is not None:
        # Attach to a process started with `python -m debugpy --listen host:port`.
        args["connect"] = {"host": config.host or "127.0.0.1", "port": config.port}
    elif config.pid is not None:
        # Inject into an arbitrary running Python process.
        args["processId"] = config.pid
    else:
        raise ValueError("debugpy attach needs port= (debugpy --listen) or pid=")
    return args


async def detect() -> InstalledTool:
    version = await _debugpy_version(sys.executable)
    return InstalledTool(
        suite="debug",
        name="debugpy",
        path=sys.executable if version else "",
        version=version,
        available=bool(version),
    )


def sniff(program: str) -> int:
    if program.endswith(".py"):
        return 90
    return 0


_INSTALL = InstallSpec(
    platforms={
        "darwin": [InstallStep(kind="pip", argv=["pip", "install", "debugpy"], description="Install debugpy")],
        "linux": [InstallStep(kind="pip", argv=["pip", "install", "debugpy"], description="Install debugpy")],
        "windows": [InstallStep(kind="pip", argv=["pip", "install", "debugpy"], description="Install debugpy")],
    },
    note="debugpy must be importable by the interpreter that runs the target (activate its venv first).",
    url="https://github.com/microsoft/debugpy",
)


def _register() -> None:
    register_adapter(
        AdapterSpec(
            name="debugpy",
            languages=("python",),
            transport=make_transport,
            launch_template=launch_template,
            attach_template=attach_template,
            detect=detect,
            sniff=sniff,
            install=_INSTALL,
            quirks=AdapterQuirks(supports_attach_pid=True, supports_attach_socket=True),
            description="Python (debugpy), launch scripts/modules, attach by PID or debugpy --listen port",
        )
    )


_register()
