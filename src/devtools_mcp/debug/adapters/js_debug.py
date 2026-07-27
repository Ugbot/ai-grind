"""js-debug adapter: JavaScript/TypeScript (Node.js) debugging via vscode-js-debug.

vscode-js-debug ships a standalone DAP server (dapDebugServer.js). We spawn
`node dapDebugServer.js <port> 127.0.0.1` ONCE per session tree and open one
TCP connection per DAP session: the first connection is the root session;
js-debug then issues startDebugging reverse requests, and every child session
opens a NEW connection to the SAME port (the child configuration is passed
through verbatim as its launch/attach arguments). The running server rides
along in the config's `extra` dict — children reuse the parent's config
object (see dap_session.spawn_child) — refcounted per open socket and
terminated when the last socket closes.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import socket
from collections import deque

from devtools_mcp.debug.adapters.base import (
    AdapterQuirks,
    AdapterSpec,
    register_adapter,
)
from devtools_mcp.debug.models import AttachConfig, LaunchConfig
from devtools_mcp.debug.protocol import SocketTransport
from devtools_mcp.registry import InstalledTool, InstallSpec, InstallStep

PINNED_VERSION = "1.117.0"

# install.py's run_steps/_download do NOT expand '~' (argv is exec'd directly,
# dest goes straight to pathlib.Path) — expand here, at spec-build time.
_ROOT = os.path.expanduser(os.path.join("~", ".devtools-mcp", "adapters", "js-debug"))
_TARBALL = os.path.join(_ROOT, "js-debug-dap.tar.gz")
_RELEASE_URL = (
    "https://github.com/microsoft/vscode-js-debug/releases/download/"
    f"v{PINNED_VERSION}/js-debug-dap-v{PINNED_VERSION}.tar.gz"
)

_SERVER_KEY = "_js_debug_server"  # private slot in config.extra for the shared server
_SERVER_READY_TIMEOUT = 15.0
_SERVER_OUTPUT_TAIL = 20
_SNIFF_SUFFIXES = (".js", ".mjs", ".cjs", ".ts")


def server_js_path() -> str:
    """dapDebugServer.js location: $DEVTOOLS_JS_DEBUG else the install root."""
    explicit = os.environ.get("DEVTOOLS_JS_DEBUG", "")
    if explicit:
        return os.path.expanduser(explicit)
    return os.path.join(_ROOT, "js-debug", "src", "dapDebugServer.js")


def _free_port() -> int:
    """Ask the OS for a free TCP port (bind to 0, read, close)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    assert port > 0, f"bad free port {port}"
    return port


class JsDebugServer:
    """One `node dapDebugServer.js <port> 127.0.0.1` process, shared by every
    DAP session of one session tree. Refcounted: each transport acquire()s on
    open and release()s on close; the node process dies when the count hits 0.
    """

    def __init__(self, node: str, server_js: str) -> None:
        assert node and server_js, "node executable and server path required"
        self.node = node
        self.server_js = server_js
        self.port = 0
        self.process: asyncio.subprocess.Process | None = None
        self._refs = 0
        self._tail: deque[str] = deque(maxlen=_SERVER_OUTPUT_TAIL)
        self._drain_task: asyncio.Task | None = None

    def alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    def output_tail(self) -> str:
        return "\n".join(self._tail)

    async def start(self) -> None:
        assert self.process is None, "js-debug server already started"
        self.port = _free_port()
        self.process = await asyncio.create_subprocess_exec(
            self.node,
            self.server_js,
            str(self.port),
            "127.0.0.1",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            await asyncio.wait_for(self._await_ready(), timeout=_SERVER_READY_TIMEOUT)
        except TimeoutError:
            await self._kill()
            raise RuntimeError(
                f"js-debug DAP server did not start listening within {_SERVER_READY_TIMEOUT}s "
                f"(node {self.node}, server {self.server_js}).\n{self.output_tail()}"
            ) from None
        self._drain_task = asyncio.ensure_future(self._drain())

    async def _await_ready(self) -> None:
        """Consume stdout until the 'Debug server listening at ...' line."""
        assert self.process is not None and self.process.stdout is not None
        while True:
            line = await self.process.stdout.readline()
            if not line:
                raise RuntimeError(
                    f"js-debug DAP server exited before listening "
                    f"(exit code {self.process.returncode}).\n{self.output_tail()}"
                )
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                self._tail.append(text)
            if "listening" in text.lower():
                return

    async def _drain(self) -> None:
        """Keep the stdout pipe from filling; retain a tail for diagnostics."""
        assert self.process is not None and self.process.stdout is not None
        try:
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    return
                self._tail.append(line.decode("utf-8", errors="replace").rstrip())
        except (asyncio.CancelledError, OSError):
            return

    def acquire(self) -> None:
        assert self.alive(), "acquire on a dead js-debug server"
        self._refs += 1

    async def release(self) -> None:
        assert self._refs > 0, "js-debug server refcount underflow"
        self._refs -= 1
        if self._refs == 0:
            await self._kill()

    async def _kill(self) -> None:
        if self._drain_task is not None:
            self._drain_task.cancel()
            self._drain_task = None
        if self.process is None or self.process.returncode is not None:
            return
        try:
            self.process.terminate()
            await asyncio.wait_for(self.process.wait(), timeout=3.0)
        except (TimeoutError, ProcessLookupError):
            with contextlib.suppress(ProcessLookupError):
                self.process.kill()


class JsDebugSocketTransport(SocketTransport):
    """SocketTransport that holds one refcount on the shared JsDebugServer
    and releases it on close (killing node when the last socket closes)."""

    def __init__(self, server: JsDebugServer) -> None:
        super().__init__("127.0.0.1", server.port)
        self._server = server
        self._released = False

    def stderr_tail(self) -> str:
        return self._server.output_tail()

    def returncode(self) -> int | None:
        process = self._server.process
        return process.returncode if process is not None else None

    async def close(self) -> None:
        await super().close()
        if not self._released:
            self._released = True
            await self._server.release()


async def make_transport(config: LaunchConfig | AttachConfig) -> JsDebugSocketTransport:
    """Root call: spawn the DAP server, stash it in config.extra. Child calls
    (spawn_child reuses the parent's config object) find it there and just
    open a fresh socket to the same port."""
    server = config.extra.get(_SERVER_KEY)
    if not isinstance(server, JsDebugServer) or not server.alive():
        node = shutil.which("node")
        if not node:
            raise RuntimeError("node not found on PATH — js-debug needs Node.js 18+ installed")
        server_js = server_js_path()
        if not os.path.isfile(server_js):
            raise RuntimeError(
                f"js-debug is not installed: {server_js} does not exist. "
                "Install it via devtools_install(suite='debug', tool='js-debug') "
                "or point $DEVTOOLS_JS_DEBUG at an existing dapDebugServer.js."
            )
        server = JsDebugServer(node, server_js)
        await server.start()
        config.extra[_SERVER_KEY] = server
    transport = JsDebugSocketTransport(server)
    server.acquire()
    try:
        await transport.start()
    except Exception:
        await server.release()
        raise
    return transport


def launch_template(config: LaunchConfig) -> dict:
    args: dict[str, object] = {
        "type": "pwa-node",
        "request": "launch",
        "name": "devtools-mcp",
        "program": config.program,
        "args": config.args,
        "cwd": config.cwd or os.path.dirname(os.path.abspath(config.program)),
        "console": "internalConsole",
        "stopOnEntry": config.stop_on_entry,
        "outputCapture": "std",
    }
    if config.env:
        args["env"] = config.env
    runtime_args = config.extra.get("runtime_args")
    if runtime_args:
        args["runtimeArgs"] = [str(a) for a in runtime_args]  # type: ignore[union-attr]
    node = config.extra.get("node")
    if node:
        args["runtimeExecutable"] = str(node)
    return args


def attach_template(config: AttachConfig) -> dict:
    args: dict[str, object] = {
        "type": "pwa-node",
        "request": "attach",
        "name": "devtools-mcp",
    }
    if config.port is not None:
        # Attach to a process started with `node --inspect[-brk]` (default 9229).
        args["port"] = config.port
        args["address"] = config.host or "127.0.0.1"
    elif config.pid is not None:
        args["processId"] = str(config.pid)
        args["attachExistingChildren"] = True
    else:
        raise ValueError("js-debug attach needs port= (node --inspect, default 9229) or pid=")
    return args


def _installed_version() -> str:
    """Version of the installed js-debug: the VERSION file the install steps
    write next to the extracted tree (the release tarball itself carries no
    package.json), else js-debug/package.json if one exists, else the pin."""
    js_debug_dir = os.path.dirname(os.path.dirname(server_js_path()))
    try:
        with open(os.path.join(js_debug_dir, "VERSION"), encoding="utf-8") as fh:
            version = fh.read().strip()
    except OSError:
        version = ""
    if not version:
        try:
            with open(os.path.join(js_debug_dir, "package.json"), encoding="utf-8") as fh:
                version = str(json.load(fh).get("version", ""))
        except (OSError, ValueError):
            version = ""
    return version or PINNED_VERSION


async def detect() -> InstalledTool:
    node = shutil.which("node") or ""
    server_js = server_js_path()
    server_present = os.path.isfile(server_js)
    available = bool(node) and server_present
    if available:
        path = server_js
    elif not node:
        path = f"node not on PATH (Node.js 18+ required); DAP server expected at {server_js}"
    else:
        path = f"missing {server_js} — install via devtools_install(suite='debug', tool='js-debug')"
    return InstalledTool(
        suite="debug",
        name="js-debug",
        path=path,
        version=_installed_version() if available else "",
        available=available,
    )


def sniff(program: str) -> int:
    if program.endswith(_SNIFF_SUFFIXES):
        return 85
    return 0


def _steps() -> list[InstallStep]:
    return [
        InstallStep(
            kind="download",
            argv=[_RELEASE_URL, _TARBALL],
            description=f"Download js-debug DAP server v{PINNED_VERSION}",
        ),
        InstallStep(
            kind="shell",
            argv=["tar", "-xzf", _TARBALL, "-C", _ROOT],
            description=f"Extract into {_ROOT}",
        ),
        InstallStep(
            kind="shell",
            # node is a prerequisite anyway, and this is portable to Windows
            # (forward slashes are fine for Node's fs). The tarball has no
            # package.json, so record the pinned version for detect().
            argv=[
                "node",
                "-e",
                "require('fs').writeFileSync("
                f"{json.dumps(_ROOT.replace(os.sep, '/') + '/js-debug/VERSION')}, "
                f"{json.dumps(PINNED_VERSION)})",
            ],
            description=f"Record installed version {PINNED_VERSION}",
        ),
    ]


_INSTALL = InstallSpec(
    platforms={"darwin": _steps(), "linux": _steps(), "windows": _steps()},
    note=(
        "Requires Node.js 18+ on PATH (node runs the DAP server). "
        "To use an existing install, set DEVTOOLS_JS_DEBUG to a dapDebugServer.js path."
    ),
    url="https://github.com/microsoft/vscode-js-debug",
)


def _register() -> None:
    register_adapter(
        AdapterSpec(
            name="js-debug",
            languages=("javascript", "typescript", "node", "js"),
            transport=make_transport,
            launch_template=launch_template,
            attach_template=attach_template,
            detect=detect,
            sniff=sniff,
            install=_INSTALL,
            quirks=AdapterQuirks(
                multi_session=True,
                supports_attach_socket=True,
                supports_attach_pid=True,
            ),
            description=(
                "JavaScript/TypeScript (vscode-js-debug) — launch Node programs, "
                "attach to a --inspect port or PID; one child session per process"
            ),
        )
    )


_register()
