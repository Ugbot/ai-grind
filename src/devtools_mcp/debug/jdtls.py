"""Minimal LSP client hosting eclipse.jdt.ls with the microsoft/java-debug bundle.

The java debug adapter is not a standalone process: com.microsoft.java.debug
is an Eclipse plugin that runs INSIDE the jdt.ls language server and is asked
via LSP workspace/executeCommand, to start a DAP server, answering with a
TCP port. This module owns that lifecycle: boot jdt.ls over stdio, do the
initialize dance (passing the debug plugin jar as a bundle), wait until the
workspace is imported, then expose exactly the operations the java adapter
needs:

- start_debug_session() -> int          (the DAP server port; idempotent,
  java-debug runs one shared ServerSocket, one session per TCP connection)
- resolve_main_class(project_root)      (vscode.java.resolveMainClass)
- resolve_classpath(main_class, name)   (vscode.java.resolveClasspath)

LSP uses the same Content-Length framing as DAP, so the wire helpers are
reused from debug.protocol (encode_message/read_message). No framing code
is duplicated here. Clients are cached per project root (bounded, LRU) since
booting jdt.ls and importing a project costs 10-60s.
"""

from __future__ import annotations

import asyncio
import contextlib
import glob
import hashlib
import os
import pathlib
import platform
import shutil
import sys
from collections import deque
from typing import Any

from devtools_mcp.debug.protocol import encode_message, read_message

# Bounds (Tiger Style: everything bounded, fail loud on overflow).
MAX_LOG_LINES = 200
MAX_LOG_LINE_CHARS = 400
MAX_PENDING_REQUESTS = 64
MAX_CACHED_CLIENTS = 4
READY_TIMEOUT = 120.0  # first Gradle/Maven import is slow
REQUEST_TIMEOUT = 60.0
_READY_POLL_INTERVAL = 15.0
_SHUTDOWN_TIMEOUT = 10.0
_LOG_TAIL_LINES = 20

# install.py does NOT expanduser() paths, expand here, at import time.
_HOME = os.path.expanduser(os.path.join("~", ".devtools-mcp", "adapters", "jdtls"))
_WORKSPACES = os.path.expanduser(os.path.join("~", ".devtools-mcp", "jdtls-workspaces"))


class JdtlsError(Exception):
    """jdt.ls failed: not installed, died, timed out, or answered with an error."""


def jdtls_home() -> str:
    """Install root: $DEVTOOLS_MCP_JDTLS_HOME else ~/.devtools-mcp/adapters/jdtls.

    The legacy ``DEVTOOLS_JDTLS_HOME`` name is still honored as a fallback for
    back-compat; prefer the DEVTOOLS_MCP_-prefixed name (matches every other
    devtools-mcp env var).
    """
    explicit = os.environ.get("DEVTOOLS_MCP_JDTLS_HOME", "") or os.environ.get("DEVTOOLS_JDTLS_HOME", "")
    return os.path.expanduser(explicit) if explicit else _HOME


def find_launcher(home: str = "") -> str:
    """The equinox launcher jar inside the jdt.ls distribution, or ''."""
    jars = sorted(glob.glob(os.path.join(home or jdtls_home(), "plugins", "org.eclipse.equinox.launcher_*.jar")))
    return jars[-1] if jars else ""


def find_config_dir(home: str = "") -> str:
    """The platform config dir of the jdt.ls distribution (arm variant preferred), or ''."""
    root = home or jdtls_home()
    if sys.platform == "win32":
        names = ("config_win",)
    elif sys.platform == "darwin":
        names = ("config_mac_arm", "config_mac") if platform.machine() == "arm64" else ("config_mac",)
    else:
        arm = platform.machine() in ("arm64", "aarch64")
        names = ("config_linux_arm", "config_linux") if arm else ("config_linux",)
    for name in names:
        candidate = os.path.join(root, name)
        if os.path.isdir(candidate):
            return candidate
    return ""


def find_debug_plugin(home: str = "") -> str:
    """The com.microsoft.java.debug.plugin jar in the install root, or ''."""
    jars = sorted(glob.glob(os.path.join(home or jdtls_home(), "com.microsoft.java.debug.plugin-*.jar")))
    return jars[-1] if jars else ""


def workspace_dir(project_root: str) -> str:
    """Per-project jdt.ls workspace (-data): keyed by a hash of the root path."""
    digest = hashlib.sha1(os.path.abspath(project_root).encode("utf-8")).hexdigest()[:12]
    return os.path.join(_WORKSPACES, digest)


class JdtlsClient:
    """One jdt.ls process bound to one project root, spoken to over stdio LSP."""

    def __init__(self, project_root: str) -> None:
        assert project_root, "project_root must not be empty"
        self.project_root = os.path.abspath(os.path.expanduser(project_root))
        self.process: asyncio.subprocess.Process | None = None
        self._pending: dict[int, asyncio.Future] = {}
        self._next_id = 0
        self._log: deque[str] = deque(maxlen=MAX_LOG_LINES)
        self._ready = asyncio.Event()
        self._pump_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._write_lock = asyncio.Lock()
        self._closed = False

    # -- lifecycle -----------------------------------------------------------

    def alive(self) -> bool:
        return not self._closed and self.process is not None and self.process.returncode is None

    def log_tail(self, lines: int = _LOG_TAIL_LINES) -> str:
        """Bounded recent server chatter (status/log messages) for diagnostics."""
        return "\n".join(list(self._log)[-lines:])

    def _log_line(self, line: str) -> None:
        line = line.strip()
        if line:
            self._log.append(line[:MAX_LOG_LINE_CHARS])

    def _spawn_argv(self, java: str, launcher: str, config_dir: str) -> list[str]:
        return [
            java,
            "-Declipse.application=org.eclipse.jdt.ls.core.id1",
            "-Dosgi.bundles.defaultStartLevel=4",
            "-Declipse.product=org.eclipse.jdt.ls.core.product",
            "-Xmx1G",
            "--add-modules=ALL-SYSTEM",
            "--add-opens",
            "java.base/java.util=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.lang=ALL-UNNAMED",
            "-jar",
            launcher,
            "-configuration",
            config_dir,
            "-data",
            workspace_dir(self.project_root),
        ]

    async def start(self) -> None:
        """Spawn jdt.ls, run the LSP initialize dance, wait for readiness."""
        assert self.process is None, "jdt.ls client already started"
        java = shutil.which("java")
        if not java:
            raise JdtlsError("`java` not found on PATH, jdt.ls needs a JDK 17+")
        home = jdtls_home()
        launcher = find_launcher(home)
        config_dir = find_config_dir(home)
        plugin_jar = find_debug_plugin(home)
        if not launcher or not config_dir or not plugin_jar:
            raise JdtlsError(
                f"jdt.ls install incomplete under {home}: "
                f"launcher={'ok' if launcher else 'MISSING plugins/org.eclipse.equinox.launcher_*.jar'}, "
                f"config={'ok' if config_dir else 'MISSING config_<os> dir'}, "
                f"java-debug={'ok' if plugin_jar else 'MISSING com.microsoft.java.debug.plugin-*.jar'}. "
                "Install via devtools_install(suite='debug', tool='java')."
            )
        os.makedirs(workspace_dir(self.project_root), exist_ok=True)
        self.process = await asyncio.create_subprocess_exec(
            *self._spawn_argv(java, launcher, config_dir),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.project_root,
        )
        assert self.process.stdout is not None and self.process.stdin is not None
        self._pump_task = asyncio.ensure_future(self._pump())
        self._stderr_task = asyncio.ensure_future(self._drain_stderr())
        root_uri = pathlib.Path(self.project_root).as_uri()
        await self.request(
            "initialize",
            {
                "processId": os.getpid(),
                "clientInfo": {"name": "devtools-mcp"},
                "rootUri": root_uri,
                "capabilities": {
                    "workspace": {
                        "executeCommand": {"dynamicRegistration": True},
                        "configuration": True,
                        "workspaceFolders": True,
                    },
                    "window": {"workDoneProgress": True},
                },
                "initializationOptions": {"bundles": [plugin_jar]},
                "workspaceFolders": [{"uri": root_uri, "name": os.path.basename(self.project_root) or "project"}],
                "trace": "off",
            },
        )
        await self.notify("initialized", {})
        await self._wait_ready()

    async def _wait_ready(self) -> None:
        """Block until jdt.ls reports ServiceReady on language/status, with a
        bounded fallback poll of the debug bundle (a non-empty resolveMainClass
        answer means the import finished enough to debug)."""
        deadline = asyncio.get_event_loop().time() + READY_TIMEOUT
        while not self._ready.is_set():
            if not self.alive():
                raise JdtlsError(
                    f"jdt.ls exited during startup (code {self.process.returncode if self.process else '?'}).\n"
                    f"{self.log_tail()}"
                )
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise JdtlsError(
                    f"jdt.ls not ready within {READY_TIMEOUT:.0f}s (no ServiceReady on language/status). "
                    f"First imports of large Gradle/Maven projects can exceed this.\n{self.log_tail()}"
                )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._ready.wait(), timeout=min(_READY_POLL_INTERVAL, remaining))
            if self._ready.is_set():
                return
            with contextlib.suppress(JdtlsError):
                if await self.resolve_main_class():
                    self._log_line("[ready] resolveMainClass answered before ServiceReady")
                    self._ready.set()

    async def shutdown(self) -> None:
        """LSP shutdown/exit, then kill the process. Idempotent."""
        if self.process is None:
            self._closed = True
            return
        if self.alive():
            with contextlib.suppress(JdtlsError, OSError):
                await self.request("shutdown", None, timeout=_SHUTDOWN_TIMEOUT)
                await self.notify("exit")
        self._closed = True
        for task in (self._pump_task, self._stderr_task):
            if task is not None:
                task.cancel()
        self._pump_task = self._stderr_task = None
        if self.process.returncode is None:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=5.0)
            except (TimeoutError, ProcessLookupError):
                with contextlib.suppress(ProcessLookupError):
                    self.process.kill()
        self._fail_pending()

    # -- wire ------------------------------------------------------------------

    async def _send(self, payload: dict) -> None:
        assert self.process is not None and self.process.stdin is not None
        data = encode_message(payload)
        async with self._write_lock:
            self.process.stdin.write(data)
            await self.process.stdin.drain()

    async def request(self, method: str, params: dict | None, timeout: float = REQUEST_TIMEOUT) -> Any:
        """One JSON-RPC request; returns the result, raises JdtlsError on error."""
        if self._closed or (self.process is not None and self.process.returncode is not None):
            raise JdtlsError(f"jdt.ls is not running.\n{self.log_tail()}")
        assert len(self._pending) < MAX_PENDING_REQUESTS, "too many in-flight LSP requests"
        self._next_id += 1
        rid = self._next_id
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[rid] = future
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            payload["params"] = params
        try:
            await self._send(payload)
            response = await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError:
            raise JdtlsError(f"{method}: no response from jdt.ls within {timeout:.0f}s\n{self.log_tail()}") from None
        except OSError as exc:
            raise JdtlsError(f"{method}: write to jdt.ls failed: {exc}") from None
        finally:
            self._pending.pop(rid, None)
        error = response.get("error")
        if error is not None:
            raise JdtlsError(f"{method}: {error.get('message', 'request failed')} (code {error.get('code')})")
        return response.get("result")

    async def notify(self, method: str, params: dict | None = None) -> None:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        await self._send(payload)

    async def _pump(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        try:
            while True:
                message = await read_message(self.process.stdout)
                if message.get("method") is not None:
                    if "id" in message:
                        await self._answer_server_request(message)
                    else:
                        self._handle_notification(str(message["method"]), message.get("params") or {})
                else:
                    future = self._pending.get(message.get("id", -1))
                    if future is not None and not future.done():
                        future.set_result(message)
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, OSError, ValueError):
            self._fail_pending()
        except asyncio.CancelledError:
            raise

    async def _drain_stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        try:
            while True:
                line = await self.process.stderr.readline()
                if not line:
                    return
                self._log_line(line.decode("utf-8", errors="replace"))
        except (asyncio.CancelledError, OSError):
            return

    async def _answer_server_request(self, message: dict) -> None:
        """Server→client requests: answer the handful jdt.ls actually sends.
        Everything unknown gets a null result so the server never hangs."""
        method = str(message["method"])
        params = message.get("params") or {}
        result: Any = None
        if method == "workspace/configuration":
            result = [None for _ in params.get("items") or []]
        elif method == "workspace/applyEdit":
            result = {"applied": False}
        elif method not in (
            "window/workDoneProgress/create",
            "client/registerCapability",
            "client/unregisterCapability",
        ):
            self._log_line(f"[server-request:{method}] answered null")
        with contextlib.suppress(OSError):
            await self._send({"jsonrpc": "2.0", "id": message["id"], "result": result})

    def _handle_notification(self, method: str, params: dict) -> None:
        if method == "language/status":
            status_type = str(params.get("type", ""))
            self._log_line(f"[status:{status_type}] {params.get('message', '')}")
            if status_type == "ServiceReady":
                self._ready.set()
        elif method == "window/logMessage":
            self._log_line(str(params.get("message", "")))
        elif method == "language/progressReport":
            self._log_line(f"[progress] {params.get('status', '') or params.get('task', '')}")
        # telemetry/event, $/progress, publishDiagnostics, language/eventNotification: ignore.

    def _fail_pending(self) -> None:
        self._closed = True
        code = self.process.returncode if self.process is not None else None
        crash = JdtlsError(f"jdt.ls connection closed (exit code {code}).\n{self.log_tail()}")
        for future in self._pending.values():
            if not future.done():
                future.set_exception(crash)
        self._pending.clear()

    # -- java-debug operations ---------------------------------------------------

    async def execute_command(
        self, command: str, arguments: list | None = None, timeout: float = REQUEST_TIMEOUT
    ) -> Any:
        return await self.request(
            "workspace/executeCommand", {"command": command, "arguments": arguments or []}, timeout=timeout
        )

    async def start_debug_session(self) -> int:
        """Start (or get) java-debug's DAP server; returns its TCP port. The
        port is shared. Every new connection to it is a separate session."""
        result = await self.execute_command("vscode.java.startDebugSession")
        try:
            port = int(result)
        except (TypeError, ValueError):
            raise JdtlsError(f"vscode.java.startDebugSession returned {result!r}, expected a port") from None
        assert 0 < port < 65536, f"bad DAP port {port}"
        return port

    async def resolve_main_class(self, project_root: str = "") -> list[dict]:
        """[{mainClass, projectName, filePath}, ...] discovered in the workspace."""
        uri = pathlib.Path(os.path.abspath(project_root or self.project_root)).as_uri()
        result = await self.execute_command("vscode.java.resolveMainClass", [uri])
        return [dict(item) for item in result or [] if isinstance(item, dict)]

    async def resolve_classpath(self, main_class: str, project_name: str) -> tuple[list[str], list[str]]:
        """(modulePaths, classPaths) for running main_class in project_name."""
        assert main_class, "main_class must not be empty"
        result = await self.execute_command("vscode.java.resolveClasspath", [main_class, project_name])
        if not isinstance(result, list) or len(result) != 2:
            raise JdtlsError(f"vscode.java.resolveClasspath returned {result!r}, expected [modulePaths, classPaths]")
        module_paths, class_paths = result
        return [str(p) for p in module_paths or []], [str(p) for p in class_paths or []]


# -- bounded per-project cache ----------------------------------------------------

_CLIENTS: dict[str, JdtlsClient] = {}  # project_root -> client, insertion order = LRU order
_CLIENTS_LOCK = asyncio.Lock()


async def get_client(project_root: str) -> JdtlsClient:
    """The cached (booted, ready) client for project_root; boots one if needed.
    Bounded at MAX_CACHED_CLIENTS: the least-recently-used client is shut down."""
    root = os.path.abspath(os.path.expanduser(project_root))
    async with _CLIENTS_LOCK:
        client = _CLIENTS.pop(root, None)
        if client is not None and client.alive():
            _CLIENTS[root] = client  # refresh LRU position
            return client
        if client is not None:
            await client.shutdown()  # dead process, reap before replacing
        client = JdtlsClient(root)
        try:
            await client.start()
        except BaseException:
            await client.shutdown()
            raise
        _CLIENTS[root] = client
        while len(_CLIENTS) > MAX_CACHED_CLIENTS:
            evicted_root = next(iter(_CLIENTS))
            evicted = _CLIENTS.pop(evicted_root)
            await evicted.shutdown()
        return client


async def shutdown_all() -> None:
    """Shut down every cached jdt.ls (server teardown / tests)."""
    async with _CLIENTS_LOCK:
        clients = list(_CLIENTS.values())
        _CLIENTS.clear()
    for client in clients:
        with contextlib.suppress(Exception):
            await client.shutdown()
