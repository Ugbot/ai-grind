"""java adapter: Java debugging via microsoft/java-debug hosted in eclipse.jdt.ls.

Unlike the other adapters there is no standalone adapter binary: the
com.microsoft.java.debug plugin runs inside a headless jdt.ls language
server (see debug.jdtls). transport() boots/reuses one JdtlsClient per
project root, calls vscode.java.startDebugSession, and connects a
SocketTransport. java-debug runs ONE shared DAP ServerSocket per jdt.ls
(JavaDebugServer is a singleton; startDebugSession is idempotent and
returns the same port — verified live and in JavaDebugServer.java): each
new TCP CONNECTION to that port is a separate debug session, so we call
startDebugSession + open a fresh socket per transport. The client rides
along in config.extra["_jdtls_client"] so startDebugging child sessions
(which reuse the parent's config object, see dap_session.spawn_child)
share the same jdt.ls instead of booting another.

launch_template is sync, so main-class/classpath resolution happens in
transport(): resolveMainClass/resolveClasspath results are stashed in
config.extra under _resolved_* keys and the template only reads config.

Verified against the java-debug source (protocol/Requests.java @ main):
LaunchArguments takes mainClass (String), args (String — NOT a list;
vscode-java-debug also flattens arrays to one string before sending),
vmArgs (String), classPaths (String[]), modulePaths (String[]), cwd, env,
projectName, stopOnEntry, console; AttachArguments takes hostName, port,
timeout (ms, default 30000), projectName. Command names verified against
JavaDebugDelegateCommandHandler.java: vscode.java.startDebugSession,
vscode.java.resolveMainClass, vscode.java.resolveClasspath (singular).

IMPORTANT project note: for Kotlin-only Gradle projects jdt.ls resolution
FAILS (jdt.ls models Java sources only — no main classes, empty
classpaths). Use the kotlin adapter for those, or pass explicit
extra={'main_class': ..., 'classpath': [...]}.

Requires a JDK 17+ on PATH. The first import of a project takes 30-60s
(jdt.ls workspace import); later sessions reuse the cached client.
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil

from devtools_mcp.debug import jdtls
from devtools_mcp.debug.adapters.base import (
    AdapterQuirks,
    AdapterSpec,
    find_project_file,
    register_adapter,
)
from devtools_mcp.debug.jdtls import JdtlsClient
from devtools_mcp.debug.models import AttachConfig, LaunchConfig
from devtools_mcp.debug.protocol import SocketTransport
from devtools_mcp.registry import InstalledTool, InstallSpec, InstallStep

# Pinned versions — both URLs verified live (HTTP 200) on 2026-07-24.
JDTLS_VERSION = "1.60.0"
_JDTLS_TARBALL_URL = (
    "https://download.eclipse.org/jdtls/milestones/"
    f"{JDTLS_VERSION}/jdt-language-server-{JDTLS_VERSION}-202606262232.tar.gz"
)
JAVA_DEBUG_VERSION = "0.53.1"
_JAVA_DEBUG_JAR_URL = (
    "https://repo1.maven.org/maven2/com/microsoft/java/com.microsoft.java.debug.plugin/"
    f"{JAVA_DEBUG_VERSION}/com.microsoft.java.debug.plugin-{JAVA_DEBUG_VERSION}.jar"
)

# install.py does NOT expanduser() download destinations — expand at spec-build time.
_HOME = os.path.expanduser(os.path.join("~", ".devtools-mcp", "adapters", "jdtls"))
_TARBALL = os.path.join(_HOME, "jdt-language-server.tar.gz")
_PLUGIN_JAR = os.path.join(_HOME, f"com.microsoft.java.debug.plugin-{JAVA_DEBUG_VERSION}.jar")

_MIN_JDK = 17
_DEFAULT_ATTACH_TIMEOUT_MS = 30_000
_MAX_CANDIDATES_LISTED = 10
_PROJECT_MARKERS = (
    "pom.xml",
    "build.gradle",
    "build.gradle.kts",
    "settings.gradle",
    "settings.gradle.kts",
    ".project",
)

_CLIENT_KEY = "_jdtls_client"  # private slot in config.extra for the shared jdt.ls
_RESOLVED_MAIN_KEY = "_resolved_main_class"
_RESOLVED_PROJECT_KEY = "_resolved_project_name"
_RESOLVED_CLASSPATH_KEY = "_resolved_classpath"
_RESOLVED_MODULEPATH_KEY = "_resolved_modulepaths"

_KOTLIN_HINT = (
    "For Kotlin-only Gradle projects jdt.ls cannot resolve main classes — use the kotlin "
    "adapter, or pass extra={'main_class': ..., 'classpath': [...]} explicitly."
)


async def _java_version() -> str:
    """First `java -version` line's quoted version (java prints to stderr), or ''."""
    java = shutil.which("java")
    if not java:
        return ""
    try:
        proc = await asyncio.create_subprocess_exec(
            java,
            "-version",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await asyncio.wait_for(proc.communicate(), timeout=15.0)
    except (OSError, TimeoutError):
        return ""
    if proc.returncode != 0:
        return ""
    lines = err.decode(errors="replace").strip().splitlines()
    match = re.search(r'version "([^"]+)"', lines[0]) if lines else None
    return match.group(1) if match else ""


def _java_major(version: str) -> int:
    """Major JDK version: '25.0.3' -> 25, '1.8.0_392' -> 8, '' -> 0."""
    parts = version.split(".")
    if not parts or not parts[0].isdigit():
        return 0
    major = int(parts[0])
    if major == 1 and len(parts) > 1 and parts[1].isdigit():  # pre-9 scheme
        return int(parts[1])
    return major


def _resolve_project_root(config: LaunchConfig | AttachConfig) -> str:
    """Project root: extra['project_root'] > cwd > marker walk-up from program."""
    explicit = str(config.extra.get("project_root", ""))
    if explicit:
        return os.path.abspath(os.path.expanduser(explicit))
    cwd = config.cwd if isinstance(config, LaunchConfig) else ""
    if cwd:
        return os.path.abspath(os.path.expanduser(cwd))
    if config.program:
        marker = find_project_file(config.program, _PROJECT_MARKERS)
        if marker:
            return os.path.dirname(os.path.abspath(marker))
        anchor = os.path.abspath(config.program)
        return anchor if os.path.isdir(anchor) else os.path.dirname(anchor)
    raise ValueError(
        "java debugging needs a project root for jdt.ls: pass extra={'project_root': ...}, "
        "cwd=, or program= somewhere inside the project (pom.xml/build.gradle/.project)."
    )


async def _resolve_launch_details(client: JdtlsClient, config: LaunchConfig) -> None:
    """Resolve mainClass/classPaths via jdt.ls and stash them in config.extra —
    launch_template is sync and must only read the config. Idempotent so child
    transports (which reuse the parent config) skip the work."""
    main_class = str(config.extra.get("main_class", "") or config.extra.get(_RESOLVED_MAIN_KEY, ""))
    project_name = str(config.extra.get("project_name", "") or config.extra.get(_RESOLVED_PROJECT_KEY, ""))
    if not main_class:
        candidates = await client.resolve_main_class()
        if not candidates:
            raise RuntimeError(
                f"jdt.ls found no main classes in {client.project_root}. Is the project imported "
                f"and does it contain a `public static void main`? {_KOTLIN_HINT}"
            )
        if len(candidates) > 1:
            listing = ", ".join(
                f"{c.get('mainClass', '?')} ({c.get('projectName', '?')})" for c in candidates[:_MAX_CANDIDATES_LISTED]
            )
            overflow = len(candidates) - _MAX_CANDIDATES_LISTED
            more = f", ... {overflow} more" if overflow > 0 else ""
            raise RuntimeError(f"Multiple main classes found — pass extra={{'main_class': ...}}: {listing}{more}")
        main_class = str(candidates[0].get("mainClass", ""))
        project_name = str(candidates[0].get("projectName", "") or "")
    config.extra[_RESOLVED_MAIN_KEY] = main_class
    config.extra[_RESOLVED_PROJECT_KEY] = project_name
    if not config.extra.get("classpath") and not config.extra.get(_RESOLVED_CLASSPATH_KEY):
        module_paths, class_paths = await client.resolve_classpath(main_class, project_name)
        if not class_paths and not module_paths:
            raise RuntimeError(
                f"jdt.ls resolved an empty classpath for {main_class!r} (project {project_name!r}). " f"{_KOTLIN_HINT}"
            )
        config.extra[_RESOLVED_CLASSPATH_KEY] = class_paths
        config.extra[_RESOLVED_MODULEPATH_KEY] = module_paths


async def make_transport(config: LaunchConfig | AttachConfig) -> SocketTransport:
    """Get/boot the project's jdt.ls, resolve launch details, then open a fresh
    TCP connection to java-debug's DAP server (one connection = one session).
    Children reuse the stashed client; startDebugSession is idempotent."""
    client = config.extra.get(_CLIENT_KEY)
    if not isinstance(client, JdtlsClient) or not client.alive():
        client = await jdtls.get_client(_resolve_project_root(config))
        config.extra[_CLIENT_KEY] = client
    if isinstance(config, LaunchConfig):
        await _resolve_launch_details(client, config)
    port = await client.start_debug_session()
    transport = SocketTransport("127.0.0.1", port)
    await transport.start()
    return transport


def _join_args(args: list[str]) -> str:
    """java-debug takes program args as ONE string (LaunchArguments.args is a
    String); quote anything with whitespace/quotes the way vscode-java-debug does."""
    parts = []
    for arg in args:
        if arg and not any(ch.isspace() for ch in arg) and '"' not in arg:
            parts.append(arg)
        else:
            parts.append('"' + arg.replace("\\", "\\\\").replace('"', '\\"') + '"')
    return " ".join(parts)


def launch_template(config: LaunchConfig) -> dict:
    main_class = str(config.extra.get("main_class", "") or config.extra.get(_RESOLVED_MAIN_KEY, ""))
    if not main_class:
        raise ValueError(
            "java launch needs a main class: pass extra={'main_class': 'com.example.Main'} "
            "(transport-time resolution found none)."
        )
    class_paths = config.extra.get("classpath") or config.extra.get(_RESOLVED_CLASSPATH_KEY) or []
    module_paths = config.extra.get("modulepaths") or config.extra.get(_RESOLVED_MODULEPATH_KEY) or []
    if not class_paths and not module_paths:
        raise ValueError(
            "java launch needs a classpath: pass extra={'classpath': [...]} "
            "(transport-time resolution produced none). " + _KOTLIN_HINT
        )
    args: dict[str, object] = {
        "type": "java",
        "request": "launch",
        "name": "devtools-mcp",
        "mainClass": main_class,
        "classPaths": [str(p) for p in class_paths],  # type: ignore[union-attr]
        "console": "internalConsole",  # default integratedTerminal would runInTerminal
        "stopOnEntry": config.stop_on_entry,
    }
    if module_paths:
        args["modulePaths"] = [str(p) for p in module_paths]  # type: ignore[union-attr]
    project_name = str(config.extra.get("project_name", "") or config.extra.get(_RESOLVED_PROJECT_KEY, ""))
    if project_name:
        args["projectName"] = project_name
    if config.args:
        args["args"] = _join_args(config.args)
    if config.cwd:
        args["cwd"] = config.cwd
    if config.env:
        args["env"] = config.env
    vm_arguments = str(config.extra.get("vm_arguments", ""))
    if vm_arguments:
        args["vmArgs"] = vm_arguments
    return args


def attach_template(config: AttachConfig) -> dict:
    if config.port is None:
        raise ValueError(
            "java attach needs port= — the JDWP port of a JVM started with "
            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        )
    args: dict[str, object] = {
        "type": "java",
        "request": "attach",
        "name": "devtools-mcp",
        "hostName": config.host or "localhost",
        "port": config.port,
        "timeout": int(config.extra.get("timeout_ms", _DEFAULT_ATTACH_TIMEOUT_MS)),  # type: ignore[call-overload]
    }
    project_name = str(config.extra.get("project_name", ""))
    if project_name:
        args["projectName"] = project_name
    return args


async def detect() -> InstalledTool:
    java_version = await _java_version()
    major = _java_major(java_version)
    home = jdtls.jdtls_home()
    launcher = jdtls.find_launcher(home)
    plugin_jar = jdtls.find_debug_plugin(home)
    available = major >= _MIN_JDK and bool(launcher) and bool(plugin_jar)
    version = ""
    if plugin_jar:
        match = re.search(r"com\.microsoft\.java\.debug\.plugin-(.+)\.jar$", os.path.basename(plugin_jar))
        version = match.group(1) if match else "unknown"
        version += f" (java {java_version})" if java_version else " (java not found)"
        if 0 < major < _MIN_JDK:
            version += f" — JDK {_MIN_JDK}+ required"
    if available:
        path = plugin_jar
    elif not launcher or not plugin_jar:
        path = f"missing jdt.ls/java-debug under {home} — devtools_install(suite='debug', tool='java')"
    else:
        path = f"JDK {_MIN_JDK}+ required on PATH (found: {java_version or 'none'})"
    return InstalledTool(suite="debug", name="java", path=path, version=version, available=available)


def sniff(program: str) -> int:
    if program.endswith(".java"):
        return 85
    return 0


def _steps() -> list[InstallStep]:
    return [
        InstallStep(
            kind="download",
            argv=[_JDTLS_TARBALL_URL, _TARBALL],
            description=f"Download eclipse.jdt.ls {JDTLS_VERSION} (~50 MB)",
        ),
        InstallStep(
            kind="shell",
            # tar handles .tar.gz on macOS/Linux and Windows 10+ (bsdtar).
            argv=["tar", "-xzf", _TARBALL, "-C", _HOME],
            description=f"Extract jdt.ls into {_HOME}",
        ),
        InstallStep(
            kind="download",
            argv=[_JAVA_DEBUG_JAR_URL, _PLUGIN_JAR],
            description=f"Download the java-debug plugin bundle {JAVA_DEBUG_VERSION} (Maven Central)",
        ),
    ]


_INSTALL = InstallSpec(
    platforms={"darwin": _steps(), "linux": _steps(), "windows": _steps()},
    note=(
        f"Requires a JDK {_MIN_JDK}+ on PATH (jdt.ls itself runs on it). The first debug session "
        "per project imports it into a jdt.ls workspace — expect 30-60s; later sessions reuse "
        "the cached server. " + _KOTLIN_HINT
    ),
    url="https://github.com/microsoft/java-debug",
)


def _register() -> None:
    register_adapter(
        AdapterSpec(
            name="java",
            languages=("java",),
            transport=make_transport,
            launch_template=launch_template,
            attach_template=attach_template,
            detect=detect,
            sniff=sniff,
            install=_INSTALL,
            quirks=AdapterQuirks(multi_session=True, supports_attach_socket=True),
            description=(
                "Java (microsoft/java-debug in headless jdt.ls) — launch a main class with "
                "auto-resolved classpath, or attach to a JDWP port"
            ),
        )
    )


_register()
