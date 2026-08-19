"""kotlin adapter: Kotlin/JVM debugging via fwcd/kotlin-debug-adapter.

A standalone DAP server over stdio, spawned as `kotlin-debug-adapter`
(a JVM launcher script from the Gradle distribution zip or Homebrew).
It debugs a main class of a *built* Gradle/Maven project. It resolves the
classpath from the project's build output (build/classes/kotlin/main or
target/classes/kotlin/main), so the project must be compiled first
(`./gradlew build` / `mvn compile`). Attach connects to any JVM started
with -agentlib:jdwp (which also covers Java/Scala processes).

Verified against the adapter source (KotlinDebugAdapter.kt): launch reads
exactly `projectRoot`, `mainClass`, `vmArguments` (a single string). There
is NO program-arguments field; attach reads `projectRoot`, `hostName`,
`port`, `timeout`. The binary has no --version flag (the launcher starts the
DAP server immediately), so we derive the version from the distribution's
lib/adapter-<version>.jar.

Live-tested quirks (adapter 0.4.4, JDK 25, Gradle 9.6): setBreakpoints
requires `source.name` alongside `source.path`, omitting it NPEs inside the
adapter (DAPConverter.toInternalSource) and fails the request. It also emits
the `initialized` event twice and may duplicate `stopped` events.
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
    find_project_file,
    register_adapter,
)
from devtools_mcp.debug.models import AttachConfig, LaunchConfig
from devtools_mcp.debug.protocol import StdioTransport
from devtools_mcp.registry import InstalledTool, InstallSpec, InstallStep

_BIN_NAME = "kotlin-debug-adapter.bat" if sys.platform == "win32" else "kotlin-debug-adapter"
# install.py does NOT expanduser() download destinations, so expand here at spec-build time.
_ADAPTER_HOME = os.path.expanduser(os.path.join("~", ".devtools-mcp", "adapters", "kotlin-debug-adapter"))
# The release asset `adapter.zip` unpacks to adapter/bin/kotlin-debug-adapter{,.bat} (verified 0.4.4).
_MANAGED_BIN = os.path.join(_ADAPTER_HOME, "adapter", "bin", _BIN_NAME)
_UNIX_MANAGED_BIN = os.path.join(_ADAPTER_HOME, "adapter", "bin", "kotlin-debug-adapter")
_ZIP_PATH = os.path.join(_ADAPTER_HOME, "adapter.zip")
_RELEASE_ZIP_URL = "https://github.com/fwcd/kotlin-debug-adapter/releases/latest/download/adapter.zip"

# projectRoot must be a Gradle/Maven project folder; these mark one.
_PROJECT_MARKERS = (
    "settings.gradle.kts",
    "settings.gradle",
    "build.gradle.kts",
    "build.gradle",
    "pom.xml",
)

_DEFAULT_ATTACH_TIMEOUT_MS = 30_000
_COMPILE_HINT = "the project must be COMPILED first (e.g. `./gradlew build` or `mvn compile`)"


def resolve_binary() -> str:
    """Adapter binary: $DEVTOOLS_KOTLIN_DAP > PATH > managed install dir."""
    explicit = os.environ.get("DEVTOOLS_KOTLIN_DAP", "")
    if explicit:
        return os.path.expanduser(explicit)
    found = shutil.which("kotlin-debug-adapter")
    if found:
        return found
    if os.path.isfile(_MANAGED_BIN):
        return _MANAGED_BIN
    return ""


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
    if not lines:
        return ""
    match = re.search(r'version "([^"]+)"', lines[0])
    return match.group(1) if match else lines[0].strip()


def _dist_version(binary: str) -> str:
    """Version from the distribution's lib/adapter-<version>.jar (no --version flag)."""
    root = os.path.dirname(os.path.dirname(os.path.realpath(binary)))
    for jar in sorted(glob.glob(os.path.join(root, "lib", "adapter-*.jar"))):
        match = re.match(r"adapter-(.+)\.jar$", os.path.basename(jar))
        if match:
            return match.group(1)
    return "unknown"


def _resolve_project_root(explicit: str, anchor: str) -> str:
    """Absolute Gradle/Maven project root: explicit > marker walk-up > anchor dir."""
    if explicit:
        return os.path.abspath(os.path.expanduser(explicit))
    if anchor:
        marker = find_project_file(anchor, _PROJECT_MARKERS)
        if marker:
            return os.path.dirname(os.path.abspath(marker))
        anchor = os.path.abspath(anchor)
        return anchor if os.path.isdir(anchor) else os.path.dirname(anchor)
    return ""


async def make_transport(config: LaunchConfig | AttachConfig) -> StdioTransport:
    binary = resolve_binary()
    if not binary or not os.path.isfile(binary):
        raise RuntimeError(
            "kotlin-debug-adapter not found. Unzip the GitHub release adapter.zip into "
            f"{_ADAPTER_HOME} (see devtools_install), or point $DEVTOOLS_KOTLIN_DAP at the binary. "
            "There is no Homebrew formula for it."
        )
    if not await _java_version():
        raise RuntimeError(
            "kotlin-debug-adapter needs a JDK: `java` was not found on PATH (or `java -version` failed). "
            "Install a JDK 11+ and re-run."
        )
    transport = StdioTransport([binary])
    await transport.start()
    return transport


def launch_template(config: LaunchConfig) -> dict:
    main_class = str(config.extra.get("main_class", ""))
    if not main_class:
        raise ValueError(
            "kotlin launch needs extra={'main_class': 'com.example.MainKt'}, kotlin-debug-adapter "
            "runs a JVM main class resolved from the built project, not a source file. "
            f"Also note {_COMPILE_HINT}."
        )
    if config.args:
        raise ValueError(
            "kotlin-debug-adapter does not support program arguments (its launch request has no such "
            "field, only projectRoot/mainClass/vmArguments). Pass JVM options via "
            "extra={'vm_arguments': '...'}, or start the JVM yourself with -agentlib:jdwp and attach."
        )
    if config.env:
        raise ValueError(
            "kotlin-debug-adapter does not support env vars in its launch request. "
            "Start the JVM yourself with the env set and -agentlib:jdwp, then attach."
        )
    project_root = _resolve_project_root(
        str(config.extra.get("project_root", "")) or config.cwd,
        config.program,
    )
    if not project_root:
        raise ValueError(
            "kotlin launch needs a project root: pass cwd= or extra={'project_root': ...} "
            "(or program= somewhere inside the project). Must be the Gradle/Maven project root, "
            f"the adapter resolves the classpath from the build output, so {_COMPILE_HINT}."
        )
    args: dict[str, object] = {
        "type": "kotlin",
        "request": "launch",
        "name": "devtools-mcp",
        "mainClass": main_class,
        "projectRoot": project_root,
    }
    vm_arguments = str(config.extra.get("vm_arguments", ""))
    if vm_arguments:
        args["vmArguments"] = vm_arguments
    return args


def attach_template(config: AttachConfig) -> dict:
    if config.port is None:
        raise ValueError(
            "kotlin attach needs port=, the JDWP port of a JVM started with "
            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        )
    project_root = _resolve_project_root(str(config.extra.get("project_root", "")), config.program)
    if not project_root:
        raise ValueError(
            "kotlin attach needs extra={'project_root': ...} (or program= somewhere inside the "
            "project), the adapter resolves sources from the Gradle/Maven project root, and "
            f"{_COMPILE_HINT}."
        )
    timeout_ms = int(config.extra.get("timeout_ms", _DEFAULT_ATTACH_TIMEOUT_MS))  # type: ignore[call-overload]
    return {
        "type": "kotlin",
        "request": "attach",
        "name": "devtools-mcp",
        "hostName": config.host or "localhost",
        "port": config.port,
        "timeout": timeout_ms,
        "projectRoot": project_root,
    }


async def detect() -> InstalledTool:
    binary = resolve_binary()
    java = await _java_version()
    version = ""
    if binary:
        version = _dist_version(binary)
        version += f" (java {java})" if java else " (java not found, JDK required)"
    return InstalledTool(
        suite="debug",
        name="kotlin-debug-adapter",
        path=binary,
        version=version,
        available=bool(binary) and bool(java),
    )


def sniff(program: str) -> int:
    if program.endswith((".kt", ".kts")):
        return 85
    return 0


# No Homebrew formula exists for kotlin-debug-adapter (verified: formulae.brew.sh
# 404s; only kotlin-language-server is packaged), so every POSIX platform installs
# from the GitHub release zip.
_POSIX_STEPS = [
    InstallStep(
        kind="download",
        argv=[_RELEASE_ZIP_URL, _ZIP_PATH],
        description="Download the latest kotlin-debug-adapter release (adapter.zip)",
    ),
    InstallStep(
        kind="shell",
        argv=["unzip", "-o", _ZIP_PATH, "-d", _ADAPTER_HOME],
        description="Unpack adapter.zip -> adapter/bin/kotlin-debug-adapter",
    ),
    InstallStep(
        kind="shell",
        argv=["chmod", "+x", _UNIX_MANAGED_BIN],
        description="Ensure the launcher script is executable",
    ),
]

_INSTALL = InstallSpec(
    platforms={
        "darwin": _POSIX_STEPS,
        "linux": _POSIX_STEPS,
        "windows": [
            InstallStep(
                kind="download",
                argv=[_RELEASE_ZIP_URL, _ZIP_PATH],
                description="Download the latest kotlin-debug-adapter release (adapter.zip)",
            ),
            InstallStep(
                kind="shell",
                argv=[
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    f"Expand-Archive -Force '{_ZIP_PATH}' '{_ADAPTER_HOME}'",
                ],
                description="Unpack adapter.zip -> adapter\\bin\\kotlin-debug-adapter.bat",
            ),
        ],
    },
    note=(
        "Requires a JDK 11+ on PATH. The target project must be built before launch "
        "(./gradlew build or mvn compile), the adapter resolves the classpath from the build "
        "output. Attach works against any JVM started with -agentlib:jdwp (also Java/Scala)."
    ),
    url="https://github.com/fwcd/kotlin-debug-adapter",
)


def _register() -> None:
    register_adapter(
        AdapterSpec(
            name="kotlin",
            languages=("kotlin",),
            transport=make_transport,
            launch_template=launch_template,
            attach_template=attach_template,
            detect=detect,
            sniff=sniff,
            install=_INSTALL,
            quirks=AdapterQuirks(supports_attach_socket=True),
            description=(
                "Kotlin/JVM via kotlin-debug-adapter, launch mainClass from a built Gradle/Maven "
                "project, or attach to a JDWP port"
            ),
        )
    )


_register()
