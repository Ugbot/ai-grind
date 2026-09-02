"""Tool registry: auto-detect installed tools, dispatch runs, analysis, and formatting.

Backend contract
----------------
Each suite is a subpackage registering one BackendSpec via register_backend()
at import time (module-level _register() in its backend.py). Loading is driven
by the explicit _BACKEND_MODULES manifest below plus the 'devtools_mcp.backends'
entry-point group for out-of-tree plugins. See load_backends().

Capabilities are DERIVED from which optional fields a spec sets, never declared:
a spec with `stacks` supports flame graphs, one with `install` supports
devtools_install. Deriving makes declaration/implementation drift impossible.

Dependency convention: a backend needing a non-core Python dependency must
(a) import it lazily inside runner functions, (b) report absence via detect()
as unavailable with an install hint, and (c) get a `devtools-mcp[<suite>]`
extra at that point, not before. Core deps stay minimal.
"""

from __future__ import annotations

import importlib
import importlib.metadata
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

Platform = Literal["windows", "linux", "darwin"]

# Bounds (Tiger Style: everything bounded, fail loud on overflow).
MAX_BACKENDS = 64
MAX_TOOLS_PER_SUITE = 64
MAX_INSTALL_STEPS = 8

_INSTALL_KINDS = ("winget", "choco", "apt", "dnf", "brew", "pip", "download", "shell")


@dataclass
class InstalledTool:
    """A detected tool on the system."""

    suite: str  # "valgrind", "lldb", "dtrace", "perf"
    name: str  # "memcheck", "lldb", "dtrace", "perf"
    path: str  # "/usr/bin/valgrind"
    version: str  # "3.26.0"
    available: bool = True


@dataclass(frozen=True)
class InstallStep:
    """One command that installs (part of) a suite's underlying tool.

    kind="download" argv is [url, dest_path]; every other kind argv is the
    exact command vector (never a shell string).
    """

    kind: str  # one of _INSTALL_KINDS
    argv: list[str]
    description: str
    elevation: bool = False  # needs admin (Windows) / sudo (POSIX)

    def __post_init__(self) -> None:
        assert self.kind in _INSTALL_KINDS, f"unknown install kind {self.kind!r}"
        assert self.argv, "install step argv must not be empty"
        assert all(isinstance(a, str) and a for a in self.argv), f"bad argv: {self.argv!r}"


@dataclass(frozen=True)
class InstallSpec:
    """Per-OS install commands for a suite's underlying tool(s)."""

    platforms: dict[Platform, list[InstallStep]]  # ordered steps per OS
    note: str = ""  # post-install caveats (PATH, new session, license)
    url: str = ""  # docs/download page fallback

    def __post_init__(self) -> None:
        assert self.platforms, "install spec must cover at least one platform"
        for platform, steps in self.platforms.items():
            assert platform in ("windows", "linux", "darwin"), f"bad platform {platform!r}"
            assert 0 < len(steps) <= MAX_INSTALL_STEPS, f"{platform}: {len(steps)} steps (max {MAX_INSTALL_STEPS})"


# --- Backend registration ---
# Each backend registers itself via register_backend() at import time.


@dataclass
class BackendSpec:
    """Specification for a tool suite backend."""

    suite: str
    tools: list[str]  # tool names within the suite
    detect: Callable[[], Any]  # async fn -> list[InstalledTool]
    run: Callable[..., Any]  # async fn(tool, binary, args, ...) -> RunBase
    df_builders: dict[str, Callable]  # tool -> fn(result) -> DataFrame
    format_summary: Callable[..., str]  # fn(result) -> str summary
    format_details: Callable[..., str] | None = None
    # Optional: fn(result) -> list[StackSample] for sampling backends (perf,
    # dtrace profile, ETW, JFR, async-profiler, CDB stacks). Enables flame graphs.
    stacks: Callable[..., Any] | None = None
    # Optional: per-OS install commands, surfaced via devtools_install.
    install: InstallSpec | None = None
    # Optional: per-tool install specs for suites whose tools have unrelated
    # install stories (the debug suite's adapters). devtools_install(suite,
    # tool=...) resolves here first, falling back to `install`.
    tool_installs: dict[str, InstallSpec] = field(default_factory=dict)
    description: str = ""  # one line shown in devtools_check

    def capabilities(self) -> frozenset[str]:
        """Derived, never declared, a capability exists iff its field does."""
        caps = {"detect", "run", "frames", "summary"}
        if self.format_details is not None:
            caps.add("details")
        if self.stacks is not None:
            caps.add("flamegraph")
        if self.install is not None or self.tool_installs:
            caps.add("install")
        assert caps, "capability set must never be empty"
        return frozenset(caps)


_BACKENDS: dict[str, BackendSpec] = {}


def register_backend(spec: BackendSpec) -> None:
    """Register a tool suite backend."""
    assert spec.suite, "backend suite name must not be empty"
    assert spec.tools, f"backend {spec.suite!r} must expose at least one tool"
    assert len(spec.tools) <= MAX_TOOLS_PER_SUITE, f"{spec.suite}: too many tools ({len(spec.tools)})"
    assert spec.suite not in _BACKENDS, f"duplicate backend registration: {spec.suite!r}"
    assert len(_BACKENDS) < MAX_BACKENDS, f"backend registry full ({MAX_BACKENDS})"
    _BACKENDS[spec.suite] = spec


def get_backend(suite: str) -> BackendSpec:
    """Get a registered backend by suite name."""
    if suite not in _BACKENDS:
        msg = f"Unknown suite '{suite}'. Available: {list(_BACKENDS.keys())}"
        raise KeyError(msg)
    return _BACKENDS[suite]


def list_backends() -> list[str]:
    """List registered backend suite names."""
    return list(_BACKENDS.keys())


def capability_matrix() -> dict[str, frozenset[str]]:
    """suite -> derived capabilities, for devtools_check / devtools_list output."""
    return {suite: spec.capabilities() for suite, spec in _BACKENDS.items()}


# --- Backend loading ---
# One line per backend. Adding a suite = add a subpackage + one entry here.
_BACKEND_MODULES: tuple[str, ...] = (
    "devtools_mcp.cargo.backend",
    "devtools_mcp.cdb.backend",
    "devtools_mcp.debug.backend",
    "devtools_mcp.dtrace.backend",
    "devtools_mcp.etw.backend",
    "devtools_mcp.gradle.backend",
    "devtools_mcp.jvm.backend",
    "devtools_mcp.lldb.backend",
    "devtools_mcp.maven.backend",
    "devtools_mcp.node.backend",
    "devtools_mcp.npm.backend",
    "devtools_mcp.perf.backend",
    "devtools_mcp.pnpm.backend",
    "devtools_mcp.py.backend",
    "devtools_mcp.renderdoc.backend",
    "devtools_mcp.valgrind.backend",
    "devtools_mcp.vtune.backend",
    "devtools_mcp.yarn.backend",
)

# --- Plugin version compatibility -------------------------------------------
# A plugin may declare which host version it needs, so an incompatible plugin is
# skipped-with-warning (into the _FAILED_* map) rather than crashing the loader.
# Two declaration styles, both documented in docs/extending.md:
#   1. dist metadata `Requires-Dist: devtools-mcp>=X`, checked PRE-import, so a
#      too-new plugin never even runs its module. This is the enforced path.
#   2. a `__devtools_mcp_requires__ = ">=X"` module attribute, a convenience
#      honored POST-import (best-effort) for plugins that don't pin via metadata.

HOST_DIST_NAME: str = "devtools-mcp"


def host_version() -> str:
    """The installed devtools-mcp version ('' if it can't be resolved)."""
    try:
        return importlib.metadata.version(HOST_DIST_NAME)
    except importlib.metadata.PackageNotFoundError:
        return ""


def _spec_incompat(spec: str, hv: str) -> str | None:
    """Reason string if host version `hv` does not satisfy specifier `spec`."""
    spec = (spec or "").strip()
    if not spec or not hv:
        return None  # nothing declared, or host version unknown. Never block
    try:
        from packaging.specifiers import SpecifierSet
        from packaging.version import Version

        if Version(hv) in SpecifierSet(spec):
            return None
        return f"requires {HOST_DIST_NAME}{spec} (host is {hv})"
    except Exception:  # noqa: BLE001  # a malformed spec must never block loading
        return None


def _declared_host_requirement(ep: Any) -> str:
    """The version specifier a plugin declares against the host via dist metadata.

    Reads `Requires-Dist: devtools-mcp<spec>` off the plugin's own distribution,
    which is available BEFORE importing the plugin module. Returns '' when the
    plugin declares no requirement on the host (the common case).
    """
    dist = getattr(ep, "dist", None)
    if dist is None:
        return ""
    try:
        requires = dist.metadata.get_all("Requires-Dist") or []
    except Exception:  # noqa: BLE001
        return ""
    for raw in requires:
        norm = str(raw).replace("(", "").replace(")", "").strip()
        if not norm.lower().startswith(HOST_DIST_NAME):
            return ""
        rest = norm[len(HOST_DIST_NAME) :].strip()
        rest = rest.split(";", 1)[0].strip()  # drop environment markers
        if rest.startswith("["):  # drop extras, e.g. devtools-mcp[viz]>=1
            rest = rest.split("]", 1)[-1].strip()
        if rest and rest[0] in "<>=!~":
            return rest
    return ""


def host_incompat_reason(ep: Any) -> str | None:
    """Pre-import gate: reason string when a plugin needs a host version we lack.

    None means "compatible / no declaration", load it. A non-None reason means
    the loader skips the plugin (records the reason) before importing it.
    """
    return _spec_incompat(_declared_host_requirement(ep), host_version())


def _attr_incompat_reason(obj: Any) -> str | None:
    """Post-import gate: honor a `__devtools_mcp_requires__` attribute if present."""
    spec = getattr(obj, "__devtools_mcp_requires__", "")
    return _spec_incompat(str(spec or ""), host_version())


_FAILED_BACKENDS: dict[str, str] = {}  # module/entry-point -> one-line error


def load_backends() -> None:
    """Import all backend modules; a broken backend degrades, never crashes.

    Idempotent. In-tree modules come from _BACKEND_MODULES; out-of-tree
    plugins from the 'devtools_mcp.backends' entry-point group (a plugin
    package registers by exposing a module that calls register_backend on
    import, exactly like in-tree backends). A plugin that declares an
    incompatible host version is skipped-with-warning, never imported.
    """
    assert len(_BACKEND_MODULES) <= MAX_BACKENDS, "backend manifest exceeds bound"
    for module_name in _BACKEND_MODULES:
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001  # degrade to unavailable, don't die
            _FAILED_BACKENDS[module_name] = f"{type(exc).__name__}: {exc}"
    entry_points = importlib.metadata.entry_points(group="devtools_mcp.backends")
    for ep in entry_points:
        key = f"entry-point:{ep.name}"
        reason = host_incompat_reason(ep)
        if reason:
            _FAILED_BACKENDS[key] = f"skipped: {reason}"
            continue
        try:
            loaded = ep.load()
        except Exception as exc:  # noqa: BLE001
            _FAILED_BACKENDS[key] = f"{type(exc).__name__}: {exc}"
            continue
        attr_reason = _attr_incompat_reason(loaded)
        if attr_reason:
            _FAILED_BACKENDS[key] = f"skipped: {attr_reason}"


def failed_backends() -> dict[str, str]:
    """Backends that failed to import, module -> one-line error."""
    return dict(_FAILED_BACKENDS)


_FAILED_TOOL_PLUGINS: dict[str, str] = {}
_LOADED_TOOL_PLUGINS: set[str] = set()  # entry-point names that loaded cleanly


def load_tool_plugins() -> None:
    """Load out-of-tree MCP tools from the 'devtools_mcp.mcp_tools' entry-point
    group. Unlike backends (which register without the server), tool plugins
    attach @mcp.tool()s and so must load AFTER the FastMCP `mcp` instance and
    the in-tree tools exist, server.py calls this at the very end of import.
    A broken or version-incompatible plugin degrades to an entry in the failed
    map, never crashes. Idempotent.
    """
    for ep in importlib.metadata.entry_points(group="devtools_mcp.mcp_tools"):
        key = f"tools:{ep.name}"
        if key in _FAILED_TOOL_PLUGINS or key in _LOADED_TOOL_PLUGINS:
            continue
        reason = host_incompat_reason(ep)
        if reason:
            _FAILED_TOOL_PLUGINS[key] = f"skipped: {reason}"
            continue
        try:
            loaded = ep.load()
        except Exception as exc:  # noqa: BLE001  # degrade, don't die
            _FAILED_TOOL_PLUGINS[key] = f"{type(exc).__name__}: {exc}"
            continue
        attr_reason = _attr_incompat_reason(loaded)
        if attr_reason:
            _FAILED_TOOL_PLUGINS[key] = f"skipped: {attr_reason}"
            continue
        _LOADED_TOOL_PLUGINS.add(key)


def failed_tool_plugins() -> dict[str, str]:
    return dict(_FAILED_TOOL_PLUGINS)


def loaded_tool_plugins() -> set[str]:
    """Entry-point keys ('tools:<name>') of MCP tool plugins that loaded cleanly."""
    return set(_LOADED_TOOL_PLUGINS)


@dataclass
class ToolRegistry:
    """Auto-detected tools on the system."""

    tools: dict[str, InstalledTool] = field(default_factory=dict)

    async def detect_all(self) -> None:
        """Probe the system for all installed tools across registered backends."""
        self.tools.clear()
        for _suite, backend in _BACKENDS.items():
            try:
                detected = await backend.detect()
                for tool in detected:
                    key = f"{tool.suite}:{tool.name}"
                    self.tools[key] = tool
            except Exception:
                pass

    async def detect_suite(self, suite: str) -> None:
        """Re-probe one suite (after an install) without touching the rest."""
        backend = get_backend(suite)
        stale = [key for key in self.tools if key.startswith(f"{suite}:")]
        assert len(stale) <= MAX_TOOLS_PER_SUITE, f"{suite}: stale entries exceed bound"
        for key in stale:
            del self.tools[key]
        try:
            detected = await backend.detect()
        except Exception:
            return
        for tool in detected:
            self.tools[f"{tool.suite}:{tool.name}"] = tool

    def is_available(self, suite: str, tool: str | None = None) -> bool:
        """Check if a suite/tool is available."""
        if tool:
            return self.tools.get(f"{suite}:{tool}", InstalledTool("", "", "", "", available=False)).available
        return any(t.available for t in self.tools.values() if t.suite == suite)

    def list_available(self) -> list[InstalledTool]:
        """List all available tools."""
        return [t for t in self.tools.values() if t.available]

    def format_check(self) -> str:
        """Format a human-readable check output."""
        if not self.tools:
            return "No tools detected. Run detect_all() first."
        parts = ["**Installed development tools:**", ""]
        by_suite: dict[str, list[InstalledTool]] = {}
        for tool in self.tools.values():
            by_suite.setdefault(tool.suite, []).append(tool)
        extra_caps = {"details", "flamegraph", "install"}
        for suite, tools in sorted(by_suite.items()):
            spec = _BACKENDS.get(suite)
            caps = sorted(spec.capabilities() & extra_caps) if spec else []
            cap_note = f"  [{', '.join(caps)}]" if caps else ""
            parts.append(f"**{suite}:**{cap_note}")
            for t in tools:
                status = "available" if t.available else "not found"
                version = f" ({t.version})" if t.version else ""
                parts.append(f"  - {t.name}: {status}{version} [{t.path}]")
            parts.append("")
        unavailable = [s for s in _BACKENDS if not self.is_available(s)]
        if unavailable:
            parts.append(f"**Not installed:** {', '.join(unavailable)}")
            installable = [s for s in unavailable if _BACKENDS[s].install is not None]
            if installable:
                parts.append(
                    "Install commands available, devtools_install(suite=...) for: " + ", ".join(sorted(installable))
                )
        failed = failed_backends()
        if failed:
            parts.append("")
            parts.append("**Failed to load:**")
            parts.extend(f"  - {module}: {error}" for module, error in sorted(failed.items()))
        return "\n".join(parts)
