"""Tool registry: auto-detect installed tools, dispatch runs, analysis, and formatting.

Backend contract
----------------
Each suite is a subpackage registering one BackendSpec via register_backend()
at import time (module-level _register() in its backend.py). Loading is driven
by the explicit _BACKEND_MODULES manifest below plus the 'devtools_mcp.backends'
entry-point group for out-of-tree plugins — see load_backends().

Capabilities are DERIVED from which optional fields a spec sets, never declared:
a spec with `stacks` supports flame graphs, one with `install` supports
devtools_install. Deriving makes declaration/implementation drift impossible.

Dependency convention: a backend needing a non-core Python dependency must
(a) import it lazily inside runner functions, (b) report absence via detect()
as unavailable with an install hint, and (c) get a `devtools-mcp[<suite>]`
extra at that point — not before. Core deps stay minimal.
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
    description: str = ""  # one line shown in devtools_check

    def capabilities(self) -> frozenset[str]:
        """Derived, never declared — a capability exists iff its field does."""
        caps = {"detect", "run", "frames", "summary"}
        if self.format_details is not None:
            caps.add("details")
        if self.stacks is not None:
            caps.add("flamegraph")
        if self.install is not None:
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

_FAILED_BACKENDS: dict[str, str] = {}  # module/entry-point -> one-line error


def load_backends() -> None:
    """Import all backend modules; a broken backend degrades, never crashes.

    Idempotent. In-tree modules come from _BACKEND_MODULES; out-of-tree
    plugins from the 'devtools_mcp.backends' entry-point group (a plugin
    package registers by exposing a module that calls register_backend on
    import, exactly like in-tree backends).
    """
    assert len(_BACKEND_MODULES) <= MAX_BACKENDS, "backend manifest exceeds bound"
    for module_name in _BACKEND_MODULES:
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001 — degrade to unavailable, don't die
            _FAILED_BACKENDS[module_name] = f"{type(exc).__name__}: {exc}"
    entry_points = importlib.metadata.entry_points(group="devtools_mcp.backends")
    for ep in entry_points:
        try:
            ep.load()
        except Exception as exc:  # noqa: BLE001
            _FAILED_BACKENDS[f"entry-point:{ep.name}"] = f"{type(exc).__name__}: {exc}"


def failed_backends() -> dict[str, str]:
    """Backends that failed to import, module -> one-line error."""
    return dict(_FAILED_BACKENDS)


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
                    "Install commands available — devtools_install(suite=...) for: " + ", ".join(sorted(installable))
                )
        failed = failed_backends()
        if failed:
            parts.append("")
            parts.append("**Failed to load:**")
            parts.extend(f"  - {module}: {error}" for module, error in sorted(failed.items()))
        return "\n".join(parts)
