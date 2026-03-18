"""Tool registry: auto-detect installed tools, dispatch runs, analysis, and formatting."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


@dataclass
class InstalledTool:
    """A detected tool on the system."""

    suite: str  # "valgrind", "lldb", "dtrace", "perf"
    name: str  # "memcheck", "lldb", "dtrace", "perf"
    path: str  # "/usr/bin/valgrind"
    version: str  # "3.26.0"
    available: bool = True


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


_BACKENDS: dict[str, BackendSpec] = {}


def register_backend(spec: BackendSpec) -> None:
    """Register a tool suite backend."""
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
        for suite, tools in sorted(by_suite.items()):
            parts.append(f"**{suite}:**")
            for t in tools:
                status = "available" if t.available else "not found"
                version = f" ({t.version})" if t.version else ""
                parts.append(f"  - {t.name}: {status}{version} [{t.path}]")
            parts.append("")
        unavailable = [s for s in _BACKENDS if not self.is_available(s)]
        if unavailable:
            parts.append(f"**Not installed:** {', '.join(unavailable)}")
        return "\n".join(parts)
