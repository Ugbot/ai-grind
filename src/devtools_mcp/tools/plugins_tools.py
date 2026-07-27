"""Plugins tool: an observable health surface over the plugin loaders.

devtools-mcp discovers out-of-tree extensions through three entry-point groups —
``devtools_mcp.backends`` (tool suites), ``devtools_mcp.mcp_tools`` (MCP tools),
and ``devtools_mcp.viz_pages`` (console pages) — plus their in-tree equivalents.
Each loader degrades-never-crashes: a broken or version-incompatible plugin is
recorded in a ``_FAILED_*`` map instead of taking the server down. That makes
failures SILENT unless something surfaces them. This tool is that surface.

Action-multiplexed like tracker_*()/recipe():
    plugins(action="list")   — bounded inventory of what loaded (+ failed count)
    plugins(action="status") — health detail: every loaded surface AND every
                               failed/skipped entry with its one-line error
"""

from __future__ import annotations

from dataclasses import dataclass, field

from devtools_mcp.registry import (
    failed_backends,
    failed_tool_plugins,
    host_version,
    list_backends,
    loaded_tool_plugins,
)
from devtools_mcp.server import mcp
from devtools_mcp.viz import pages

_MAX_ITEMS: int = 200  # bound every list so output stays a summary


@dataclass
class _Snapshot:
    """A bounded view of the plugin surface at one moment."""

    host: str
    backends: list[str]
    tool_plugins: list[str]
    viz_pages: list[tuple[str, str]]  # (name, href)
    failed_backends: dict[str, str] = field(default_factory=dict)
    failed_tool_plugins: dict[str, str] = field(default_factory=dict)
    failed_viz_pages: dict[str, str] = field(default_factory=dict)

    def total_failed(self) -> int:
        return len(self.failed_backends) + len(self.failed_tool_plugins) + len(self.failed_viz_pages)


def _collect() -> _Snapshot:
    """Snapshot the plugin surface. Ensures viz pages are discovered first."""
    pages.load_viz_pages()  # idempotent — count console pages even without a dashboard
    return _Snapshot(
        host=host_version() or "unknown",
        backends=sorted(list_backends())[:_MAX_ITEMS],
        tool_plugins=sorted(loaded_tool_plugins())[:_MAX_ITEMS],
        viz_pages=[(p.name, f"/{p.prefix}") for p in pages.iter_pages()][:_MAX_ITEMS],
        failed_backends=dict(sorted(failed_backends().items())),
        failed_tool_plugins=dict(sorted(failed_tool_plugins().items())),
        failed_viz_pages=dict(sorted(pages.failed_viz_pages().items())),
    )


def _render_list(snap: _Snapshot) -> str:
    page_str = ", ".join(f"`{name}` ({href})" for name, href in snap.viz_pages) or "—"
    lines = [
        f"**Plugin surface** (devtools-mcp {snap.host})",
        "",
        f"- Backends ({len(snap.backends)}): " + (", ".join(f"`{b}`" for b in snap.backends) or "—"),
        f"- MCP tool plugins ({len(snap.tool_plugins)}): " + (", ".join(f"`{t}`" for t in snap.tool_plugins) or "—"),
        f"- Console pages ({len(snap.viz_pages)}): " + page_str,
    ]
    failed = snap.total_failed()
    lines.append(f"- Failed/skipped: {failed}" + ("" if failed == 0 else " (use action='status' for details)"))
    return "\n".join(lines)


def _render_status(snap: _Snapshot) -> str:
    lines = _render_list(snap).splitlines()
    lines += ["", "**Health**"]
    sections = (
        ("backends", snap.failed_backends),
        ("mcp_tools", snap.failed_tool_plugins),
        ("viz_pages", snap.failed_viz_pages),
    )
    any_fail = False
    for label, failures in sections:
        if failures:
            any_fail = True
            lines.append(f"- {label}:")
            for name, error in list(failures.items())[:_MAX_ITEMS]:
                lines.append(f"  - `{name}`: {error}")
    if not any_fail:
        lines.append("- all plugins loaded cleanly ✅")
    return "\n".join(lines)


@mcp.tool()
async def plugins(action: str = "list") -> str:
    """Observe the plugin/extension surface (backends, MCP tools, console pages).

    Every loader degrades-never-crashes, so a broken or version-incompatible
    plugin fails silently into a _FAILED_* map. This tool surfaces both what
    loaded and what failed.

    Actions:
        list   — bounded inventory: loaded backends, MCP tool plugins, console
                 pages, and a failed/skipped count.
        status — the inventory plus a health section listing every failed or
                 version-skipped entry with its one-line error message.
    """
    snap = _collect()
    if action == "list":
        return _render_list(snap)
    if action == "status":
        return _render_status(snap)
    return f"Unknown action {action!r}. One of: list, status"
