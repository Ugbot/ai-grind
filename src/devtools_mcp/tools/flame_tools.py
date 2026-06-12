"""Flame-graph tool: devtools_flamegraph — render any sampling run as a graph.

Works on any stored run whose backend provides a `stacks` builder (dtrace
profile, perf record, ETW cpu, JVM jfr/asprof, CDB stacks). Returns a BOUNDED
text flame-tree + hotspot table for the LLM and writes the full interactive SVG
to the workspace, returning only its path. The per-stack data stays queryable via
devtools_analyze / devtools_query on the same run_id.
"""

from __future__ import annotations

from mcp.server.fastmcp import Context

from devtools_mcp.flamegraph import build_call_tree, render_svg, render_text_tree
from devtools_mcp.flamegraph.render_text import top_table
from devtools_mcp.registry import get_backend
from devtools_mcp.server import get_run, mcp


@mcp.tool()
async def devtools_flamegraph(
    ctx: Context,
    run_id: str,
    fmt: str = "both",
    min_pct: float = 0.5,
    max_depth: int = 32,
    top_n: int = 15,
    width: int = 1200,
    workspace_id: str | None = None,
) -> str:
    """Render a sampling run as a flame graph (SVG file + bounded text tree).

    Args:
        run_id: A run from devtools_run with stacks (e.g. dtrace profile, perf
            record, etw cpu, jvm jfr/asprof, cdb stacks).
        fmt: "both" (default), "svg" (just write the file + path), or "text".
        min_pct: Prune tree branches below this % of total samples (default 0.5).
        max_depth: Max stack depth to show in the text tree (default 32).
        top_n: Rows in the hottest-functions table (default 15).
        width: SVG width in px (default 1200).
        workspace_id: Workspace containing the run.
    """
    ws, run = get_run(ctx, run_id, workspace_id)

    try:
        backend = get_backend(run.suite)
    except KeyError:
        return f"No backend registered for suite '{run.suite}'"

    if backend.stacks is None:
        return (
            f"{run.suite}:{run.tool} does not produce stacks — no flame graph. "
            "Stacks come from dtrace profile, perf record, etw cpu, jvm jfr/asprof, or cdb stacks."
        )

    samples = backend.stacks(run)
    if not samples:
        return f"No stack samples in run `{run_id}` ({run.suite}:{run.tool})."

    tree = build_call_tree(samples, root_name=run.binary or "all")
    total = tree.total_weight

    parts = [
        f"**Flame graph — {run.suite}:{run.tool}** · {total:,} samples · run `{run_id}`",
        "",
    ]

    if fmt in ("both", "svg"):
        svg = render_svg(tree, title=f"{run.suite}:{run.tool} {run.binary}", width=width)
        svg_path = ws.store_artifact(run_id, "flame.svg", svg)
        parts.append(f"**SVG:** `{svg_path}` (open in a browser)")
        parts.append("")

    if fmt in ("both", "text"):
        parts.append(f"**Hottest functions (top {top_n}):**")
        parts.append(top_table(tree, top_n))
        parts.append("")
        parts.append(f"**Flame tree** (branches ≥ {min_pct}%):")
        parts.append("```")
        parts.append(render_text_tree(tree, min_pct=min_pct, max_depth=max_depth))
        parts.append("```")
        parts.append(f'_Full per-stack data is queryable: devtools_analyze(run_id="{run_id}")._')

    return "\n".join(parts)
