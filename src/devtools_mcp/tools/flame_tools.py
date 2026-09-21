"""Flame-graph tool: devtools_flamegraph, render any sampling run as a graph.

Works on any stored run whose backend provides a `stacks` builder (dtrace
profile, perf record, ETW cpu, JVM jfr/asprof, CDB stacks).

Two output formats:
- "text" (default): bounded ASCII flame-tree + hotspot table, what agents read.
- "data": folded stacks as JSON ({stack: str, count: int}[]) for GUI renderers
  (e.g. an ImPlot flame chart in the desktop GUI).

The per-stack data stays queryable via devtools_analyze / devtools_query on the
same run_id.
"""

from __future__ import annotations

import json
import re

from mcp.server.fastmcp import Context

from devtools_mcp.flamegraph import build_call_tree, filter_samples, render_text_tree
from devtools_mcp.flamegraph.fold import emit_folded, parse_folded
from devtools_mcp.flamegraph.render_text import top_table
from devtools_mcp.registry import get_backend
from devtools_mcp.server import get_run, mcp

_MAX_DATA_STACKS = 2_000  # cap on JSON stacks returned inline; heaviest kept, rest noted


@mcp.tool()
async def devtools_flamegraph(
    ctx: Context,
    run_id: str,
    fmt: str = "text",
    min_pct: float = 0.5,
    max_depth: int = 32,
    top_n: int = 15,
    stack_include: str | None = None,
    stack_exclude: str | None = None,
    workspace_id: str | None = None,
) -> str:
    """Render a sampling run as a flame graph.

    Args:
        run_id: A run from devtools_run with stacks (e.g. dtrace profile, perf
            record, etw cpu, jvm jfr/asprof, cdb stacks).
        fmt: "text" (default), ASCII flame tree + hotspot table for agent
            consumption; "data", folded stacks as JSON array of
            {stack: string, count: int} for GUI/ImPlot rendering.
        min_pct: (text only) Prune tree branches below this % of total samples
            (default 0.5).
        max_depth: (text only) Max stack depth to show in the text tree
            (default 32).
        top_n: (text only) Rows in the hottest-functions table (default 15).
        stack_include: Regex matched against EVERY frame of a sample; keep only
            samples whose stack contains a match, zoom the whole graph onto one
            subsystem's call paths without re-running the profiler.
        stack_exclude: Regex matched against EVERY frame; drop a sample whole if
            any frame matches. The filter a mixed-phase capture needs: a
            whole-process profile that includes a setup/load phase attributes its
            cost to shared leaves (memmove, memcpy, malloc), so excluding the
            phase by CALL PATH is the only way to see where execution time goes.
            Applied before the tree is built, so every % in the output is a share
            of the kept samples, not of the unfiltered run.
        workspace_id: Workspace containing the run.
    """
    if fmt not in ("text", "data"):
        return f"fmt must be 'text' or 'data', got {fmt!r}"

    _ws, run = get_run(ctx, run_id, workspace_id)

    try:
        backend = get_backend(run.suite)
    except KeyError:
        return f"No backend registered for suite '{run.suite}'"

    if backend.stacks is None:
        return (
            f"{run.suite}:{run.tool} does not produce stacks. No flame graph. "
            "Stacks come from dtrace profile, perf record, etw cpu, jvm jfr/asprof, or cdb stacks."
        )

    samples = backend.stacks(run)
    if not samples:
        return f"No stack samples in run `{run_id}` ({run.suite}:{run.tool})."

    try:
        samples, cut = filter_samples(samples, stack_include, stack_exclude)
    except re.error as exc:
        return f"Invalid stack filter regex: {exc}"
    if not samples:
        return (
            f"The stack filter removed all {cut.dropped_weight:,} samples of run "
            f"`{run_id}`. Nothing to render. Loosen stack_include/stack_exclude."
        )

    assert len(samples) > 0, "stacks() returned empty list after non-empty check"

    if fmt == "data":
        # Folded stacks as a JSON array for GUI consumers (e.g. ImPlot). This flows
        # through the model context, so keep the heaviest stacks and note the rest
        # rather than dumping the whole (potentially multi-MB) set.
        folded_text = emit_folded(samples)
        normalised = sorted(parse_folded(folded_text), key=lambda s: s.weight, reverse=True)
        total = len(normalised)
        kept = normalised[:_MAX_DATA_STACKS]
        payload = [{"stack": ";".join(s.frames), "count": s.weight} for s in kept]
        note = (
            f" (top {_MAX_DATA_STACKS:,} by weight; {total - _MAX_DATA_STACKS:,} lighter stacks omitted, "
            "open the dashboard for the full flame graph)"
            if total > _MAX_DATA_STACKS
            else ""
        )
        filtered = f" · {cut.describe()}" if cut.active else ""
        header = (
            f"Folded stack data, {run.suite}:{run.tool} · {len(payload):,} stacks"
            f"{note}{filtered} · run `{run_id}`\n\n"
        )
        return header + json.dumps(payload)

    # fmt == "text"
    tree = build_call_tree(samples, root_name=run.binary or "all")
    total = tree.total_weight

    parts = [
        f"**Flame graph, {run.suite}:{run.tool}** · {total:,} samples · run `{run_id}`",
    ]
    if cut.active:
        parts.append(f"_{cut.describe()}_")
    parts += [
        "",
        f"**Hottest functions (top {top_n}):**",
        top_table(tree, top_n),
        "",
        f"**Flame tree** (branches ≥ {min_pct}%):",
        "```",
        render_text_tree(tree, min_pct=min_pct, max_depth=max_depth),
        "```",
        f'_Full per-stack data is queryable: devtools_analyze(run_id="{run_id}")._',
        f'_GUI rendering: devtools_flamegraph(run_id="{run_id}", fmt="data") returns folded JSON._',
    ]

    return "\n".join(parts)
