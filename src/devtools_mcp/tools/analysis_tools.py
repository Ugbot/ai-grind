"""Analysis tools: devtools_analyze, devtools_query, devtools_compare."""

from __future__ import annotations

import re

import polars as pl
from mcp.server.fastmcp import Context

from devtools_mcp.filters import apply_filters, build_filter_spec
from devtools_mcp.formatters import format_filtered
from devtools_mcp.registry import get_backend
from devtools_mcp.server import get_app_ctx, get_run, mcp


@mcp.tool()
async def devtools_analyze(
    ctx: Context,
    run_id: str,
    group_by: str | None = None,
    top_n: int = 20,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_bytes: int | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int | None = None,
    sample_n: int | None = None,
    sample_every: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Analyze a run result with rich filtering.

    Builds a DataFrame from the run, applies filters, and returns a focused table.
    Works with any tool suite (valgrind, dtrace, perf, lldb snapshots).

    Args:
        run_id: The run_id from a previous devtools_run or debug_inspect
        group_by: Column to group by (e.g. "kind", "function", "file")
        top_n: Max groups when using group_by
        file_pattern: Regex to include only matching files
        function_pattern: Regex to include only matching functions
        kind_pattern: Regex to include only matching error/event kinds
        exclude_files: Regex to exclude files (e.g. "/usr/lib|vg_replace")
        exclude_functions: Regex to exclude functions (e.g. "^__|^std::")
        min_bytes: Minimum bytes threshold
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        sample_n: Random sample of N rows
        sample_every: Take every Nth row
        workspace_id: Workspace containing the run
    """
    ws, run = get_run(ctx, run_id, workspace_id)

    try:
        backend = get_backend(run.suite)
    except KeyError:
        return f"No backend registered for suite '{run.suite}'"

    builder = backend.df_builders.get(run.tool) or backend.df_builders.get("_default")
    if not builder:
        return f"No DataFrame builder for {run.suite}:{run.tool}"

    df = ws.get_dataframe(run_id)
    if df is None:
        df = builder(run)
    if df.is_empty():
        return f"No data in run `{run_id}` ({run.suite}:{run.tool})"
    ws.cache_dataframe(run_id, df)

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        kind_pattern=kind_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        min_bytes=min_bytes,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
        sample_every=sample_every,
    )

    df = apply_filters(df, spec)

    if group_by:
        if group_by not in df.columns:
            cols = ", ".join(df.columns)
            return f"Cannot group by {group_by!r}, not a column of this run. Available columns: {cols}"
        numeric_cols = [c for c in df.columns if c != group_by and df[c].dtype in (pl.Int64, pl.Float64, pl.UInt64)]
        aggs = [pl.len().alias("count")]
        for col in numeric_cols[:5]:
            aggs.append(pl.col(col).sum().alias(f"total_{col}"))
        df = df.group_by(group_by).agg(aggs).sort("count", descending=True).head(top_n)

    return format_filtered(df, f"{run.suite}:{run.tool} analysis", spec)


@mcp.tool()
async def devtools_query(
    ctx: Context,
    run_id: str,
    columns: list[str] | None = None,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int = 50,
    sample_n: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Ad-hoc DataFrame query on any run. Pass columns=["schema"] to list available columns.

    Args:
        run_id: The run_id from any previous run
        columns: Columns to return (None=all, ["schema"]=list columns)
        file_pattern: Regex include on files
        function_pattern: Regex include on functions
        kind_pattern: Regex include on kinds
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows (default 50)
        sample_n: Random sample of N rows
        workspace_id: Workspace containing the run
    """
    ws, run = get_run(ctx, run_id, workspace_id)

    try:
        backend = get_backend(run.suite)
    except KeyError:
        return f"No backend for suite '{run.suite}'"

    builder = backend.df_builders.get(run.tool) or backend.df_builders.get("_default")
    if not builder:
        return f"No DataFrame builder for {run.suite}:{run.tool}"

    df = ws.get_dataframe(run_id)
    if df is None:
        df = builder(run)
    if not df.is_empty():
        ws.cache_dataframe(run_id, df)

    if columns == ["schema"]:
        schema_info = [f"- `{col}`: {dtype}" for col, dtype in df.schema.items()]
        header = f"**Schema for {run.suite}:{run.tool} `{run_id}`:**\n\n"
        return header + "\n".join(schema_info) + f"\n\n{len(df)} total rows"

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        kind_pattern=kind_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
    )

    df = apply_filters(df, spec)

    if columns and columns != ["schema"]:
        valid_cols = [c for c in columns if c in df.columns]
        if valid_cols:
            df = df.select(valid_cols)
        else:
            return f"No matching columns. Available: {df.columns}"

    return format_filtered(df, f"Query: {run.suite}:{run.tool}", spec)


@mcp.tool()
async def devtools_aggregate(
    ctx: Context,
    run_ids: list[str] | None = None,
    tag: str | None = None,
    tags: list[str] | None = None,
    metric: str = "self",
    function_pattern: str | None = None,
    exclude_functions: str | None = None,
    stack_include: str | None = None,
    stack_exclude: str | None = None,
    top_n: int = 25,
    min_runs: int = 1,
    workspace_id: str | None = None,
) -> str:
    """Aggregate per-function cost across MANY sampling runs at once.

    devtools_compare answers "what changed between run A and run B".
    This answers the other question a profiling campaign asks: "across my
    whole benchmark suite, where does the time actually go, and which costs
    are BROAD (many workloads) versus SPIKY (one workload)?", the split that
    decides whether an optimization pays once or everywhere.

    Percentages are normalized per run before combining, so a long capture
    cannot outvote a short one. Reports, per function:
      runs: how many runs it appears in (breadth)
      mean_pct: mean share across the runs it appears in
      max_pct: its worst single run
      top_run: which run that was (where to go profile next)
      total_share: mean_pct * runs / n_runs, the suite-wide ranking key

    `function_pattern`/`exclude_functions` filter the RESULT ROWS by name;
    `stack_include`/`stack_exclude` filter the SAMPLES by call path. Reach for
    the latter whenever a whole-process profile mixes phases. See below.

    Args:
        run_ids: Runs to aggregate. None = every stacks-capable run matching
            `tag`/`tags` (or all of them, if neither is given).
        tag: Only aggregate runs carrying this tag (e.g. "tpch-sf10").
        tags: Only aggregate runs carrying ALL of these tags. A campaign tags a
            run several ways at once (by suite, by query, by sweep date), so the
            useful selection is usually the intersection, not one label.
            Combines with `tag`, which is kept for existing callers.
        metric: "self" (exclusive, where cycles burn) or "total"
            (inclusive, subtree cost, for finding expensive call paths).
        function_pattern: Regex to include only matching functions.
        exclude_functions: Regex to drop functions (e.g. framework frames).
        stack_include: Regex matched against EVERY frame; keep only samples whose
            stack contains a match. Narrows the whole aggregate to one
            subsystem's call paths.
        stack_exclude: Regex matched against EVERY frame; drop a sample whole if
            any frame matches. This is the filter a mixed-phase profile needs: a
            benchmark that spends 34% of samples loading tables ranks memmove #1
            until you drop every sample under the loader, because a leaf-name
            filter would delete the memmove that matters too. Applied BEFORE
            per-run normalization, so the surviving samples renormalize to 100%
            and shares describe the filtered universe, not a 66% remainder.
        top_n: Rows to return.
        min_runs: Only report functions appearing in at least this many runs
            (raise it to isolate the genuinely cross-cutting costs).
        workspace_id: Workspace containing the runs.
    """
    if metric not in ("self", "total"):
        return f"metric must be 'self' or 'total', got {metric!r}"

    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    if run_ids:
        candidates = list(run_ids)
    else:
        candidates = [r["run_id"] for r in ws.list_runs()]

    from devtools_mcp.flamegraph.sample_filter import StackFilter, filter_samples
    from devtools_mcp.flamegraph.tree import function_frame

    required_tags = list(tags or [])
    if tag is not None:
        required_tags.append(tag)

    frames: list[tuple[str, object]] = []
    skipped: list[str] = []
    # Each run is stack-filtered on its own, then %-normalized; the per-run cuts
    # accumulate into one header line so the reader sees how much was dropped.
    cut = StackFilter()
    for rid in candidates:
        try:
            run = ws.get_run(rid)
        except (KeyError, ValueError):
            skipped.append(f"{rid[:8]}(missing)")
            continue
        run_tags = run.tags or []
        if any(t not in run_tags for t in required_tags):
            continue
        try:
            backend = get_backend(run.suite)
        except KeyError:
            continue
        if backend.stacks is None:
            continue
        samples = backend.stacks(run)
        if not samples:
            skipped.append(f"{rid[:8]}(no stacks)")
            continue
        try:
            samples, run_cut = filter_samples(samples, stack_include, stack_exclude)
        except re.error as exc:
            return f"Invalid stack filter regex: {exc}"
        cut = cut + run_cut
        if not samples:
            skipped.append(f"{rid[:8]}(all samples stack-filtered)")
            continue
        label = run.label or f"{run.suite}:{run.tool}"
        frames.append((f"{label} [{rid[:8]}]", function_frame(samples)))

    if not frames:
        return (
            "No stacks-capable runs matched. "
            f"(candidates={len(candidates)}, tags={required_tags!r}"
            + (f", stack filter dropped {cut.dropped_weight:,} samples" if cut.active else "")
            + ")"
        )

    col = "self_pct" if metric == "self" else "total_pct"
    stacked = pl.concat(
        [f.select(["function", col]).with_columns(pl.lit(name).alias("run"))
         for name, f in frames],
        how="vertical",
    )
    n_runs = len(frames)
    agg = (
        stacked.group_by("function")
        .agg([
            pl.len().alias("runs"),
            pl.col(col).mean().round(2).alias("mean_pct"),
            pl.col(col).max().round(2).alias("max_pct"),
            pl.col("run").sort_by(col, descending=True).first().alias("top_run"),
        ])
        .filter(pl.col("runs") >= min_runs)
        .with_columns(
            (pl.col("mean_pct") * pl.col("runs") / n_runs).round(2).alias("total_share")
        )
        .sort("total_share", descending=True)
    )

    spec = build_filter_spec(
        function_pattern=function_pattern,
        exclude_functions=exclude_functions,
        limit=top_n,
    )
    agg = apply_filters(agg, spec)
    title = (
        f"Aggregate across {n_runs} run(s) · metric={metric} "
        f"(%-normalized per run, then combined)"
    )
    if cut.active:
        title += f" · {cut.describe()}"
    if skipped:
        title += f" · skipped: {', '.join(skipped[:5])}"
    return format_filtered(agg, title, spec)


@mcp.tool()
async def devtools_compare(
    ctx: Context,
    run_id_a: str,
    run_id_b: str,
    function_pattern: str | None = None,
    exclude_functions: str | None = None,
    stack_include: str | None = None,
    stack_exclude: str | None = None,
    min_delta: int | None = None,
    sort_by: str | None = None,
    offset: int = 0,
    limit: int = 50,
    workspace_id: str | None = None,
) -> str:
    """Compare two runs of the same tool type. Shows deltas.

    Args:
        run_id_a: Baseline run
        run_id_b: Comparison run
        function_pattern: Regex to include only matching functions
        exclude_functions: Regex to exclude functions
        stack_include: Regex matched against EVERY frame of a sample; keep only
            samples whose stack contains a match (stack-based suites only).
            Compares one subsystem's call paths instead of two whole processes.
        stack_exclude: Regex matched against EVERY frame; drop a sample whole if
            any frame matches (stack-based suites only). Needed when the two runs
            share a phase you are not comparing, a setup/load phase that differs
            in length between A and B otherwise shows up as a fake regression in
            every function it touches. Applied before %-normalization, so both
            sides renormalize to their own filtered universe and the deltas stay
            comparable.
        min_delta: Minimum absolute delta to include
        sort_by: Column to sort by
        offset: Skip first N rows
        limit: Max rows
        workspace_id: Workspace containing both runs
    """
    ws_a, run_a = get_run(ctx, run_id_a, workspace_id)
    _, run_b = get_run(ctx, run_id_b, workspace_id)

    if run_a.suite != run_b.suite or run_a.tool != run_b.tool:
        return f"Cannot compare {run_a.suite}:{run_a.tool} with {run_b.suite}:{run_b.tool}"

    # Try suite-specific comparison first
    if run_a.suite == "valgrind":
        from devtools_mcp.valgrind import analysis as vg_analysis
        from devtools_mcp.valgrind.models import CallgrindResult, MassifResult, MemcheckResult

        if isinstance(run_a, MemcheckResult) and isinstance(run_b, MemcheckResult):
            df = vg_analysis.compare_memcheck(run_a, run_b)
        elif isinstance(run_a, CallgrindResult) and isinstance(run_b, CallgrindResult):
            df = vg_analysis.compare_callgrind(run_a, run_b)
        elif isinstance(run_a, MassifResult) and isinstance(run_b, MassifResult):
            info = vg_analysis.compare_massif(run_a, run_b)
            parts = ["**Comparison (A → B):**", ""]
            for k, v in info.items():
                parts.append(f"- {k}: {v:,}" if isinstance(v, int) else f"- {k}: {v:.1f}")
            return "\n".join(parts)
        else:
            return f"No comparison for {run_a.tool}"

        if min_delta is not None:
            delta_cols = [c for c in df.columns if "delta" in c]
            for col in delta_cols:
                df = df.filter(pl.col(col).abs() >= min_delta)

        spec = build_filter_spec(
            function_pattern=function_pattern,
            exclude_functions=exclude_functions,
            sort_by=sort_by,
            offset=offset,
            limit=limit,
        )
        df = apply_filters(df, spec)
        return format_filtered(df, f"Comparison: {run_a.suite}:{run_a.tool} (A → B)", spec)

    # Generic stack-based comparison: any suite whose backend produces
    # StackSamples (dtrace profile, perf record, etw cpu, jvm jfr/asprof, cdb
    # stacks) compares at the function level. Percent columns are normalized
    # per run, so different total sample counts (different capture windows)
    # compare fairly; raw counts are kept for min_delta and magnitude context.
    try:
        backend = get_backend(run_a.suite)
    except KeyError:
        backend = None
    if backend is not None and backend.stacks is not None:
        from devtools_mcp.flamegraph.sample_filter import filter_samples
        from devtools_mcp.flamegraph.tree import function_frame

        samples_a = backend.stacks(run_a)
        samples_b = backend.stacks(run_b)
        if not samples_a or not samples_b:
            return (
                f"One of the runs has no stack samples "
                f"(A: {len(samples_a or [])}, B: {len(samples_b or [])})."
            )
        try:
            samples_a, cut_a = filter_samples(samples_a, stack_include, stack_exclude)
            samples_b, cut_b = filter_samples(samples_b, stack_include, stack_exclude)
        except re.error as exc:
            return f"Invalid stack filter regex: {exc}"
        if not samples_a or not samples_b:
            return (
                "The stack filter removed every sample from one of the runs "
                f"(A kept {len(samples_a)}, B kept {len(samples_b)}), "
                "nothing left to compare."
            )
        fa = function_frame(samples_a).rename(
            {"self": "self_a", "total": "total_a",
             "self_pct": "self_pct_a", "total_pct": "total_pct_a"})
        fb = function_frame(samples_b).rename(
            {"self": "self_b", "total": "total_b",
             "self_pct": "self_pct_b", "total_pct": "total_pct_b"})
        df = fa.join(fb, on="function", how="full", coalesce=True).fill_null(0)
        df = df.with_columns([
            (pl.col("self_b") - pl.col("self_a")).alias("self_delta"),
            (pl.col("total_b") - pl.col("total_a")).alias("total_delta"),
            (pl.col("total_pct_b") - pl.col("total_pct_a"))
                .round(2).alias("total_pct_delta"),
        ])
        # Column order: the story first (function, normalized shares, delta),
        # raw counts last for magnitude.
        df = df.select([
            "function", "total_pct_a", "total_pct_b", "total_pct_delta",
            "self_a", "self_b", "self_delta", "total_a", "total_b",
            "total_delta",
        ])
        if min_delta is not None:
            df = df.filter(pl.col("total_delta").abs() >= min_delta)
        spec = build_filter_spec(
            function_pattern=function_pattern,
            exclude_functions=exclude_functions,
            sort_by=sort_by,
            offset=offset,
            limit=limit,
        )
        if sort_by is None:
            df = df.sort(pl.col("total_pct_delta").abs(), descending=True)
        df = apply_filters(df, spec)
        n_a = sum(x.weight for x in samples_a)
        n_b = sum(x.weight for x in samples_b)
        title = (
            f"Comparison: {run_a.suite}:{run_a.tool} (A → B) · "
            f"{n_a:,} → {n_b:,} samples · %-columns normalized per run"
        )
        if cut_a.active:
            title += f" · {(cut_a + cut_b).describe()}"
        return format_filtered(df, title, spec)

    return f"Comparison not yet implemented for suite '{run_a.suite}'"
