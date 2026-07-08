"""Batch tools: devtools_check, devtools_run, devtools_list, devtools_raw, delete, export."""

from __future__ import annotations

import tempfile
from pathlib import Path

from mcp.server.fastmcp import Context

from devtools_mcp.registry import get_backend
from devtools_mcp.server import get_app_ctx, get_run, mcp


@mcp.tool()
async def devtools_check(ctx: Context) -> str:
    """Detect all installed development tools and their versions.

    Probes the system for valgrind, lldb, dtrace, and perf.
    Run this first to see what's available.
    """
    app = get_app_ctx(ctx)
    return app.registry.format_check()


@mcp.tool()
async def devtools_run(
    ctx: Context,
    suite: str,
    tool: str,
    binary: str,
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
    label: str = "",
    notes: str = "",
    tags: list[str] | None = None,
    task_key: str = "",
    parent_run_id: str = "",
    batch_id: str = "",
) -> str:
    """Run any development tool against a binary.

    Dispatches to the correct backend (valgrind, dtrace, perf) based on suite.
    Returns a concise summary with run_id for deeper analysis via devtools_analyze
    or devtools_search.

    Args:
        suite: Tool suite — "valgrind", "dtrace", "perf"
        tool: Specific tool — e.g. "memcheck", "callgrind", "massif", "stat", "trace"
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        extra_args: Extra flags for the tool (e.g. valgrind suppression files)
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
        label: Short human title for the run catalog (shown on dashboard cards)
        notes: What/why context — shown as preview text on dashboard cards
        tags: Labels for filtering runs in the dashboard
        task_key: Link to tracker task key (e.g. GRIND-42)
        parent_run_id: Prior run this re-run relates to
        batch_id: Group id for related runs
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    if not app.registry.is_available(suite, tool):
        available = [f"{t.suite}:{t.name}" for t in app.registry.list_available()]
        return f"Tool {suite}:{tool} is not available.\n\nInstalled tools: {', '.join(available) or 'none'}"

    try:
        backend = get_backend(suite)
    except KeyError as e:
        return str(e)

    err, parsed, raw_path = await backend.run(
        tool=tool,
        binary=binary,
        args=args,
        extra_args=extra_args,
        timeout=timeout,
    )

    if err:
        return err

    if label:
        parsed.label = label
    if notes:
        parsed.notes = notes
    if tags:
        parsed.tags = [t.strip().lower() for t in tags if t.strip()]
    if task_key:
        parsed.task_key = task_key.strip().upper()
    if parent_run_id:
        parsed.parent_run_id = parent_run_id
    if batch_id:
        parsed.batch_id = batch_id

    tool_info = app.registry.tools.get(f"{suite}:{tool}")
    if tool_info:
        parsed.tool_version = tool_info.version
        parsed.tool_path = tool_info.path

    summary = backend.format_summary(parsed)
    ws.store_run(parsed, raw_path, summary=summary)
    return summary


@mcp.tool()
async def devtools_list(ctx: Context, workspace_id: str | None = None) -> str:
    """List all stored runs in the workspace.

    Shows run_id, suite, tool, binary, duration, exit code, tags, and task_key.
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    runs = ws.list_runs()

    if not runs:
        return f"No runs in workspace `{ws.name}`."

    parts = [f"**Workspace:** `{ws.name}` ({len(runs)} run(s))", ""]
    for run in runs:
        extra = []
        if run.get("label"):
            extra.append(run["label"])
        if run.get("task_key"):
            extra.append(f"task={run['task_key']}")
        if run.get("tags"):
            extra.append(f"tags={run['tags']}")
        note = f" ({', '.join(extra)})" if extra else ""
        parts.append(
            f"- `{run['run_id']}` | {run['suite']}:{run['tool']} | {run['binary']} | "
            f"{run['duration']} | exit {run['exit_code']}{note}"
        )
    return "\n".join(parts)


@mcp.tool()
async def devtools_raw(ctx: Context, run_id: str, workspace_id: str | None = None) -> str:
    """Get the raw tool output for a run.

    Returns truncated output if the file exceeds 200KB.
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    try:
        raw_path = ws.get_raw_path(run_id)
    except KeyError:
        run = ws.get_run(run_id)
        inline = getattr(run, "raw_output", "") or ""
        if inline:
            content = inline
        else:
            return f"No raw output stored for run `{run_id}`."
    else:
        try:
            with open(raw_path, errors="replace") as f:
                content = f.read()
        except FileNotFoundError:
            return f"Raw output file not found: {raw_path}"

    max_len = 200_000
    if len(content) > max_len:
        return content[:max_len] + f"\n\n... truncated ({len(content):,} total bytes)"
    return content


@mcp.tool()
async def devtools_delete_run(ctx: Context, run_id: str, workspace_id: str | None = None) -> str:
    """Delete a run from memory and the persistent catalog."""
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    if ws.delete_run(run_id):
        return f"Deleted run `{run_id}`."
    return f"Run `{run_id}` not found."


@mcp.tool()
async def devtools_export(
    ctx: Context,
    run_id: str,
    format: str = "bundle",
    workspace_id: str | None = None,
) -> str:
    """Export a run as a zip bundle or parquet/json path.

    format: bundle (zip of run dir), parquet, or json (result.json path).
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    ws.get_run(run_id)  # ensure exists
    store = ws._store()

    if format == "bundle":
        out = Path(tempfile.gettempdir()) / f"devtools-mcp-{run_id}.zip"
        path = store.export_bundle(run_id, out)
        return f"Exported bundle: {path}"
    if format == "parquet":
        from devtools_mcp.registry import get_backend

        run = ws.get_run(run_id)
        backend = get_backend(run.suite)
        builder = backend.df_builders.get(run.tool) or backend.df_builders.get("_default")
        if not builder:
            return f"No data builder for {run.suite}:{run.tool}"
        df = builder(run)
        store.save_parquet(run_id, df)
        return f"Parquet: {store._run_dir(run_id) / 'data.parquet'}"
    if format == "json":
        return f"JSON: {store._run_dir(run_id) / 'result.json'}"
    return f"Unknown format {format!r}. Use bundle, parquet, or json."


@mcp.tool()
async def devtools_tag_run(
    ctx: Context,
    run_id: str,
    label: str = "",
    notes: str = "",
    tags: list[str] | None = None,
    task_key: str = "",
    workspace_id: str | None = None,
) -> str:
    """Update metadata on an existing run (label, notes, tags, task_key)."""
    ws, run = get_run(ctx, run_id, workspace_id)
    if label:
        run.label = label
    if notes:
        run.notes = notes
    if tags is not None:
        run.tags = [t.strip().lower() for t in tags if t.strip()]
    if task_key:
        run.task_key = task_key.strip().upper()
    summary = run.stored_summary
    if not summary:
        try:
            backend = get_backend(run.suite)
            summary = backend.format_summary(run)
        except KeyError:
            summary = ""
    ws.store_run(run, ws.raw_files.get(run_id, ""), summary=summary, enrich=False)
    return f"Updated metadata for `{run_id}`."
