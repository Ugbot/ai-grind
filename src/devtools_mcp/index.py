"""Unified searchable index across all stored runs.

Builds a single Polars DataFrame from all runs in a workspace,
enabling cross-run text search, filtering, and correlation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from devtools_mcp.registry import get_backend

if TYPE_CHECKING:
    from devtools_mcp.workspace import Workspace


def build_index(workspace: Workspace) -> pl.DataFrame:
    """Build a unified index DataFrame from all runs in the workspace.

    Each run contributes rows from its suite-specific DataFrame builder.
    Columns are normalized: run_id, suite, tool, function, file, line,
    kind, message, value (numeric catch-all for bytes/cost/count).
    """
    all_rows: list[dict] = []

    for run_id, run in workspace.runs.items():
        try:
            backend = get_backend(run.suite)
        except KeyError:
            continue

        # Get the appropriate DataFrame builder for this tool
        builder = backend.df_builders.get(run.tool)
        if builder is None:
            # Try a default builder for the suite
            builder = backend.df_builders.get("_default")
        if builder is None:
            continue

        try:
            df = builder(run)
        except Exception:
            continue

        if df.is_empty():
            continue

        # Normalize columns into the index schema
        for row in df.iter_rows(named=True):
            index_row: dict = {
                "run_id": run_id,
                "suite": run.suite,
                "tool": run.tool,
                "binary": run.binary,
            }
            # Map common column names to normalized fields
            index_row["function"] = _coalesce(row, "function", "top_function", "caller", "symbol")
            index_row["file"] = _coalesce(row, "file", "top_file", "shared_object")
            index_row["line"] = _coalesce_int(row, "line", "top_line")
            index_row["kind"] = _coalesce(row, "kind", "event", "agg_type", "stop_reason")
            index_row["message"] = _coalesce(row, "what", "message", "text")
            index_row["value"] = _coalesce_float(
                row,
                "bytes_leaked",
                "total_bytes",
                "heap_bytes",
                "self_Ir",
                "cost_a",
                "overhead_pct",
                "count",
                "value",
            )
            all_rows.append(index_row)

    if not all_rows:
        return pl.DataFrame(schema=_INDEX_SCHEMA)

    return pl.DataFrame(all_rows, schema=_INDEX_SCHEMA)


_INDEX_SCHEMA = {
    "run_id": pl.Utf8,
    "suite": pl.Utf8,
    "tool": pl.Utf8,
    "binary": pl.Utf8,
    "function": pl.Utf8,
    "file": pl.Utf8,
    "line": pl.Int64,
    "kind": pl.Utf8,
    "message": pl.Utf8,
    "value": pl.Float64,
}


def search_index(
    index: pl.DataFrame,
    query: str | None = None,
    suite: str | None = None,
    run_ids: list[str] | None = None,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    min_value: float | None = None,
    limit: int = 20,
) -> pl.DataFrame:
    """Search the unified index with text query and structured filters."""
    df = index

    if df.is_empty():
        return df

    if suite:
        df = df.filter(pl.col("suite") == suite)

    if run_ids:
        df = df.filter(pl.col("run_id").is_in(run_ids))

    if file_pattern:
        df = df.filter(pl.col("file").str.contains(f"(?i){file_pattern}"))

    if function_pattern:
        df = df.filter(pl.col("function").str.contains(f"(?i){function_pattern}"))

    if kind_pattern:
        df = df.filter(pl.col("kind").str.contains(f"(?i){kind_pattern}"))

    if min_value is not None:
        df = df.filter(pl.col("value") >= min_value)

    # Text search across function, file, kind, message
    if query:
        pattern = f"(?i){query}"
        df = df.filter(
            pl.col("function").str.contains(pattern)
            | pl.col("file").str.contains(pattern)
            | pl.col("kind").str.contains(pattern)
            | pl.col("message").str.contains(pattern)
        )

    return df.head(limit)


def correlate_runs(
    workspace: Workspace,
    run_id_a: str,
    run_id_b: str,
    join_on: str = "function",
    limit: int = 50,
) -> pl.DataFrame:
    """Correlate two runs by joining their DataFrames on a shared column.

    Returns a merged DataFrame showing data from both runs side by side.
    """
    run_a = workspace.get_run(run_id_a)
    run_b = workspace.get_run(run_id_b)

    backend_a = get_backend(run_a.suite)
    backend_b = get_backend(run_b.suite)

    builder_a = backend_a.df_builders.get(run_a.tool) or backend_a.df_builders.get("_default")
    builder_b = backend_b.df_builders.get(run_b.tool) or backend_b.df_builders.get("_default")

    if not builder_a or not builder_b:
        return pl.DataFrame(schema={join_on: pl.Utf8})

    df_a = builder_a(run_a)
    df_b = builder_b(run_b)

    if df_a.is_empty() or df_b.is_empty():
        return pl.DataFrame(schema={join_on: pl.Utf8})

    # Find the join column — try common mappings
    col_a = _find_join_col(df_a, join_on)
    col_b = _find_join_col(df_b, join_on)

    if not col_a or not col_b:
        return pl.DataFrame(schema={join_on: pl.Utf8})

    # Rename columns to avoid conflicts, preserving the join column
    suffix_a = f"_{run_a.suite}_{run_a.tool}"
    suffix_b = f"_{run_b.suite}_{run_b.tool}"

    # Select key numeric columns from each
    select_a = [col_a]
    select_b = [col_b]
    for col in df_a.columns:
        if col != col_a and df_a[col].dtype in (pl.Int64, pl.Float64, pl.UInt64):
            select_a.append(col)
    for col in df_b.columns:
        if col != col_b and df_b[col].dtype in (pl.Int64, pl.Float64, pl.UInt64):
            select_b.append(col)

    df_a_sel = df_a.select(select_a[:8])  # cap at 8 columns to keep output manageable
    df_b_sel = df_b.select(select_b[:8])

    if col_a != join_on:
        df_a_sel = df_a_sel.rename({col_a: join_on})
    if col_b != join_on:
        df_b_sel = df_b_sel.rename({col_b: join_on})

    # Suffix non-join columns
    for col in df_a_sel.columns:
        if col != join_on:
            df_a_sel = df_a_sel.rename({col: col + suffix_a})
    for col in df_b_sel.columns:
        if col != join_on:
            df_b_sel = df_b_sel.rename({col: col + suffix_b})

    joined = df_a_sel.join(df_b_sel, on=join_on, how="inner").head(limit)
    return joined


def _find_join_col(df: pl.DataFrame, target: str) -> str | None:
    """Find the best column in df matching the target join column name."""
    # Direct match
    if target in df.columns:
        return target
    # Common aliases
    aliases = {
        "function": ["function", "top_function", "caller", "fn", "symbol", "name"],
        "file": ["file", "top_file", "shared_object"],
        "kind": ["kind", "event", "error_kind"],
    }
    candidates = aliases.get(target, [target])
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _coalesce(row: dict, *keys: str) -> str | None:
    """Return the first non-None string value from the row."""
    for k in keys:
        val = row.get(k)
        if val is not None:
            return str(val)
    return None


def _coalesce_int(row: dict, *keys: str) -> int | None:
    """Return the first non-None integer value from the row."""
    for k in keys:
        val = row.get(k)
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                pass
    return None


def _coalesce_float(row: dict, *keys: str) -> float | None:
    """Return the first non-None float value from the row."""
    for k in keys:
        val = row.get(k)
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                pass
    return None
