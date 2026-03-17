"""Rich filtering and sampling engine for Valgrind analysis DataFrames.

Provides a declarative FilterSpec that can be applied to any Polars DataFrame
produced by the analysis module. Supports:
- Pattern matching (regex) on string columns (file, function, kind, etc.)
- Exclusion patterns (skip library/system code)
- Numeric thresholds (min/max on any numeric column)
- Pagination (offset + limit)
- Sampling (random, every-nth, stratified)
- Time ranges (for massif timelines)
- Sort override
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import polars as pl


@dataclass
class FilterSpec:
    """Declarative filter specification applicable to any analysis DataFrame.

    All fields are optional — only specified filters are applied.
    Filters are applied in order: include → exclude → thresholds → sort → sample → paginate.
    """

    # --- Pattern inclusion (regex, case-insensitive) ---
    # Each maps a column name to a regex pattern. Rows matching ANY pattern are kept.
    file_pattern: str | None = None          # regex on "file" or "top_file" column
    function_pattern: str | None = None      # regex on "function" or "top_function" column
    kind_pattern: str | None = None          # regex on "kind" column
    what_pattern: str | None = None          # regex on "what" column

    # --- Pattern exclusion (regex, case-insensitive) ---
    # Rows matching ANY exclusion pattern are removed.
    exclude_files: str | None = None         # regex: exclude files matching (e.g. "/usr/lib|/lib64")
    exclude_functions: str | None = None     # regex: exclude functions matching (e.g. "^_|^std::")

    # --- Numeric thresholds ---
    # Dict mapping column name to (min, max). Either bound can be None.
    # e.g. {"bytes_leaked": (1024, None)} means bytes_leaked >= 1024
    thresholds: dict[str, tuple[float | None, float | None]] = field(default_factory=dict)

    # --- Sorting ---
    sort_by: str | None = None               # column to sort by (overrides default)
    sort_descending: bool = True             # sort direction

    # --- Pagination ---
    offset: int = 0                          # skip first N rows after filtering
    limit: int | None = None                 # max rows to return (None = all)

    # --- Sampling ---
    sample_n: int | None = None              # random sample of N rows
    sample_fraction: float | None = None     # random fraction (0.0-1.0)
    sample_every: int | None = None          # take every Nth row (systematic sampling)
    sample_seed: int | None = None           # seed for reproducible random sampling
    stratify_by: str | None = None           # stratified sampling: sample_n per group in this column

    # --- Time range (massif) ---
    time_min: int | None = None              # minimum time value
    time_max: int | None = None              # maximum time value

    # --- Memory range (massif) ---
    min_bytes: int | None = None             # minimum total_bytes / heap_bytes
    max_bytes: int | None = None             # maximum total_bytes / heap_bytes

    # --- Stack depth filter (error tools) ---
    min_stack_depth: int | None = None
    max_stack_depth: int | None = None

    # --- Thread filter (helgrind/drd) ---
    thread_ids: list[int] | None = None      # only errors from these threads


def apply_filters(df: pl.DataFrame, spec: FilterSpec) -> pl.DataFrame:
    """Apply a FilterSpec to a Polars DataFrame.

    Returns the filtered, sampled, paginated DataFrame.
    """
    if df.is_empty():
        return df

    # --- Pattern inclusion ---
    df = _apply_pattern(df, spec.file_pattern, ["file", "top_file"])
    df = _apply_pattern(df, spec.function_pattern, ["function", "top_function"])
    df = _apply_pattern(df, spec.kind_pattern, ["kind"])
    df = _apply_pattern(df, spec.what_pattern, ["what"])

    # --- Pattern exclusion ---
    df = _apply_exclude(df, spec.exclude_files, ["file", "top_file"])
    df = _apply_exclude(df, spec.exclude_functions, ["function", "top_function"])

    # --- Numeric thresholds ---
    for col, (lo, hi) in spec.thresholds.items():
        if col in df.columns:
            if lo is not None:
                df = df.filter(pl.col(col) >= lo)
            if hi is not None:
                df = df.filter(pl.col(col) <= hi)

    # --- Time range ---
    if spec.time_min is not None and "time" in df.columns:
        df = df.filter(pl.col("time") >= spec.time_min)
    if spec.time_max is not None and "time" in df.columns:
        df = df.filter(pl.col("time") <= spec.time_max)

    # --- Memory range ---
    bytes_col = _find_column(df, ["total_bytes", "heap_bytes", "bytes_leaked"])
    if bytes_col:
        if spec.min_bytes is not None:
            df = df.filter(pl.col(bytes_col) >= spec.min_bytes)
        if spec.max_bytes is not None:
            df = df.filter(pl.col(bytes_col) <= spec.max_bytes)

    # --- Stack depth ---
    if "stack_depth" in df.columns:
        if spec.min_stack_depth is not None:
            df = df.filter(pl.col("stack_depth") >= spec.min_stack_depth)
        if spec.max_stack_depth is not None:
            df = df.filter(pl.col("stack_depth") <= spec.max_stack_depth)

    # --- Thread filter ---
    if spec.thread_ids is not None and "thread_id" in df.columns:
        df = df.filter(pl.col("thread_id").is_in(spec.thread_ids))

    # --- Sorting ---
    if spec.sort_by and spec.sort_by in df.columns:
        df = df.sort(spec.sort_by, descending=spec.sort_descending)

    # --- Sampling (before pagination) ---
    df = _apply_sampling(df, spec)

    # --- Pagination ---
    if spec.offset > 0:
        df = df.slice(spec.offset)
    if spec.limit is not None:
        df = df.head(spec.limit)

    return df


def build_filter_spec(
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    what_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_bytes: int | None = None,
    max_bytes: int | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int | None = None,
    sample_n: int | None = None,
    sample_fraction: float | None = None,
    sample_every: int | None = None,
    sample_seed: int | None = None,
    stratify_by: str | None = None,
    time_min: int | None = None,
    time_max: int | None = None,
    min_stack_depth: int | None = None,
    max_stack_depth: int | None = None,
    thread_ids: list[int] | None = None,
    thresholds: dict[str, tuple[float | None, float | None]] | None = None,
) -> FilterSpec:
    """Build a FilterSpec from individual parameters (convenience for MCP tools)."""
    return FilterSpec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        kind_pattern=kind_pattern,
        what_pattern=what_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        min_bytes=min_bytes,
        max_bytes=max_bytes,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
        sample_fraction=sample_fraction,
        sample_every=sample_every,
        sample_seed=sample_seed,
        stratify_by=stratify_by,
        time_min=time_min,
        time_max=time_max,
        min_stack_depth=min_stack_depth,
        max_stack_depth=max_stack_depth,
        thread_ids=thread_ids,
        thresholds=thresholds or {},
    )


def describe_active_filters(spec: FilterSpec) -> str:
    """Return a human-readable description of which filters are active."""
    parts = []
    if spec.file_pattern:
        parts.append(f"file =~ /{spec.file_pattern}/i")
    if spec.function_pattern:
        parts.append(f"function =~ /{spec.function_pattern}/i")
    if spec.kind_pattern:
        parts.append(f"kind =~ /{spec.kind_pattern}/i")
    if spec.what_pattern:
        parts.append(f"what =~ /{spec.what_pattern}/i")
    if spec.exclude_files:
        parts.append(f"exclude files =~ /{spec.exclude_files}/i")
    if spec.exclude_functions:
        parts.append(f"exclude functions =~ /{spec.exclude_functions}/i")
    for col, (lo, hi) in spec.thresholds.items():
        if lo is not None and hi is not None:
            parts.append(f"{lo} <= {col} <= {hi}")
        elif lo is not None:
            parts.append(f"{col} >= {lo}")
        elif hi is not None:
            parts.append(f"{col} <= {hi}")
    if spec.min_bytes is not None:
        parts.append(f"min_bytes >= {spec.min_bytes}")
    if spec.max_bytes is not None:
        parts.append(f"max_bytes <= {spec.max_bytes}")
    if spec.time_min is not None:
        parts.append(f"time >= {spec.time_min}")
    if spec.time_max is not None:
        parts.append(f"time <= {spec.time_max}")
    if spec.min_stack_depth is not None:
        parts.append(f"stack_depth >= {spec.min_stack_depth}")
    if spec.max_stack_depth is not None:
        parts.append(f"stack_depth <= {spec.max_stack_depth}")
    if spec.thread_ids is not None:
        parts.append(f"thread_id in {spec.thread_ids}")
    if spec.sort_by:
        direction = "desc" if spec.sort_descending else "asc"
        parts.append(f"sort by {spec.sort_by} {direction}")
    if spec.sample_n is not None:
        parts.append(f"sample {spec.sample_n} rows")
    if spec.sample_fraction is not None:
        parts.append(f"sample {spec.sample_fraction*100:.0f}%")
    if spec.sample_every is not None:
        parts.append(f"every {spec.sample_every}th row")
    if spec.stratify_by:
        parts.append(f"stratified by {spec.stratify_by}")
    if spec.offset > 0:
        parts.append(f"offset {spec.offset}")
    if spec.limit is not None:
        parts.append(f"limit {spec.limit}")
    if not parts:
        return "no filters"
    return ", ".join(parts)


# --- Internal helpers ---


def _find_column(df: pl.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name from candidates that exists in df."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _apply_pattern(df: pl.DataFrame, pattern: str | None, columns: list[str]) -> pl.DataFrame:
    """Keep rows where ANY of the candidate columns match the regex pattern."""
    if pattern is None or df.is_empty():
        return df
    col = _find_column(df, columns)
    if col is None:
        return df
    try:
        return df.filter(
            pl.col(col).cast(pl.Utf8).str.contains(f"(?i){pattern}")
        )
    except Exception:
        return df


def _apply_exclude(df: pl.DataFrame, pattern: str | None, columns: list[str]) -> pl.DataFrame:
    """Remove rows where ANY of the candidate columns match the exclusion regex."""
    if pattern is None or df.is_empty():
        return df
    col = _find_column(df, columns)
    if col is None:
        return df
    try:
        return df.filter(
            ~pl.col(col).cast(pl.Utf8).str.contains(f"(?i){pattern}")
        )
    except Exception:
        return df


def _apply_sampling(df: pl.DataFrame, spec: FilterSpec) -> pl.DataFrame:
    """Apply sampling strategy to DataFrame."""
    if df.is_empty():
        return df

    # Stratified sampling: sample_n rows per group
    if spec.stratify_by and spec.sample_n and spec.stratify_by in df.columns:
        seed = spec.sample_seed or 42
        groups = df[spec.stratify_by].unique().to_list()
        sampled_parts = []
        for group_val in groups:
            group_df = df.filter(pl.col(spec.stratify_by) == group_val)
            n = min(spec.sample_n, len(group_df))
            sampled_parts.append(group_df.sample(n=n, seed=seed))
        if sampled_parts:
            return pl.concat(sampled_parts)
        return df.head(0)

    # Systematic sampling (every Nth)
    if spec.sample_every is not None and spec.sample_every > 0:
        return df.gather_every(spec.sample_every)

    # Random sample by count
    if spec.sample_n is not None:
        n = min(spec.sample_n, len(df))
        return df.sample(n=n, seed=spec.sample_seed)

    # Random sample by fraction
    if spec.sample_fraction is not None:
        frac = max(0.0, min(1.0, spec.sample_fraction))
        return df.sample(fraction=frac, seed=spec.sample_seed)

    return df
