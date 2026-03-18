"""DataFrame and table formatters."""

from __future__ import annotations

import polars as pl

from devtools_mcp.filters import FilterSpec, describe_active_filters


def format_dataframe(df: pl.DataFrame, title: str = "", max_rows: int = 30) -> str:
    """Format a Polars DataFrame as a markdown table."""
    parts: list[str] = []
    if title:
        parts.append(f"**{title}**")
        parts.append("")

    if df.is_empty():
        parts.append("No data.")
        return "\n".join(parts)

    display_df = df.head(max_rows)
    headers = display_df.columns
    parts.append("| " + " | ".join(headers) + " |")
    parts.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in display_df.iter_rows():
        cells = []
        for val in row:
            if isinstance(val, float):
                cells.append(f"{val:.2f}")
            elif isinstance(val, int) and abs(val) > 9999:
                cells.append(f"{val:,}")
            else:
                cells.append(str(val) if val is not None else "")
        parts.append("| " + " | ".join(cells) + " |")

    if len(df) > max_rows:
        parts.append(f"\n*Showing {max_rows} of {len(df)} rows*")

    return "\n".join(parts)


def format_comparison(comparison_df: pl.DataFrame, title: str = "Comparison") -> str:
    """Format a comparison DataFrame."""
    return format_dataframe(comparison_df, title=title)


def format_filtered(
    df: pl.DataFrame,
    title: str,
    spec: FilterSpec,
    max_rows: int = 50,
) -> str:
    """Format a DataFrame with filter description header."""
    desc = describe_active_filters(spec)
    header = f"**{title}**"
    if desc != "no filters":
        header += f"\nFilters: {desc}"
    header += f"\nTotal rows after filtering: {len(df)}"
    return header + "\n\n" + format_dataframe(df, max_rows=max_rows)
