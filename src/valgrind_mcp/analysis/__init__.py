"""Polars-based analysis functions for all Valgrind tool outputs."""

from valgrind_mcp.analysis.cachegrind import (
    cache_miss_rates,
    cachegrind_df,
)
from valgrind_mcp.analysis.callgrind import (
    call_graph_summary,
    callgrind_df,
    hotspots,
)
from valgrind_mcp.analysis.comparison import (
    compare_callgrind,
    compare_massif,
    compare_memcheck,
)
from valgrind_mcp.analysis.massif import (
    massif_timeline_df,
    peak_allocations,
)
from valgrind_mcp.analysis.memcheck import (
    errors_by_file,
    errors_by_function,
    errors_by_kind,
    memcheck_errors_df,
)
from valgrind_mcp.analysis.threadcheck import (
    thread_errors_by_function,
    thread_errors_by_kind,
    threadcheck_errors_df,
)

__all__ = [
    "callgrind_df",
    "call_graph_summary",
    "cache_miss_rates",
    "cachegrind_df",
    "compare_callgrind",
    "compare_massif",
    "compare_memcheck",
    "errors_by_file",
    "errors_by_function",
    "errors_by_kind",
    "hotspots",
    "massif_timeline_df",
    "memcheck_errors_df",
    "peak_allocations",
    "thread_errors_by_function",
    "thread_errors_by_kind",
    "threadcheck_errors_df",
]
