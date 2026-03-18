"""Tests for Polars-based analysis functions."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import polars as pl

from devtools_mcp.models import RunBase
from devtools_mcp.valgrind import analysis
from devtools_mcp.valgrind.parsers import (
    parse_cachegrind,
    parse_callgrind,
    parse_massif,
    parse_memcheck_xml,
    parse_threadcheck_xml,
)


def _run_base(tool: str) -> RunBase:
    return RunBase(
        run_id=str(uuid.uuid4()),
        suite="valgrind",
        tool=tool,
        binary="./test_binary",
        args=[],
        valgrind_args=[],
        timestamp=datetime.now(UTC),
        exit_code=0,
        duration_seconds=1.0,
    )


class TestMemcheckAnalysis:
    def test_errors_df_has_correct_columns(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.memcheck_errors_df(result)
        assert "error_id" in df.columns
        assert "kind" in df.columns
        assert "bytes_leaked" in df.columns
        assert "top_function" in df.columns

    def test_errors_df_row_count_matches(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.memcheck_errors_df(result)
        assert len(df) == len(result.errors)

    def test_errors_by_kind_groups_correctly(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.errors_by_kind(result)
        if not df.is_empty():
            total_count = df["count"].sum()
            assert total_count == len(result.errors)

    def test_errors_by_function_respects_top_n(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.errors_by_function(result, top_n=3)
        assert len(df) <= 3

    def test_errors_by_file_returns_dataframe(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.errors_by_file(result)
        assert isinstance(df, pl.DataFrame)


class TestThreadCheckAnalysis:
    def test_threadcheck_df_has_thread_id(self, helgrind_xml_file: str):
        result = parse_threadcheck_xml(helgrind_xml_file, _run_base("helgrind"))
        df = analysis.threadcheck_errors_df(result)
        assert "thread_id" in df.columns

    def test_thread_errors_by_kind(self, helgrind_xml_file: str):
        result = parse_threadcheck_xml(helgrind_xml_file, _run_base("helgrind"))
        df = analysis.thread_errors_by_kind(result)
        if not df.is_empty():
            assert "kind" in df.columns
            assert "count" in df.columns


class TestCallgrindAnalysis:
    def test_callgrind_df_has_cost_columns(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        df = analysis.callgrind_df(result)
        assert "function" in df.columns
        assert "self_Ir" in df.columns

    def test_hotspots_sorted_descending(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        df = analysis.hotspots(result, event="Ir", top_n=10)
        if len(df) > 1:
            costs = df["self_Ir"].to_list()
            assert costs == sorted(costs, reverse=True)

    def test_hotspots_has_percentage(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        df = analysis.hotspots(result, event="Ir")
        assert "self_pct" in df.columns
        assert "inclusive_pct" in df.columns

    def test_call_graph_summary(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        df = analysis.call_graph_summary(result)
        if not df.is_empty():
            assert "caller" in df.columns
            assert "callee" in df.columns
            assert "call_count" in df.columns


class TestCachegrindAnalysis:
    def test_cachegrind_df_has_miss_rates(self, cachegrind_file: str):
        result = parse_cachegrind(cachegrind_file, _run_base("cachegrind"))
        df = analysis.cachegrind_df(result)
        assert "i1_miss_rate" in df.columns
        assert "d1_read_miss_rate" in df.columns

    def test_cache_miss_rates_by_function(self, cachegrind_file: str):
        result = parse_cachegrind(cachegrind_file, _run_base("cachegrind"))
        df = analysis.cache_miss_rates(result)
        if not df.is_empty():
            assert "function" in df.columns
            assert "total_ir" in df.columns
            assert "i1_miss_pct" in df.columns


class TestMassifAnalysis:
    def test_timeline_df_columns(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        df = analysis.massif_timeline_df(result)
        assert "time" in df.columns
        assert "heap_bytes" in df.columns
        assert "total_bytes" in df.columns

    def test_timeline_row_count(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        df = analysis.massif_timeline_df(result)
        assert len(df) == len(result.snapshots)

    def test_peak_allocations(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        allocs = analysis.peak_allocations(result)
        assert len(allocs) > 0
        assert allocs[0]["depth"] == 0


class TestComparisons:
    def test_compare_memcheck(self, memcheck_xml_file: str):
        # Parse same file twice (simulating two runs)
        result_a = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        result_b = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.compare_memcheck(result_a, result_b)
        assert isinstance(df, pl.DataFrame)
        if "count_delta" in df.columns and not df.is_empty():
            # Same file, so deltas should be 0
            assert df["count_delta"].sum() == 0

    def test_compare_callgrind(self, callgrind_file: str):
        result_a = parse_callgrind(callgrind_file, _run_base("callgrind"))
        result_b = parse_callgrind(callgrind_file, _run_base("callgrind"))
        df = analysis.compare_callgrind(result_a, result_b, event="Ir")
        assert "cost_delta" in df.columns

    def test_compare_massif(self, massif_file: str):
        result_a = parse_massif(massif_file, _run_base("massif"))
        result_b = parse_massif(massif_file, _run_base("massif"))
        info = analysis.compare_massif(result_a, result_b)
        assert info["peak_delta_bytes"] == 0
