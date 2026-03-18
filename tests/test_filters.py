"""Tests for the filtering and sampling engine."""

from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime

import polars as pl
import pytest

from devtools_mcp.filters import FilterSpec, apply_filters, build_filter_spec, describe_active_filters
from devtools_mcp.models import RunBase
from devtools_mcp.valgrind import analysis
from devtools_mcp.valgrind.parsers import (
    parse_cachegrind,
    parse_callgrind,
    parse_massif,
    parse_memcheck_xml,
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


class TestPatternFilters:
    def test_file_pattern_includes_matching(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.memcheck_errors_df(result)
        spec = build_filter_spec(file_pattern="main\\.c")
        filtered = apply_filters(df, spec)
        # All rows should have main.c in top_file
        for val in filtered["top_file"].to_list():
            if val is not None:
                assert "main.c" in val

    def test_function_pattern_includes_matching(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        df = analysis.callgrind_df(result)
        if df.is_empty():
            pytest.skip("No callgrind data")
        # Pick a function that exists
        some_fn = df["function"][0]
        prefix = some_fn[:4]
        spec = build_filter_spec(function_pattern=prefix)
        filtered = apply_filters(df, spec)
        assert len(filtered) > 0
        for fn in filtered["function"].to_list():
            assert prefix.lower() in fn.lower()

    def test_kind_pattern_filters(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.memcheck_errors_df(result)
        spec = build_filter_spec(kind_pattern="Leak")
        filtered = apply_filters(df, spec)
        for kind in filtered["kind"].to_list():
            assert "Leak" in kind

    def test_exclude_files_removes_matching(self, cachegrind_file: str):
        result = parse_cachegrind(cachegrind_file, _run_base("cachegrind"))
        df = analysis.cachegrind_df(result)
        if df.is_empty():
            pytest.skip("No cachegrind data")
        some_file = df["file"][0]
        spec = build_filter_spec(exclude_files=some_file.replace(".", "\\."))
        filtered = apply_filters(df, spec)
        for f in filtered["file"].to_list():
            assert f != some_file

    def test_exclude_functions_removes_matching(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        df = analysis.callgrind_df(result)
        if df.is_empty():
            pytest.skip("No callgrind data")
        some_fn = df["function"][0]
        spec = build_filter_spec(exclude_functions=some_fn)
        filtered = apply_filters(df, spec)
        assert some_fn not in filtered["function"].to_list()


class TestThresholdFilters:
    def test_min_bytes_filters(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.memcheck_errors_df(result)
        spec = build_filter_spec(min_bytes=100)
        filtered = apply_filters(df, spec)
        for val in filtered["bytes_leaked"].to_list():
            assert val >= 100

    def test_threshold_on_arbitrary_column(self):
        df = pl.DataFrame(
            {
                "function": [f"fn_{i}" for i in range(20)],
                "ir": [random.randint(100, 100000) for _ in range(20)],
            }
        )
        spec = build_filter_spec(thresholds={"ir": (5000, 50000)})
        filtered = apply_filters(df, spec)
        for val in filtered["ir"].to_list():
            assert 5000 <= val <= 50000

    def test_time_range_filters_massif(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        df = analysis.massif_timeline_df(result)
        if df.is_empty():
            pytest.skip("No massif data")
        times = df["time"].to_list()
        mid = times[len(times) // 2]
        spec = build_filter_spec(time_min=mid)
        filtered = apply_filters(df, spec)
        for t in filtered["time"].to_list():
            assert t >= mid


class TestSampling:
    def test_sample_n(self):
        df = pl.DataFrame(
            {
                "x": list(range(100)),
                "y": [random.random() for _ in range(100)],
            }
        )
        spec = build_filter_spec(sample_n=10, sample_seed=42)
        filtered = apply_filters(df, spec)
        assert len(filtered) == 10

    def test_sample_every(self):
        df = pl.DataFrame({"x": list(range(100))})
        spec = build_filter_spec(sample_every=5)
        filtered = apply_filters(df, spec)
        assert len(filtered) == 20
        assert filtered["x"].to_list() == list(range(0, 100, 5))

    def test_sample_fraction(self):
        df = pl.DataFrame({"x": list(range(1000))})
        spec = build_filter_spec(sample_fraction=0.1, sample_seed=42)
        filtered = apply_filters(df, spec)
        # Should be roughly 100 rows (10%), allow some variance
        assert 50 <= len(filtered) <= 150

    def test_stratified_sampling(self):
        df = pl.DataFrame(
            {
                "kind": ["A"] * 50 + ["B"] * 30 + ["C"] * 20,
                "value": [random.randint(1, 100) for _ in range(100)],
            }
        )
        spec = build_filter_spec(sample_n=5, stratify_by="kind", sample_seed=42)
        filtered = apply_filters(df, spec)
        # Should have 5 from each group = 15 total
        assert len(filtered) == 15
        kind_counts = filtered.group_by("kind").agg(pl.len().alias("n"))
        for row in kind_counts.iter_rows():
            assert row[1] == 5


class TestPagination:
    def test_offset_and_limit(self):
        df = pl.DataFrame({"x": list(range(50))})
        spec = build_filter_spec(offset=10, limit=5)
        filtered = apply_filters(df, spec)
        assert len(filtered) == 5
        assert filtered["x"].to_list() == [10, 11, 12, 13, 14]

    def test_offset_only(self):
        df = pl.DataFrame({"x": list(range(20))})
        spec = build_filter_spec(offset=15)
        filtered = apply_filters(df, spec)
        assert len(filtered) == 5

    def test_limit_only(self):
        df = pl.DataFrame({"x": list(range(50))})
        spec = build_filter_spec(limit=10)
        filtered = apply_filters(df, spec)
        assert len(filtered) == 10


class TestSorting:
    def test_sort_override(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        df = analysis.callgrind_df(result)
        if df.is_empty() or "self_Ir" not in df.columns:
            pytest.skip("No callgrind data")
        spec = build_filter_spec(sort_by="self_Ir", sort_descending=True)
        filtered = apply_filters(df, spec)
        costs = filtered["self_Ir"].to_list()
        assert costs == sorted(costs, reverse=True)

    def test_sort_ascending(self):
        df = pl.DataFrame({"x": [3, 1, 4, 1, 5, 9, 2, 6]})
        spec = build_filter_spec(sort_by="x", sort_descending=False)
        filtered = apply_filters(df, spec)
        assert filtered["x"].to_list() == sorted([3, 1, 4, 1, 5, 9, 2, 6])


class TestCombinedFilters:
    def test_filter_then_sample(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        df = analysis.memcheck_errors_df(result)
        # Filter for leaks, then sample
        spec = build_filter_spec(kind_pattern="Leak", sample_n=2, sample_seed=42)
        filtered = apply_filters(df, spec)
        assert len(filtered) <= 2
        for kind in filtered["kind"].to_list():
            assert "Leak" in kind

    def test_filter_sort_paginate(self):
        df = pl.DataFrame(
            {
                "function": [f"fn_{i}" for i in range(50)],
                "file": ["a.c"] * 25 + ["b.c"] * 25,
                "ir": [random.randint(100, 100000) for _ in range(50)],
            }
        )
        spec = build_filter_spec(
            file_pattern="a\\.c",
            sort_by="ir",
            sort_descending=True,
            offset=5,
            limit=10,
        )
        filtered = apply_filters(df, spec)
        assert len(filtered) <= 10
        for f in filtered["file"].to_list():
            assert "a.c" in f
        # Check sorted
        irs = filtered["ir"].to_list()
        assert irs == sorted(irs, reverse=True)

    def test_empty_df_handled(self):
        df = pl.DataFrame(schema={"x": pl.Int64, "y": pl.Utf8})
        spec = build_filter_spec(file_pattern="anything", sample_n=5, limit=10)
        filtered = apply_filters(df, spec)
        assert filtered.is_empty()


class TestDescribeFilters:
    def test_no_filters(self):
        spec = FilterSpec()
        assert describe_active_filters(spec) == "no filters"

    def test_describes_all_active(self):
        spec = build_filter_spec(
            file_pattern="main",
            exclude_functions="^_",
            min_bytes=1024,
            sort_by="ir",
            limit=10,
            sample_every=3,
        )
        desc = describe_active_filters(spec)
        assert "main" in desc
        assert "^_" in desc
        assert "1024" in desc
        assert "sort by ir" in desc
        assert "limit 10" in desc
        assert "every 3th" in desc
