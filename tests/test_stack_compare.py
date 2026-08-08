"""Tests for the generic stack-based run comparison plumbing.

Covers the three pieces added for cross-run sampling comparison:
- flamegraph.tree.function_frame (per-function self/total + normalized %)
- dtrace frame offset normalization (one function, not N offset buckets)
- the join/delta math devtools_compare builds on top of them
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import polars as pl

from devtools_mcp.dtrace.analysis import dtrace_stack_samples
from devtools_mcp.dtrace.models import DTraceResult, DTraceStackTrace
from devtools_mcp.flamegraph.tree import function_frame
from devtools_mcp.models import StackSample


def _dtrace_result(stacks: list[DTraceStackTrace]) -> DTraceResult:
    return DTraceResult(
        run_id=str(uuid.uuid4()),
        suite="dtrace",
        tool="cpu",
        binary="./test",
        timestamp=datetime.now(UTC),
        stacks=stacks,
    )


class TestFunctionFrame:
    def _samples(self) -> list[StackSample]:
        return [
            StackSample(frames=["main", "parse", "lex"], weight=10),
            StackSample(frames=["main", "parse"], weight=5),
            StackSample(frames=["main", "exec", "probe"], weight=85),
        ]

    def test_columns_and_totals(self):
        df = function_frame(self._samples())
        assert set(df.columns) == {"function", "self", "total", "self_pct", "total_pct"}
        row = df.filter(pl.col("function") == "main").row(0, named=True)
        # main is on every stack: inclusive == whole run, exclusive == 0.
        assert row["total"] == 100
        assert row["self"] == 0
        assert row["total_pct"] == 100.0

    def test_self_vs_total_split(self):
        df = function_frame(self._samples())
        parse = df.filter(pl.col("function") == "parse").row(0, named=True)
        assert parse["self"] == 5      # the stack ending at parse
        assert parse["total"] == 15    # plus the lex child
        probe = df.filter(pl.col("function") == "probe").row(0, named=True)
        assert probe["self"] == probe["total"] == 85

    def test_pct_normalized_per_run(self):
        # Same shape, 10x the weight: percentages must be identical.
        small = function_frame(self._samples())
        big = function_frame(
            [StackSample(frames=s.frames, weight=s.weight * 10) for s in self._samples()]
        )
        a = small.sort("function").select(["function", "self_pct", "total_pct"])
        b = big.sort("function").select(["function", "self_pct", "total_pct"])
        assert a.equals(b)

    def test_empty_input(self):
        df = function_frame([])
        assert df.height == 0
        assert "function" in df.columns


class TestDtraceOffsetNormalization:
    def test_offsets_stripped_and_merged(self):
        # The same function at three instruction offsets must aggregate as ONE
        # function — with offsets kept, a hot function fragments into offset
        # buckets and every per-function view under-reports it.
        result = _dtrace_result(
            [
                DTraceStackTrace(frames=["mod`probe+0x4d4", "mod`caller+0x10"], count=3),
                DTraceStackTrace(frames=["mod`probe+0x5ac", "mod`caller+0x10"], count=4),
                DTraceStackTrace(frames=["mod`probe+0x59c", "mod`caller+0x24"], count=5),
            ]
        )
        samples = dtrace_stack_samples(result)
        df = function_frame(samples)
        probe = df.filter(pl.col("function") == "mod`probe")
        assert probe.height == 1
        assert probe.row(0, named=True)["total"] == 12

    def test_reversal_to_root_first(self):
        result = _dtrace_result(
            [DTraceStackTrace(frames=["leaf", "mid", "root"], count=1)]
        )
        (sample,) = dtrace_stack_samples(result)
        assert sample.frames == ["root", "mid", "leaf"]

    def test_non_offset_plus_preserved(self):
        # A '+' that is not a trailing hex offset (e.g. operator+) must survive.
        result = _dtrace_result(
            [DTraceStackTrace(frames=["mod`op::operator+(int)+0x1c"], count=1)]
        )
        (sample,) = dtrace_stack_samples(result)
        assert sample.frames == ["mod`op::operator+(int)"]


class TestCompareJoinMath:
    def test_delta_math_with_disjoint_functions(self):
        # Mirrors the devtools_compare join: full outer on function, nulls -> 0,
        # normalized-percent deltas. A function present in only one run must
        # appear with the other side zeroed, not vanish.
        fa = function_frame([StackSample(frames=["m", "old_path"], weight=80),
                             StackSample(frames=["m", "shared"], weight=20)]).rename(
            {"self": "self_a", "total": "total_a",
             "self_pct": "self_pct_a", "total_pct": "total_pct_a"})
        fb = function_frame([StackSample(frames=["m", "new_path"], weight=40),
                             StackSample(frames=["m", "shared"], weight=60)]).rename(
            {"self": "self_b", "total": "total_b",
             "self_pct": "self_pct_b", "total_pct": "total_pct_b"})
        df = fa.join(fb, on="function", how="full", coalesce=True).fill_null(0)
        df = df.with_columns(
            (pl.col("total_pct_b") - pl.col("total_pct_a")).round(2).alias("total_pct_delta")
        )
        by = {r["function"]: r for r in df.iter_rows(named=True)}
        assert by["old_path"]["total_pct_delta"] == -80.0
        assert by["new_path"]["total_pct_delta"] == 40.0
        assert by["shared"]["total_pct_delta"] == 40.0
        assert by["m"]["total_pct_delta"] == 0.0
