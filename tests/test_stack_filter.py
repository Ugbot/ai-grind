"""Tests for stack-containment filtering (flamegraph.sample_filter).

Row filters ask "is this function named X"; these filters ask "did this sample
come through X". The distinction is the whole point, so the tests are about
semantics — which samples survive, and what the survivors' percentages mean —
not about plumbing.

The motivating case is a whole-process profile that mixes a setup phase with the
phase under study: a shared leaf (memmove) ranks #1 only because most of it is
under the loader. Excluding the leaf by name would delete the part that matters;
excluding the phase by call path is the only honest cut.
"""

from __future__ import annotations

import re

import polars as pl
import pytest

from devtools_mcp.flamegraph.sample_filter import StackFilter, filter_samples
from devtools_mcp.flamegraph.tree import function_frame
from devtools_mcp.models import StackSample


def _mixed_phase_run() -> list[StackSample]:
    """A profile that is 1/3 load phase, 2/3 execution — both ending in memmove.

    memmove is 40 of 100 samples and would rank first raw, but 30 of those 40 are
    inside the loader. Under an execution-only view it must fall behind the real
    execution hotspot.
    """
    return [
        StackSample(frames=["main", "load_parquet", "decompress_page", "_platform_memmove"], weight=30),
        StackSample(frames=["main", "load_parquet", "build_block_zonemaps"], weight=4),
        StackSample(frames=["main", "run_query", "join_probe_typed"], weight=36),
        StackSample(frames=["main", "run_query", "join_probe_typed", "_platform_memmove"], weight=10),
        StackSample(frames=["main", "run_query", "swtch_pri"], weight=20),
    ]


class TestStackContainment:
    def test_excludes_a_sample_whose_leaf_is_innocent(self):
        # The dropped sample's LEAF is _platform_memmove — a name that appears in
        # kept samples too. Only the call path distinguishes them, which is
        # exactly what a row-level exclude_functions cannot express.
        kept, cut = filter_samples(_mixed_phase_run(), stack_exclude="load_parquet")
        assert cut.dropped_weight == 34
        assert cut.kept_weight == 66
        leaves = {s.frames[-1] for s in kept}
        assert "_platform_memmove" in leaves  # the execution-side memmove survives
        assert all("load_parquet" not in s.frames for s in kept)

    def test_include_keeps_only_matching_call_paths(self):
        kept, cut = filter_samples(_mixed_phase_run(), stack_include="run_query")
        assert cut.kept_weight == 66
        assert cut.dropped_weight == 34
        assert all("run_query" in s.frames for s in kept)

    def test_include_and_exclude_combined_exclude_wins(self):
        # A sample matching BOTH is dropped: exclusion names a phase that is not
        # the subject of the measurement, so it overrides inclusion.
        samples = [
            StackSample(frames=["main", "run_query", "helper"], weight=10),
            StackSample(frames=["main", "run_query", "load_parquet", "helper"], weight=7),
            StackSample(frames=["main", "other"], weight=3),
        ]
        kept, cut = filter_samples(samples, stack_include="run_query", stack_exclude="load_parquet")
        assert [s.weight for s in kept] == [10]
        assert cut.kept_weight == 10
        assert cut.dropped_weight == 10  # 7 excluded + 3 not included

    def test_matches_any_frame_not_just_the_leaf(self):
        samples = [StackSample(frames=["main", "deep", "middle", "leaf"], weight=5)]
        for pattern in ("main", "middle", "leaf"):
            kept, _ = filter_samples(samples, stack_exclude=pattern)
            assert kept == [], f"{pattern!r} should have matched a frame"

    def test_regex_alternation_and_case_insensitivity(self):
        # Matches the row filters' (?i)-style contains semantics.
        samples = [
            StackSample(frames=["main", "Register_Bolt_Batch"], weight=1),
            StackSample(frames=["main", "column_ndv"], weight=1),
            StackSample(frames=["main", "execute"], weight=1),
        ]
        kept, cut = filter_samples(samples, stack_exclude="register_bolt_batch|column_ndv")
        assert [s.frames[-1] for s in kept] == ["execute"]
        assert cut.dropped_stacks == 2

    def test_no_patterns_is_a_no_op(self):
        samples = _mixed_phase_run()
        kept, cut = filter_samples(samples)
        assert kept is samples
        assert not cut.active
        assert cut.describe() == ""
        assert cut.kept_weight == 100

    def test_invalid_regex_raises_rather_than_silently_passing_everything(self):
        # Swallowing the error would present an UNfiltered profile as filtered —
        # the worst possible failure mode for this tool.
        with pytest.raises(re.error):
            filter_samples(_mixed_phase_run(), stack_exclude="load_parquet(")


class TestRenormalization:
    """Filtering happens before percentages, so shares describe the kept universe."""

    def test_percentages_renormalize_to_the_kept_subset(self):
        kept, cut = filter_samples(_mixed_phase_run(), stack_exclude="load_parquet")
        df = function_frame(kept)
        by = {r["function"]: r for r in df.iter_rows(named=True)}
        # 34% of samples dropped; the remaining 66 must sum to 100%, not 66%.
        assert by["main"]["total_pct"] == 100.0
        assert cut.dropped_pct == 34.0
        # Self percentages of all leaves must sum to 100 too.
        assert round(df["self_pct"].sum(), 1) == 100.0

    def test_execution_only_view_reorders_the_ranking(self):
        raw = function_frame(_mixed_phase_run())
        raw_by = {r["function"]: r for r in raw.iter_rows(named=True)}
        # Raw, memmove (40) outranks the real execution hotspot (36).
        assert raw_by["_platform_memmove"]["self"] > raw_by["join_probe_typed"]["self"]

        kept, _ = filter_samples(_mixed_phase_run(), stack_exclude="load_parquet")
        exec_by = {r["function"]: r for r in function_frame(kept).iter_rows(named=True)}
        # Execution-only, memmove is 10/66 = 15.15% and drops behind join_probe.
        assert exec_by["_platform_memmove"]["self_pct"] == 15.15
        assert exec_by["join_probe_typed"]["self_pct"] == 54.55
        assert exec_by["join_probe_typed"]["self"] > exec_by["_platform_memmove"]["self"]

    def test_normalization_is_per_run_after_filtering(self):
        # Two runs, same shape, different capture lengths AND different amounts of
        # load phase. After filtering, their execution profiles must be identical.
        short = _mixed_phase_run()
        long_run = [
            StackSample(frames=s.frames, weight=s.weight * 10 if "load_parquet" in s.frames else s.weight * 3)
            for s in _mixed_phase_run()
        ]
        a, _ = filter_samples(short, stack_exclude="load_parquet")
        b, _ = filter_samples(long_run, stack_exclude="load_parquet")
        fa = function_frame(a).sort("function").select(["function", "self_pct", "total_pct"])
        fb = function_frame(b).sort("function").select(["function", "self_pct", "total_pct"])
        assert fa.equals(fb)


class TestStackFilterReporting:
    """The header must make a filtered view impossible to mistake for the whole."""

    def test_describe_names_the_pattern_and_the_cost(self):
        _, cut = filter_samples(_mixed_phase_run(), stack_exclude="load_parquet")
        text = cut.describe()
        assert "kept 66 of 100 samples" in text
        assert "34 dropped, 34.0%" in text
        assert "load_parquet" in text
        assert "renormalized" in text

    def test_accumulates_across_runs(self):
        # devtools_aggregate filters each run separately, then reports one total.
        _, a = filter_samples(_mixed_phase_run(), stack_exclude="load_parquet")
        _, b = filter_samples(_mixed_phase_run(), stack_exclude="load_parquet")
        both = a + b
        assert both.kept_weight == 132
        assert both.dropped_weight == 68
        assert both.dropped_pct == 34.0
        assert both.stack_exclude == "load_parquet"
        assert both.active

    def test_inactive_filter_reports_nothing(self):
        assert StackFilter().describe() == ""
        assert not StackFilter(kept_weight=10).active


class TestMultiTagIntersection:
    """A campaign tags a run by suite, by query and by sweep date at once.

    Selecting the intersection is the common case, so `tags` must require ALL of
    them — a run carrying only some of the labels is not the run you asked for.
    """

    def _runs(self) -> list[tuple[str, list[str]]]:
        return [
            ("r_q1_new", ["dtrace", "q1", "sweep-20260809"]),
            ("r_q2_new", ["dtrace", "q2", "sweep-20260809"]),
            ("r_q1_old", ["dtrace", "q1", "sweep-20260701"]),
            ("r_other", ["perf", "q1"]),
        ]

    def _select(self, required: list[str]) -> list[str]:
        # Mirrors devtools_aggregate's selection predicate exactly.
        return [rid for rid, tags in self._runs() if not any(t not in tags for t in required)]

    def test_all_tags_must_match(self):
        assert self._select(["dtrace", "sweep-20260809"]) == ["r_q1_new", "r_q2_new"]
        assert self._select(["q1", "sweep-20260809"]) == ["r_q1_new"]

    def test_single_tag_still_selects_broadly(self):
        assert self._select(["q1"]) == ["r_q1_new", "r_q1_old", "r_other"]

    def test_empty_selection_matches_everything(self):
        assert len(self._select([])) == 4

    def test_impossible_intersection_is_empty(self):
        assert self._select(["perf", "sweep-20260809"]) == []


class TestAggregateWithStackFilter:
    """End-to-end aggregate math on stack-filtered runs.

    Reproduces in miniature the campaign finding: raw, the shared leaf tops the
    suite-wide ranking; execution-only, the real hotspot does — and every kept
    run still normalizes to 100%.
    """

    def _agg(self, frames: list[pl.DataFrame], col: str = "self_pct") -> pl.DataFrame:
        stacked = pl.concat(
            [f.select(["function", col]).with_columns(pl.lit(f"r{i}").alias("run"))
             for i, f in enumerate(frames)],
            how="vertical",
        )
        n = len(frames)
        return (
            stacked.group_by("function")
            .agg([
                pl.len().alias("runs"),
                pl.col(col).mean().round(2).alias("mean_pct"),
                pl.col(col).max().round(2).alias("max_pct"),
            ])
            .with_columns(
                (pl.col("mean_pct") * pl.col("runs") / n).round(2).alias("total_share")
            )
            .sort("total_share", descending=True)
        )

    def test_filtered_aggregate_demotes_the_shared_leaf(self):
        runs = [_mixed_phase_run() for _ in range(3)]
        raw = self._agg([function_frame(r) for r in runs])
        raw_by = {r["function"]: r for r in raw.iter_rows(named=True)}
        assert raw_by["_platform_memmove"]["total_share"] > raw_by["join_probe_typed"]["total_share"]

        cut = StackFilter()
        frames = []
        for r in runs:
            kept, c = filter_samples(r, stack_exclude="load_parquet|build_block_zonemaps")
            cut = cut + c
            frames.append(function_frame(kept))
        agg = self._agg(frames)
        by = {r["function"]: r for r in agg.iter_rows(named=True)}
        assert by["join_probe_typed"]["total_share"] > by["_platform_memmove"]["total_share"]
        assert by["join_probe_typed"]["runs"] == 3
        assert cut.kept_weight == 198  # 66 per run
        assert cut.dropped_weight == 102

    def test_every_kept_run_sums_to_one_hundred(self):
        for weightings in ([1, 1, 1], [5, 1, 9]):
            runs = [
                [StackSample(frames=s.frames, weight=s.weight * w) for s in _mixed_phase_run()]
                for w in weightings
            ]
            for r in runs:
                kept, _ = filter_samples(r, stack_exclude="load_parquet")
                assert round(function_frame(kept)["self_pct"].sum(), 1) == 100.0
