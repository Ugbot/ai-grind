"""Tests for the ETW (PerfView CSV) parser and analysis."""

from __future__ import annotations

import random

from devtools_mcp.etw.analysis import etw_hotspots_df, etw_stack_samples
from devtools_mcp.etw.models import EtwResult
from devtools_mcp.etw.parsers import is_synthetic, parse_perfview_csv, shorten, split_module
from devtools_mcp.models import StackSample


def _csv(n: int = 8) -> str:
    funcs = ["memset", "memcpy", "zone_probe", "put", "serialize", "deserialize"]
    mods = ["ucrtbase", "bench_app", "ntoskrnl"]
    lines = ["Name,Exc,Exc%,Inc,Inc%,First,Last"]
    # a couple of synthetic pseudo-nodes that must be filtered out
    lines.append("Process bench_app (1234),0,0.00,1000,100.00,0,500")
    lines.append("ucrtbase!?,0,0.00,50,5.00,0,500")
    for _ in range(n):
        mod, fn = random.choice(mods), random.choice(funcs)
        exc = round(random.uniform(0.5, 25.0), 2)
        inc = round(exc + random.uniform(0, 20.0), 2)
        lines.append(f"{mod}!{fn},{exc * 10:.0f},{exc:.2f},{inc * 10:.0f},{inc:.2f},0,500")
    return "\n".join(lines)


class TestParsePerfviewCsv:
    def test_parses_rows(self):
        samples = parse_perfview_csv(_csv(6))
        # 6 real + 2 synthetic = 8 parsed (filtering happens in analysis/formatter)
        assert len(samples) == 8

    def test_module_function_split(self):
        samples = parse_perfview_csv("Name,Exc,Exc%,Inc,Inc%,First,Last\nucrtbase!memset,10,5.0,20,9.0,0,1\n")
        assert samples[0].module == "ucrtbase"
        assert samples[0].function == "memset"

    def test_skips_malformed(self):
        bad = "Name,Exc,Exc%,Inc,Inc%\nfoo!bar,notanum,x,y,z\ngood!fn,1,2.0,3,4.0\n"
        samples = parse_perfview_csv(bad)
        assert len(samples) == 1
        assert samples[0].function == "fn"

    def test_empty(self):
        assert parse_perfview_csv("") == []


class TestHelpers:
    def test_split_module_no_bang(self):
        assert split_module("bareword") == ("", "bareword")

    def test_is_synthetic(self):
        assert is_synthetic("ucrtbase!?")
        assert is_synthetic("Process foo (12)")
        assert is_synthetic("Thread (1)")
        assert not is_synthetic("app!real_fn")

    def test_shorten_strips_templates(self):
        long = "app!std::vector<std::pair<int,std::string>>::push_back"
        out = shorten(long)
        assert "<>" in out
        assert len(out) <= 110


class TestAnalysis:
    def test_hotspots_df_filters_synthetic_and_aliases(self):
        samples = parse_perfview_csv(_csv(5))
        df = etw_hotspots_df(EtwResult(run_id="r", binary="app", samples=samples))
        assert "function" in df.columns and "value" in df.columns
        assert df.height == 5  # synthetic rows dropped

    def test_stack_samples_passthrough(self):
        res = EtwResult(run_id="r", binary="app", stack_samples=[StackSample(frames=["a", "b"], weight=3)])
        assert len(etw_stack_samples(res)) == 1

    def test_empty_df_schema(self):
        df = etw_hotspots_df(EtwResult(run_id="r", binary="app"))
        assert df.is_empty()
        assert "function" in df.columns
