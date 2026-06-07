"""Tests for the Node backend: V8 .cpuprofile / .heapprofile parsing."""

from __future__ import annotations

import json

from devtools_mcp.node.analysis import node_hotspots_df
from devtools_mcp.node.models import NodeResult
from devtools_mcp.node.parsers import parse_cpuprofile, parse_heapprofile


def _cf(name: str, url: str = "app.js", line: int = 1) -> dict:
    return {"functionName": name, "url": url, "lineNumber": line}


class TestCpuProfile:
    # root -> main -> {work(leaf), io(leaf)}
    PROFILE = json.dumps({
        "nodes": [
            {"id": 1, "callFrame": _cf("(root)"), "children": [2]},
            {"id": 2, "callFrame": _cf("main"), "children": [3, 4]},
            {"id": 3, "callFrame": _cf("work")},
            {"id": 4, "callFrame": _cf("io")},
        ],
        "samples": [3, 3, 3, 4],  # work x3, io x1
        "timeDeltas": [100, 100, 100, 100],
    })

    def test_aggregates_samples(self):
        samples = parse_cpuprofile(self.PROFILE)
        total = sum(s.weight for s in samples)
        assert total == 4
        assert len(samples) == 2  # two distinct stacks

    def test_root_first_order(self):
        samples = parse_cpuprofile(self.PROFILE)
        work = next(s for s in samples if s.frames[-1] == "work")
        assert work.frames[0] == "(root)"
        assert work.frames == ["(root)", "main", "work"]
        assert work.weight == 3

    def test_hitcount_fallback_when_no_samples(self):
        prof = json.dumps({"nodes": [
            {"id": 1, "callFrame": _cf("(root)"), "children": [2]},
            {"id": 2, "callFrame": _cf("hot"), "hitCount": 7},
        ]})
        samples = parse_cpuprofile(prof)
        assert sum(s.weight for s in samples) == 7

    def test_anonymous_naming(self):
        prof = json.dumps({"nodes": [{"id": 1, "callFrame": _cf("", "lib.js", 42), "hitCount": 1}]})
        samples = parse_cpuprofile(prof)
        assert "anonymous" in samples[0].frames[-1]

    def test_bad_json(self):
        assert parse_cpuprofile("nope") == []

    def test_hotspots_df(self):
        df = node_hotspots_df(NodeResult(run_id="r", tool="cpu", binary="app.js",
                                         stack_samples=parse_cpuprofile(self.PROFILE)))
        assert "function" in df.columns
        assert df.filter(df["function"] == "work")["exclusive"][0] == 3


class TestHeapProfile:
    PROFILE = json.dumps({
        "head": {
            "callFrame": _cf("(root)"),
            "selfSize": 0,
            "children": [
                {"callFrame": _cf("alloc_a"), "selfSize": 4096, "children": []},
                {"callFrame": _cf("mid"), "selfSize": 0, "children": [
                    {"callFrame": _cf("alloc_b"), "selfSize": 2048, "children": []},
                ]},
            ],
        }
    })

    def test_weights_are_bytes(self):
        samples = parse_heapprofile(self.PROFILE)
        assert sum(s.weight for s in samples) == 4096 + 2048

    def test_paths_root_first(self):
        samples = parse_heapprofile(self.PROFILE)
        b = next(s for s in samples if s.frames[-1] == "alloc_b")
        assert b.frames == ["(root)", "mid", "alloc_b"]
        assert b.weight == 2048

    def test_empty(self):
        assert parse_heapprofile("{}") == []
