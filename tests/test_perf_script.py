"""Tests for `perf script` -> folded stack collapsing."""

from __future__ import annotations

from devtools_mcp.perf.parsers import parse_perf_script


class TestPerfScript:
    # perf script prints frames LEAF-FIRST (innermost first, root last).
    SAMPLE = """\
app  1234 1000.1: cycles:
\t        559f00 work+0x8 (/usr/bin/app)
\t        559def run+0x20 (/usr/bin/app)
\t        559abc main+0x10 (/usr/bin/app)

app  1234 1000.2: cycles:
\t        559f00 work+0x8 (/usr/bin/app)
\t        559def run+0x20 (/usr/bin/app)
\t        559abc main+0x10 (/usr/bin/app)

app  1234 1000.3: cycles:
\t        55aaaa idle+0x4 (/usr/bin/app)
\t        559abc main+0x10 (/usr/bin/app)
"""

    def test_aggregates_identical_stacks(self):
        samples = parse_perf_script(self.SAMPLE)
        # two identical main;run;work + one main;idle
        assert len(samples) == 2
        weights = sorted(s.weight for s in samples)
        assert weights == [1, 2]

    def test_root_first_order(self):
        samples = parse_perf_script(self.SAMPLE)
        deep = next(s for s in samples if s.weight == 2)
        assert deep.frames[0] == "main"   # root first
        assert deep.frames[-1] == "work"  # leaf last

    def test_strips_offsets(self):
        samples = parse_perf_script(self.SAMPLE)
        assert all("+0x" not in f for s in samples for f in s.frames)

    def test_skips_unknown_frames(self):
        text = "app 1 1.0: cycles:\n\t aaa [unknown] ([unknown])\n\t bbb real+0x1 (/x)\n"
        samples = parse_perf_script(text)
        assert samples and samples[0].frames == ["real"]

    def test_empty(self):
        assert parse_perf_script("") == []
