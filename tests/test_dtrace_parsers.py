"""Tests for DTrace output parsers."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from devtools_mcp.dtrace.parsers import parse_dtrace_output
from devtools_mcp.models import RunBase


def _run_base() -> RunBase:
    return RunBase(
        run_id=str(uuid.uuid4()),
        suite="dtrace",
        tool="trace",
        binary="./test",
        timestamp=datetime.now(UTC),
    )


class TestAggregationParser:
    SAMPLE = """\
dtrace: script './test.d' matched 42 probes
  read                                                             1523
  write                                                             892
  open                                                              234
  close                                                             201
  stat64                                                            156
"""

    def test_parses_aggregations(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        assert len(result.aggregations) == 5

    def test_aggregation_values_sorted(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        values = [a.value for a in result.aggregations]
        assert 1523 in values
        assert 892 in values

    def test_aggregation_keys(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        keys = [a.keys[0] for a in result.aggregations]
        assert "read" in keys
        assert "write" in keys


class TestMultiKeyAggregation:
    SAMPLE = """\
  bash                      read                              42
  python3                   write                             31
  node                      open                              15
"""

    def test_parses_multi_key(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        assert len(result.aggregations) == 3

    def test_multi_key_values(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        bash_agg = next(a for a in result.aggregations if "bash" in a.keys)
        assert bash_agg.value == 42
        assert len(bash_agg.keys) == 2


class TestStackParser:
    SAMPLE = """\
              libc.so.1`read+0x24
              myapp`process_data+0x48
              myapp`main+0x120
              libc.so.1`_start+0x7c
               42

              libc.so.1`write+0x24
              myapp`flush_output+0x30
              myapp`main+0x180
              libc.so.1`_start+0x7c
               15
"""

    def test_parses_stacks(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        assert len(result.stacks) >= 2

    def test_stack_has_frames(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        for stack in result.stacks:
            assert len(stack.frames) > 0

    def test_stack_counts(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        counts = sorted([s.count for s in result.stacks], reverse=True)
        assert counts[0] == 42


class TestQuantizeParser:
    SAMPLE = """\
           value  ------------- Distribution ------------- count
              16 |                                         0
              32 |@@@                                      3
              64 |@@@@@@@@@@                               10
             128 |@@@@@@@@@@@@@@@@@@@                      19
             256 |@@@@@@@@                                 8
             512 |                                         0
"""

    def test_parses_quantize(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        assert len(result.quantizations) >= 1

    def test_quantize_buckets(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        quant = result.quantizations[0]
        assert len(quant.buckets) > 0

    def test_quantize_total(self):
        result = parse_dtrace_output(self.SAMPLE, _run_base())
        quant = result.quantizations[0]
        assert quant.total == 40  # 0+3+10+19+8+0


class TestEmptyOutput:
    def test_empty_string(self):
        result = parse_dtrace_output("", _run_base())
        assert len(result.aggregations) == 0
        assert len(result.stacks) == 0

    def test_only_dtrace_info(self):
        result = parse_dtrace_output("dtrace: script matched 0 probes\n", _run_base())
        assert len(result.aggregations) == 0
