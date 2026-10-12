"""Tests for DTrace output parsers."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from devtools_mcp.dtrace.analysis import dtrace_stack_samples
from devtools_mcp.dtrace.parsers import parse_dtrace_output
from devtools_mcp.flamegraph import build_call_tree
from devtools_mcp.flamegraph.tree import function_stats
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

    @pytest.mark.parametrize("module", ["bench_txn", "651-open-baseline-bench_txn", "123"])
    def test_module_names_preserve_frames_and_cpu_attribution(self, module):
        frames = [
            f"{module}`marbledb::memtable::zone_probe_at(void*, unsigned long)+0x1c8",
            f"{module}`marbledb::txn_commit_opt_ex(void*)+0x7f8",
            "libsystem_pthread.dylib`_pthread_start+0x88",
            "libsystem_pthread.dylib`thread_start+0x8",
        ]
        other_frames = [f"{module}`flush+0x10", f"{module}`worker+0x20"]
        text = "\n".join(f"              {frame}" for frame in frames) + "\n               3263\n\n"
        text += "\n".join(f"              {frame}" for frame in other_frames) + "\n               17\n\n"
        text += "  651 read 42\n  0 12345 syscall::read:entry pid=1234\n"
        text += "  651 libfoo`work 42\n  652 libfoo`other 7\n"
        text += "  0 12345 syscall::read:entry symbol=libfoo`work\n"
        text += "  1 12346 syscall::write:entry symbol=libfoo`other\n"

        result = parse_dtrace_output(text, _run_base())

        assert [stack.frames for stack in result.stacks] == [frames, other_frames]
        assert [stack.count for stack in result.stacks] == [3263, 17]
        assert [(agg.keys, agg.value) for agg in result.aggregations] == [
            (["651", "read"], 42),
            (["651", "libfoo`work"], 42),
            (["652", "libfoo`other"], 7),
        ]
        assert [(hit.cpu, hit.probe, hit.args) for hit in result.probe_hits] == [
            (0, "syscall::read:entry", "pid=1234"),
            (0, "syscall::read:entry", "symbol=libfoo`work"),
            (1, "syscall::write:entry", "symbol=libfoo`other"),
        ]
        tree = build_call_tree(dtrace_stack_samples(result))
        stats = function_stats(tree)
        assert tree.total_weight == 3280
        assert stats[frames[0].rsplit("+", 1)[0]] == (3263, 3263)
        assert stats["libsystem_pthread.dylib`_pthread_start"] == (0, 3263)


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
