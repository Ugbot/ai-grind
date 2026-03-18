"""Tests for perf output parsers."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from devtools_mcp.models import RunBase
from devtools_mcp.perf.parsers import parse_perf_annotate, parse_perf_report, parse_perf_stat


def _run_base(tool: str = "stat") -> RunBase:
    return RunBase(
        run_id=str(uuid.uuid4()),
        suite="perf",
        tool=tool,
        binary="./test",
        timestamp=datetime.now(UTC),
    )


class TestPerfStatParser:
    SAMPLE_CSV = """\
1234567,,cycles,0.12%,100.00%
987654,,instructions,0.08%,100.00%
45678,,cache-misses,0.45%,100.00%
123456,,cache-references,0.10%,100.00%
56789,,branch-misses,0.20%,100.00%
1000000000,,duration_time,,100.00%
"""

    def test_parses_counters(self):
        result = parse_perf_stat(self.SAMPLE_CSV, _run_base())
        assert len(result.counters) == 6

    def test_counter_values(self):
        result = parse_perf_stat(self.SAMPLE_CSV, _run_base())
        cycles = next(c for c in result.counters if c.event == "cycles")
        assert cycles.value == 1234567

    def test_counter_variance(self):
        result = parse_perf_stat(self.SAMPLE_CSV, _run_base())
        cycles = next(c for c in result.counters if c.event == "cycles")
        assert cycles.variance_pct == 0.12

    def test_ipc_calculated(self):
        result = parse_perf_stat(self.SAMPLE_CSV, _run_base())
        assert result.ipc is not None
        assert result.ipc == 987654 / 1234567

    def test_empty_input(self):
        result = parse_perf_stat("", _run_base())
        assert len(result.counters) == 0
        assert result.ipc is None


class TestPerfReportParser:
    SAMPLE = """\
# Overhead  Command       Shared Object              Symbol
# ........  ............  .........................  .......................
#
    42.12%  myapp         myapp                      [.] hot_function
    15.67%  myapp         libc.so.6                  [.] malloc
     8.34%  myapp         myapp                      [.] process_data
     5.21%  myapp         [kernel.kallsyms]          [k] page_fault
     3.45%  myapp         libc.so.6                  [.] free
"""

    def test_parses_samples(self):
        result = parse_perf_report(self.SAMPLE, _run_base("record"))
        assert len(result.samples) == 5

    def test_sample_overhead(self):
        result = parse_perf_report(self.SAMPLE, _run_base("record"))
        assert result.samples[0].overhead_pct == 42.12
        assert result.samples[0].symbol == "hot_function"

    def test_sample_shared_object(self):
        result = parse_perf_report(self.SAMPLE, _run_base("record"))
        malloc = next(s for s in result.samples if s.symbol == "malloc")
        assert malloc.shared_object == "libc.so.6"

    def test_empty_input(self):
        result = parse_perf_report("", _run_base("record"))
        assert len(result.samples) == 0


class TestPerfAnnotateParser:
    SAMPLE = """\
 Percent |      Source code & Disassembly of myapp
------------------------------------------------
         :
         :      Disassembly of section .text:
         :
    5.23 :  400520:   mov    %rax,%rdi
         :  400523:   call   401000 <malloc@plt>
   12.45 :  400528:   test   %rax,%rax
         :  40052a:   je     400540 <error>
    8.90 :  40052c:   mov    %rax,(%rbx)
"""

    def test_parses_lines(self):
        result = parse_perf_annotate(self.SAMPLE, _run_base("annotate"))
        assert len(result.lines) > 0

    def test_hot_lines_have_percent(self):
        result = parse_perf_annotate(self.SAMPLE, _run_base("annotate"))
        hot = [ln for ln in result.lines if ln.percent > 0]
        assert len(hot) == 3
        percents = sorted([ln.percent for ln in hot], reverse=True)
        assert percents[0] == 12.45

    def test_instructions_parsed(self):
        result = parse_perf_annotate(self.SAMPLE, _run_base("annotate"))
        mov_line = next(ln for ln in result.lines if "mov" in ln.instruction and ln.percent > 0)
        assert mov_line.address == "400520"

    def test_empty_input(self):
        result = parse_perf_annotate("", _run_base("annotate"))
        assert len(result.lines) == 0
