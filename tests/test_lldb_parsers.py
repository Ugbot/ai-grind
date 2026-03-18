"""Tests for LLDB output parsers with sample output."""

from __future__ import annotations

from devtools_mcp.lldb.parsers import (
    parse_backtrace,
    parse_breakpoint_list,
    parse_registers,
    parse_thread_list,
    parse_variables,
)


class TestBacktraceParser:
    SAMPLE_BT = """\
* thread #1, queue = 'com.apple.main-thread', stop reason = breakpoint 1.1
    frame #0: 0x0000000100003f60 a.out`main at main.c:10:5
    frame #1: 0x0000000100003f20 a.out`_start at crt0.c:85
    frame #2: 0x00007ff8004b1345 dyld`start + 1765
  thread #2, name = 'worker', stop reason = signal SIGSTOP
    frame #0: 0x00007ff800123456 libsystem_kernel.dylib`__psynch_cvwait + 8
    frame #1: 0x0000000100004000 a.out`worker_fn at worker.c:42:10
"""

    def test_parses_multiple_threads(self):
        threads = parse_backtrace(self.SAMPLE_BT)
        assert len(threads) == 2

    def test_thread_has_stop_reason(self):
        threads = parse_backtrace(self.SAMPLE_BT)
        assert threads[0].stop_reason == "breakpoint 1.1"
        assert threads[1].stop_reason == "signal SIGSTOP"

    def test_thread_has_name(self):
        threads = parse_backtrace(self.SAMPLE_BT)
        assert threads[1].name == "worker"

    def test_frames_parsed(self):
        threads = parse_backtrace(self.SAMPLE_BT)
        assert len(threads[0].frames) == 3
        assert len(threads[1].frames) == 2

    def test_frame_has_function(self):
        threads = parse_backtrace(self.SAMPLE_BT)
        assert threads[0].frames[0].function == "main"
        assert threads[0].frames[0].file == "main.c"
        assert threads[0].frames[0].line == 10

    def test_frame_has_module(self):
        threads = parse_backtrace(self.SAMPLE_BT)
        assert threads[0].frames[0].module == "a.out"
        assert threads[0].frames[2].module == "dyld"

    def test_frame_has_address(self):
        threads = parse_backtrace(self.SAMPLE_BT)
        assert threads[0].frames[0].address == "0x0000000100003f60"

    def test_empty_input(self):
        threads = parse_backtrace("")
        assert threads == []


class TestThreadListParser:
    SAMPLE = """\
Process 12345 stopped
* thread #1: tid = 0x1a2b3c, 0x0000000100003f60 a.out`main, stop reason = breakpoint 1.1
  thread #2: tid = 0x1a2b3d, name = 'worker', 0x00007ff800123456 libsystem_kernel.dylib`__psynch_cvwait
"""

    def test_parses_threads(self):
        threads = parse_thread_list(self.SAMPLE)
        assert len(threads) >= 2

    def test_thread_ids(self):
        threads = parse_thread_list(self.SAMPLE)
        ids = [t.thread_id for t in threads]
        assert 1 in ids
        assert 2 in ids


class TestVariablesParser:
    SAMPLE = """\
(int) argc = 1
(const char **) argv = 0x00007ffeefbff5f0
(char [256]) buffer = "hello world"
(struct Point) origin = (x = 0, y = 0)
"""

    def test_parses_variables(self):
        variables = parse_variables(self.SAMPLE)
        assert len(variables) == 4

    def test_variable_types(self):
        variables = parse_variables(self.SAMPLE)
        assert variables[0].type == "int"
        assert variables[0].name == "argc"
        assert variables[0].value == "1"

    def test_complex_types(self):
        variables = parse_variables(self.SAMPLE)
        assert variables[1].type == "const char **"
        assert variables[2].type == "char [256]"
        assert variables[2].value == '"hello world"'

    def test_empty_input(self):
        variables = parse_variables("")
        assert variables == []


class TestBreakpointParser:
    SAMPLE = """\
Current breakpoints:
1: name = 'main', locations = 1, resolved = 1, hit count = 3
  1.1: where = a.out`main + 20 at main.c:10:5, address = 0x0000000100003f60, resolved, hit count = 3
2: name = 'worker_fn', locations = 1, resolved = 1, hit count = 0
  2.1: where = a.out`worker_fn at worker.c:42, address = 0x0000000100004000, resolved, hit count = 0
"""

    def test_parses_breakpoints(self):
        breakpoints = parse_breakpoint_list(self.SAMPLE)
        assert len(breakpoints) >= 2

    def test_breakpoint_hit_count(self):
        breakpoints = parse_breakpoint_list(self.SAMPLE)
        bp1 = next(bp for bp in breakpoints if bp.id == 1)
        assert bp1.hit_count == 3
        assert bp1.name == "main"

    def test_empty_input(self):
        breakpoints = parse_breakpoint_list("")
        assert breakpoints == []


class TestRegisterParser:
    SAMPLE = """\
General Purpose Registers:
        rax = 0x0000000000000001
        rbx = 0x0000000000000000
        rcx = 0x00007ffeefbff5f0
        rdx = 0x00007ffeefbff600
        rsp = 0x00007ffeefbff4e0
        rbp = 0x00007ffeefbff510
        rip = 0x0000000100003f60  a.out`main + 20 at main.c:10
"""

    def test_parses_registers(self):
        regs = parse_registers(self.SAMPLE)
        assert len(regs) >= 7

    def test_register_values(self):
        regs = parse_registers(self.SAMPLE)
        assert "rax" in regs
        assert regs["rax"].startswith("0x")

    def test_empty_input(self):
        regs = parse_registers("")
        assert regs == {}
