"""Tests for CDB batch-output parsers."""

from __future__ import annotations

from devtools_mcp.cdb.analysis import cdb_frames_df, cdb_stack_samples
from devtools_mcp.cdb.parsers import parse_analyze, parse_registers, parse_stacks
from devtools_mcp.cdb.runner import parse_cdb_output


class TestParseStacks:
    SINGLE = """\
 # Child-SP          RetAddr               Call Site
00 000000d4`fd3ff568 00007ffd`12340001 app!crash+0x14 [c:\\src\\main.cpp @ 42]
01 000000d4`fd3ff5a0 00007ffd`12340002 app!process+0x80 [c:\\src\\main.cpp @ 88]
02 000000d4`fd3ff5f0 00007ffd`6e110001 app!main+0x10 [c:\\src\\main.cpp @ 10]
03 000000d4`fd3ff620 00007ffd`6e110002 KERNEL32!BaseThreadInitThunk+0x14
"""

    MULTI = """\
   0  Id: 1abc.2def Suspend: 1 Teb: 00000000 Unfrozen
 # ChildEBP RetAddr  Call Site
00 0019f8c4 76e11234 ntdll!NtWaitForSingleObject+0x14
01 0019f8c8 76e05678 KERNELBASE!WaitForSingleObjectEx+0x99

   1  Id: 1abc.3aaa Suspend: 1 Teb: 00000000 Unfrozen
 # ChildEBP RetAddr  Call Site
00 0029fab0 76e19999 app!worker+0x40 [worker.cpp @ 7]
01 0029fae0 76e1aaaa app!main+0x10
"""

    def test_single_thread(self):
        threads = parse_stacks(self.SINGLE)
        assert len(threads) == 1
        assert len(threads[0].frames) == 4

    def test_frame_symbol_and_source(self):
        threads = parse_stacks(self.SINGLE)
        f0 = threads[0].frames[0]
        assert f0.module == "app"
        assert f0.function == "crash"
        assert f0.offset == "+0x14"
        assert f0.file.endswith("main.cpp")
        assert f0.line == 42
        assert f0.symbol == "app!crash"

    def test_multi_thread(self):
        threads = parse_stacks(self.MULTI)
        assert len(threads) == 2
        assert threads[0].tid == "1abc.2def"
        assert threads[1].frames[0].function == "worker"

    def test_skips_headers(self):
        threads = parse_stacks(self.SINGLE)
        # header line "# Child-SP ... Call Site" must not become a frame
        assert all(f.function for f in threads[0].frames)

    def test_empty(self):
        assert parse_stacks("") == []


class TestParseAnalyze:
    SAMPLE = """\
EXCEPTION_CODE: (NTSTATUS) 0xc0000005 - The instruction at 0x... referenced memory
EXCEPTION_CODE_STR: c0000005_ACCESS_VIOLATION
FAULTING_IP:
app!crash+14
SYMBOL_NAME:  app!crash+14
MODULE_NAME: app
IMAGE_NAME:  app.exe
FAILURE_BUCKET_ID: INVALID_POINTER_READ_c0000005_app.exe!crash
STACK_TEXT:
00 000000 app!crash+0x14
"""

    def test_key_fields(self):
        fields, exc = parse_analyze(self.SAMPLE)
        assert fields["SYMBOL_NAME"] == "app!crash+14"
        assert fields["MODULE_NAME"] == "app"
        assert fields["IMAGE_NAME"] == "app.exe"
        assert "INVALID_POINTER_READ" in fields["FAILURE_BUCKET_ID"]

    def test_exception(self):
        _fields, exc = parse_analyze(self.SAMPLE)
        assert exc == "c0000005_ACCESS_VIOLATION"


class TestParseRegisters:
    SAMPLE = (
        "rax=0000000000000000 rbx=00007ffd12340000 rcx=0000000000000001\n"
        "rip=00007ffd12340014 rsp=000000d4fd3ff568 rbp=0000000000000000\n"
    )

    def test_parses_registers(self):
        regs = parse_registers(self.SAMPLE)
        assert regs["rip"] == "00007ffd12340014"
        assert regs["rax"] == "0000000000000000"
        assert len(regs) == 6


class TestSnapshotAndAnalysis:
    def test_parse_cdb_output_analyze(self):
        text = TestParseStacks.SINGLE + "\n" + TestParseAnalyze.SAMPLE
        snap = parse_cdb_output("analyze", text, "app.dmp", 0.5)
        assert snap.suite == "cdb"
        assert snap.threads
        assert snap.analysis["MODULE_NAME"] == "app"
        assert snap.exception == "c0000005_ACCESS_VIOLATION"

    def test_frames_df_aliases_function(self):
        snap = parse_cdb_output("stacks", TestParseStacks.MULTI, "app.exe", 0.1)
        df = cdb_frames_df(snap)
        assert "function" in df.columns
        assert df.height == 4  # 2 + 2 frames

    def test_stack_samples_reversed_root_first(self):
        snap = parse_cdb_output("stacks", TestParseStacks.SINGLE, "app.exe", 0.1)
        samples = cdb_stack_samples(snap)
        assert len(samples) == 1
        # frame 00 (crash) is innermost → must be LAST after reversal
        assert samples[0].frames[-1] == "app!crash"
        assert samples[0].frames[0] == "KERNEL32!BaseThreadInitThunk"
