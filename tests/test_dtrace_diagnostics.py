"""Collection diagnostics are independent of target output and subprocess exit."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

import devtools_mcp.dtrace.runner as runner
from devtools_mcp.dtrace.formatters import format_dtrace_summary
from devtools_mcp.dtrace.models import DTraceCollectionDiagnostics, DTraceResult, DTraceStackTrace

SIP = b"dtrace: system integrity protection is on, some features will not be available\n"
FAULT = b"dtrace: error on enabled probe ID 1: invalid kernel access in action #3 at DIF offset 32\n"
DROP = b"dtrace: 314186 dynamic variable drops with non-empty dirty list\n"
BENCHMARK = b'{"benchmark":"native_ring_job_throughput","validation":"PASS","error":0}\n'


class Process:
    def __init__(self, stdout: bytes, stderr: bytes, exit_code: int, timeout: bool = False):
        self.stdout, self.stderr, self.returncode, self.timeout = stdout, stderr, exit_code, timeout

    async def communicate(self):
        if self.timeout:
            raise TimeoutError
        return self.stdout, self.stderr

    async def wait(self):
        return self.returncode


async def run(monkeypatch, tmp_path, stdout=BENCHMARK, stderr=b"", exit_code=0, timeout=False):
    process = Process(stdout, stderr, exit_code, timeout)

    async def create(*_args, **_kwargs):
        return process

    def kill(target):
        assert target is process
        process.returncode = -9

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(runner, "_kill_group", kill)
    monkeypatch.setattr(runner.tempfile, "tempdir", str(tmp_path))
    error, result, raw = await runner.run_dtrace("trace", one_liner="BEGIN { exit(0); }", sudo=False)
    assert error is None and result is not None
    return result, Path(raw)


@pytest.mark.asyncio
async def test_zero_exit_and_benchmark_pass_do_not_hide_collection_faults(monkeypatch, tmp_path):
    result, raw = await run(monkeypatch, tmp_path, stderr=SIP + FAULT + DROP)
    assert result.exit_code == 0 and result.raw_output == (BENCHMARK + b"\n" + SIP + FAULT + DROP).decode()
    assert raw.read_text() == result.raw_output
    diagnostics = result.collection_diagnostics
    assert diagnostics is not None
    assert diagnostics.capture_complete and diagnostics.scan_complete
    assert diagnostics.captured_stderr_bytes == diagnostics.scanned_stderr_bytes == len(SIP + FAULT + DROP)
    assert (diagnostics.error_lines, diagnostics.drop_lines, diagnostics.advisory_lines) == (1, 1, 1)
    summary = format_dtrace_summary(result)
    assert "**Exit code:** 0" in summary and '"validation":"PASS"' in summary
    assert summary.index("ISSUES REPORTED") < summary.index('"benchmark"')
    assert "invalid kernel access" in summary and "314186 dynamic variable drops" in summary
    # The same warning must survive the structured-data formatter branch too.
    result.stacks = [DTraceStackTrace(frames=["actual_target"], count=7)]
    summary = format_dtrace_summary(result)
    assert summary.index("ISSUES REPORTED") < summary.index("Stack traces")


@pytest.mark.asyncio
async def test_large_stdout_prefix_cannot_hide_stderr_fault(monkeypatch, tmp_path):
    stdout = b"X" * 210_000 + b"\n" + BENCHMARK
    result, raw = await run(monkeypatch, tmp_path, stdout=stdout, stderr=FAULT)
    assert raw.stat().st_size > 200_000
    assert result.collection_diagnostics is not None and result.collection_diagnostics.error_lines == 1
    assert "invalid kernel access" in format_dtrace_summary(result)


@pytest.mark.asyncio
async def test_sip_notice_is_advisory_and_stdout_is_not_stderr(monkeypatch, tmp_path):
    result, _ = await run(monkeypatch, tmp_path, stdout=BENCHMARK + FAULT + DROP, stderr=SIP)
    diagnostics = result.collection_diagnostics
    assert diagnostics is not None
    assert diagnostics.error_lines == diagnostics.drop_lines == 0 and diagnostics.advisory_lines == 1
    summary = format_dtrace_summary(result)
    assert "ISSUES REPORTED" not in summary and "system integrity protection" in summary
    assert "No error/drop lines reported" in summary and "completeness is not certified" in summary


def test_observed_counts_and_examples_remain_bounded():
    diagnostics = runner._collection_diagnostics(FAULT * 1000 + DROP * 3)
    assert diagnostics.error_lines == 1000 and diagnostics.drop_lines == 3
    assert len(diagnostics.examples) == 2
    many = b"".join(f"dtrace: notice {i}\n".encode() for i in range(10)) + FAULT + DROP
    diagnostics = runner._collection_diagnostics(many)
    assert diagnostics.diagnostic_lines == 12 and len(diagnostics.examples) == 8
    assert diagnostics.examples_truncated and diagnostics.examples[0].startswith("dtrace: error")
    assert any("drops" in line for line in diagnostics.examples)
    long_line = runner._collection_diagnostics(b"dtrace: error " + b"x" * 2000)
    assert len(long_line.examples[0]) == 1024 and long_line.examples_truncated


def test_scan_cap_never_certifies_unexamined_suffix():
    diagnostics = runner._collection_diagnostics(SIP + b"x" * 128 + FAULT, max_scan_bytes=len(SIP) + 32)
    assert diagnostics.capture_complete and not diagnostics.scan_complete
    assert diagnostics.scanned_stderr_bytes == len(SIP)
    assert diagnostics.captured_stderr_bytes == len(SIP) + 128 + len(FAULT)
    assert diagnostics.error_lines == 0
    result = DTraceResult(run_id="capped", tool="trace", binary="target", collection_diagnostics=diagnostics)
    summary = format_dtrace_summary(result)
    assert "UNVERIFIED" in summary and "scan is incomplete" in summary
    assert "No error/drop lines reported" not in summary


@pytest.mark.asyncio
async def test_original_stderr_byte_extent_is_not_decoded_reencoding(monkeypatch, tmp_path):
    stderr = FAULT + b"dtrace: notice \xff\xfe\n"
    result, raw = await run(monkeypatch, tmp_path, stderr=stderr, exit_code=7)
    assert result.exit_code == 7 and raw.read_text() == result.raw_output
    assert result.collection_diagnostics is not None
    assert result.collection_diagnostics.captured_stderr_bytes == len(stderr)
    assert len(stderr.decode(errors="replace").encode()) > len(stderr)
    assert "\ufffd" in result.collection_diagnostics.examples[-1]


@pytest.mark.asyncio
async def test_timeout_exposes_missing_source_capture(monkeypatch, tmp_path):
    result, raw = await run(monkeypatch, tmp_path, stderr=FAULT, timeout=True)
    assert result.exit_code == -9 and raw.read_text() == "\nDTrace timed out"
    diagnostics = result.collection_diagnostics
    assert diagnostics is not None and not diagnostics.capture_complete
    assert diagnostics.captured_stderr_bytes == 0  # Partial communicate output was not recovered.
    summary = format_dtrace_summary(result)
    assert "UNVERIFIED" in summary and "No error/drop lines reported" not in summary


def test_old_serialized_results_do_not_imply_stderr_was_inspected():
    old = {"run_id": "legacy", "tool": "trace", "binary": "target", "raw_output": "legacy raw"}
    result = DTraceResult.model_validate(old)
    assert result.collection_diagnostics is None
    assert "stderr diagnostics were not recorded" in format_dtrace_summary(result)
    restored = DTraceResult.model_validate_json(result.model_dump_json())
    assert restored.collection_diagnostics is None


def test_zero_drop_report_does_not_claim_loss():
    diagnostics = runner._collection_diagnostics(b"dtrace: 0 dynamic variable drops\n")
    assert diagnostics.diagnostic_lines == 1 and diagnostics.drop_lines == 0


@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_restored_diagnostics_enforces_per_example_limit(as_json, nested):
    model = DTraceResult if nested else DTraceCollectionDiagnostics

    def restore(length):
        payload = {"examples": ["x" * length]}
        if nested:
            payload = {
                "run_id": "restored", "tool": "trace", "binary": "target",
                "collection_diagnostics": payload,
            }
        return model.model_validate_json(json.dumps(payload)) if as_json else model.model_validate(payload)

    valid = restore(1024)
    diagnostics = valid.collection_diagnostics if nested else valid
    assert diagnostics is not None and diagnostics.examples == ["x" * 1024]
    with pytest.raises(ValidationError, match="1024"):
        restore(1025)
