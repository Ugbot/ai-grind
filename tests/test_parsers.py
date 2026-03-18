"""Tests for all Valgrind output parsers with randomized sample data."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from devtools_mcp.models import RunBase
from devtools_mcp.valgrind.parsers import (
    parse_cachegrind,
    parse_callgrind,
    parse_massif,
    parse_memcheck_xml,
    parse_threadcheck_xml,
)


def _run_base(tool: str) -> RunBase:
    return RunBase(
        run_id=str(uuid.uuid4()),
        suite="valgrind",
        tool=tool,
        binary="./test_binary",
        args=[],
        valgrind_args=[],
        timestamp=datetime.now(UTC),
        exit_code=0,
        duration_seconds=1.0,
    )


class TestMemcheckParser:
    def test_parses_errors(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        assert result.tool == "memcheck"
        assert len(result.errors) > 0

    def test_error_has_stack(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        for error in result.errors:
            assert len(error.stack) > 0
            assert error.stack[0].fn is not None

    def test_error_has_kind(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        valid_kinds = {
            "InvalidRead",
            "InvalidWrite",
            "UninitValue",
            "UninitCondition",
            "Leak_DefinitelyLost",
            "Leak_PossiblyLost",
            "Leak_IndirectlyLost",
            "Leak_StillReachable",
        }
        for error in result.errors:
            assert error.kind in valid_kinds, f"Unexpected kind: {error.kind}"

    def test_leak_errors_have_bytes(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        leak_errors = [e for e in result.errors if e.kind.startswith("Leak_")]
        for error in leak_errors:
            assert error.bytes_leaked is not None
            assert error.bytes_leaked > 0
            assert error.blocks_leaked is not None
            assert error.blocks_leaked > 0

    def test_error_summary_populated(self, memcheck_xml_file: str):
        result = parse_memcheck_xml(memcheck_xml_file, _run_base("memcheck"))
        assert len(result.error_summary) > 0
        for _kind, count in result.error_summary.items():
            assert count > 0

    def test_handles_missing_file(self):
        result = parse_memcheck_xml("/nonexistent/file.xml", _run_base("memcheck"))
        assert len(result.errors) == 0
        assert len(result.error_summary) == 0

    def test_run_id_preserved(self, memcheck_xml_file: str):
        base = _run_base("memcheck")
        result = parse_memcheck_xml(memcheck_xml_file, base)
        assert result.run_id == base.run_id


class TestHelgrindParser:
    def test_parses_thread_errors(self, helgrind_xml_file: str):
        result = parse_threadcheck_xml(helgrind_xml_file, _run_base("helgrind"))
        assert len(result.errors) > 0

    def test_errors_have_kind(self, helgrind_xml_file: str):
        result = parse_threadcheck_xml(helgrind_xml_file, _run_base("helgrind"))
        valid_kinds = {"Race", "LockOrder", "UnlockUnlocked", "UnlockForeign"}
        for error in result.errors:
            assert error.kind in valid_kinds

    def test_errors_have_thread_id(self, helgrind_xml_file: str):
        result = parse_threadcheck_xml(helgrind_xml_file, _run_base("helgrind"))
        for error in result.errors:
            assert error.thread_id is not None
            assert error.thread_id > 0


class TestCallgrindParser:
    def test_parses_functions(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        assert result.tool == "callgrind"
        assert len(result.functions) > 0

    def test_has_events(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        assert len(result.events) > 0
        assert "Ir" in result.events

    def test_functions_have_self_cost(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        for fn in result.functions:
            assert len(fn.self_cost) > 0
            # At least one cost should be positive
            assert any(v > 0 for v in fn.self_cost.values())

    def test_totals_populated(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        assert len(result.totals) > 0
        assert result.totals.get("Ir", 0) > 0

    def test_callees_parsed(self, callgrind_file: str):
        result = parse_callgrind(callgrind_file, _run_base("callgrind"))
        # At least some functions should have callees
        all_callees = sum(len(fn.callees) for fn in result.functions)
        assert all_callees > 0

    def test_handles_missing_file(self):
        result = parse_callgrind("/nonexistent/file.out", _run_base("callgrind"))
        assert len(result.functions) == 0


class TestCachegrindParser:
    def test_parses_lines(self, cachegrind_file: str):
        result = parse_cachegrind(cachegrind_file, _run_base("cachegrind"))
        assert result.tool == "cachegrind"
        assert len(result.lines) > 0

    def test_has_events(self, cachegrind_file: str):
        result = parse_cachegrind(cachegrind_file, _run_base("cachegrind"))
        assert "Ir" in result.events

    def test_summary_populated(self, cachegrind_file: str):
        result = parse_cachegrind(cachegrind_file, _run_base("cachegrind"))
        assert result.summary.get("Ir", 0) > 0

    def test_lines_have_instruction_refs(self, cachegrind_file: str):
        result = parse_cachegrind(cachegrind_file, _run_base("cachegrind"))
        for line in result.lines:
            assert line.ir >= 0

    def test_miss_counts_less_than_refs(self, cachegrind_file: str):
        result = parse_cachegrind(cachegrind_file, _run_base("cachegrind"))
        for line in result.lines:
            assert line.i1mr <= line.ir
            if line.dr > 0:
                assert line.d1mr <= line.dr


class TestMassifParser:
    def test_parses_snapshots(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        assert result.tool == "massif"
        assert len(result.snapshots) > 0

    def test_has_peak(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        assert result.peak_snapshot_index >= 0

    def test_snapshots_ordered(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        indices = [s.index for s in result.snapshots]
        assert indices == sorted(indices)

    def test_peak_has_tree(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        peak = None
        for snap in result.snapshots:
            if snap.is_peak:
                peak = snap
                break
        assert peak is not None
        assert peak.heap_tree is not None

    def test_time_unit_parsed(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        assert result.time_unit == "i"

    def test_command_parsed(self, massif_file: str):
        result = parse_massif(massif_file, _run_base("massif"))
        assert result.command == "./test_binary"
