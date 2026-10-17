"""MCP off-loop execution and fail-closed policy evidence boundary."""

import threading

import pytest
from pydantic import TypeAdapter, ValidationError

from devtools_mcp.tools import instruments_tools as tool
from tests.test_instruments_threads import fixture


@pytest.mark.asyncio
async def test_actual_xml_coverage_through_wrapper(tmp_path):
    paths = fixture(tmp_path)
    result = await tool.devtools_instruments_threads(*map(str, paths), pid=7, window_start_ns=100, window_end_ns=200)
    assert result["analysis_validation"] == "PASS" and result["coverage"] == "PARTIAL"
    assert result["threads"][0]["qos"]["uncovered_ns"] == 40


@pytest.mark.asyncio
async def test_off_loop_and_explicit_controls(monkeypatch):
    caller = threading.get_ident()

    def analyze(*args, **kwargs):
        assert threading.get_ident() != caller
        assert args == ("i", "q", "p")
        assert kwargs == dict(
            pid=7,
            window_start_ns=10,
            window_end_ns=20,
            max_bytes=100,
            max_rows=30,
            max_nodes=50,
            max_threads=2,
            top_n=1,
        )
        return {"analysis_validation": "PASS"}

    monkeypatch.setattr(tool, "analyze_instruments_thread_policy", analyze)
    assert await tool.devtools_instruments_threads(
        "i",
        "q",
        "p",
        pid=7,
        window_start_ns=10,
        window_end_ns=20,
        max_bytes=100,
        max_rows=30,
        max_nodes=50,
        max_threads=2,
        top_n=1,
    ) == {"analysis_validation": "PASS"}


@pytest.mark.asyncio
async def test_invalid_evidence_has_no_policy_or_performance_pass():
    result = await tool.devtools_instruments_threads(
        "absent", "absent", "absent", pid=7, window_start_ns=10, window_end_ns=20
    )
    assert result["analysis_validation"] == "FAIL" and result["performance_validation"] == "UNVERIFIED"
    assert result["error"]["code"] == "invalid_evidence"


@pytest.mark.parametrize("annotation", [tool.PositiveInt, tool.UInt64])
@pytest.mark.parametrize("value", [True, False, -1, 0.5, "1"])
def test_mcp_controls_are_strict(annotation, value):
    with pytest.raises(ValidationError):
        TypeAdapter(annotation).validate_python(value)


@pytest.mark.asyncio
async def test_ring_wrapper_actual_default_and_explicit_conditional(tmp_path):
    from tests.test_instruments_ring import fixture as ring_fixture

    paths = ring_fixture(tmp_path)
    default = await tool.devtools_instruments_ring(*map(str, paths), pid=7)
    assert default["clock_join_validation"] == "UNVERIFIED" and default["worst_events"] == []
    conditional = await tool.devtools_instruments_ring(*map(str, paths), pid=7, assume_constant_offset=True, top_n=1)
    assert conditional["clock_join_validation"] == "CONDITIONAL" and conditional["worst_events"][0]["event"] == 3
    assert conditional["performance_validation"] == "FAIL"


@pytest.mark.asyncio
async def test_ring_wrapper_invalid_evidence_refuses_join():
    result = await tool.devtools_instruments_ring(*(["absent"] * 5), pid=7)
    assert result["analysis_validation"] == "FAIL" and result["clock_join_validation"] == "UNVERIFIED"
    assert result["error"]["code"] == "invalid_evidence"


@pytest.mark.asyncio
async def test_ring_wrapper_offloop_and_controls(monkeypatch):
    from devtools_mcp.evidence import instruments_ring

    caller = threading.get_ident()

    def analyze(*args, **kwargs):
        assert threading.get_ident() != caller and args == ("n", "c", "s", "t", "i")
        assert kwargs == dict(
            pid=7,
            assume_constant_offset=True,
            max_bytes=100,
            max_rows=30,
            max_nodes=50,
            max_threads=2,
            top_n=1,
            state_details=3,
        )
        return {"analysis_validation": "PASS"}

    monkeypatch.setattr(instruments_ring, "analyze_instruments_ring_arrival", analyze)
    assert await tool.devtools_instruments_ring(
        "n",
        "c",
        "s",
        "t",
        "i",
        pid=7,
        assume_constant_offset=True,
        max_bytes=100,
        max_rows=30,
        max_nodes=50,
        max_threads=2,
        top_n=1,
        state_details=3,
    ) == {"analysis_validation": "PASS"}


@pytest.mark.parametrize("value", [0, 1, "true", None])
def test_ring_wrapper_assumption_schema_is_strict(value):
    from typing import get_type_hints

    annotation = get_type_hints(tool.devtools_instruments_ring, include_extras=True)["assume_constant_offset"]
    with pytest.raises(ValidationError):
        TypeAdapter(annotation).validate_python(value)
