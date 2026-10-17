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
