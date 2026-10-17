"""MCP boundary keeps analysis and instrumented performance separate."""

import threading

import pytest

from devtools_mcp.evidence import EvidenceParseError
from devtools_mcp.tools import ring_evidence_tools as tool


@pytest.mark.asyncio
async def test_analysis_runs_off_event_loop_and_forwards_explicit_budgets(monkeypatch):
    owner_thread = threading.get_ident()
    native = {"arrival": {"validation": "FAIL"}}

    def analyze(*args, **kwargs):
        assert threading.get_ident() != owner_thread
        assert args == ("/trace.csv", "/arrival.csv", native)
        assert kwargs == {"max_events": 42, "max_bytes": 4096, "top_n": 3, "preceding_events": 0}
        return {"analysis_validation": "PASS", "performance_validation": "FAIL"}

    monkeypatch.setattr(tool, "analyze_ring_arrival_boundaries", analyze)
    result = await tool.devtools_ring_arrival(
        "/trace.csv", "/arrival.csv", native, max_events=42, max_bytes=4096, top_n=3
    )
    assert result == {"analysis_validation": "PASS", "performance_validation": "FAIL"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [EvidenceParseError("mixed output"), OSError("unreadable"), ValueError("bad cap")])
async def test_invalid_evidence_never_returns_a_performance_pass(monkeypatch, failure):
    def analyze(*args, **kwargs):
        raise failure

    monkeypatch.setattr(tool, "analyze_ring_arrival_boundaries", analyze)
    result = await tool.devtools_ring_arrival("/trace.csv", "/arrival.csv", {})
    assert result["analysis_validation"] == "FAIL"
    assert result["performance_validation"] == "UNVERIFIED"
    assert result["error"] == {"code": "invalid_evidence", "message": str(failure)}


@pytest.mark.asyncio
async def test_programmer_failure_is_not_hidden_as_input_refusal(monkeypatch):
    def analyze(*args, **kwargs):
        raise RuntimeError("unexpected internal failure")

    monkeypatch.setattr(tool, "analyze_ring_arrival_boundaries", analyze)
    with pytest.raises(RuntimeError, match="unexpected internal failure"):
        await tool.devtools_ring_arrival("/trace.csv", "/arrival.csv", {})


@pytest.mark.asyncio
async def test_actual_eight_column_cpu_evidence_is_validated_through_wrapper(tmp_path):
    from tests.test_evidence_ring_arrival import cpu_fixture

    boundary, arrival, benchmark = cpu_fixture(tmp_path)
    result = await tool.devtools_ring_arrival(
        str(boundary), str(arrival), benchmark, max_events=3, max_bytes=8192, top_n=1
    )
    assert result["analysis_validation"] == "PASS"
    assert result["performance_validation"] == "FAIL"
    assert result["thread_cpu"]["whole_thread_cpu_advance_ns"] == {"producer": 70, "consumer": 152}
    assert result["thread_cpu"]["negative_residual_spans"] == 4
    assert len(result["worst_events"]) == 1
    assert result["worst_events"][0]["thread_cpu"]["observer"]["wall_minus_thread_cpu_ns"] == -9

    boundary.write_text(boundary.read_text().replace("B,7,20,4,observe_in,330,0,650", "B,7,20,4,observe_in,330,0,634"))
    refused = await tool.devtools_ring_arrival(str(boundary), str(arrival), benchmark, max_events=3)
    assert refused["analysis_validation"] == "FAIL"
    assert refused["performance_validation"] == "UNVERIFIED"
    assert "EOF thread CPU" in refused["error"]["message"]


@pytest.mark.asyncio
async def test_wrapper_returns_cpu_predecessor_window_with_separate_row_budget(tmp_path):
    from tests.test_evidence_ring_arrival import cpu_fixture

    boundary, arrival, benchmark = cpu_fixture(tmp_path)
    result = await tool.devtools_ring_arrival(
        str(boundary), str(arrival), benchmark, max_events=3, top_n=1, preceding_events=2
    )
    assert result["analysis_validation"] == "PASS"
    assert len(result["worst_events"]) == 1
    window = result["worst_event_predecessors"]
    assert window["requested"] == 2 and window["available"] == 1
    assert window["events"][0]["event"] == 2
    assert window["events"][0]["thread_cpu"]["observer"]["wall_minus_thread_cpu_ns"] == -9


@pytest.mark.parametrize("bad", [True, False, -1, 0.0, "1"])
def test_mcp_predecessor_schema_requires_nonnegative_strict_integer(bad):
    from pydantic import TypeAdapter, ValidationError

    with pytest.raises(ValidationError):
        TypeAdapter(tool.NonNegativeInt).validate_python(bad)


@pytest.mark.asyncio
async def test_wrapper_refuses_oversized_predecessor_budget_before_reading():
    result = await tool.devtools_ring_arrival(
        "missing-boundaries", "missing-arrival", {}, max_events=3, preceding_events=4
    )
    assert result["analysis_validation"] == "FAIL"
    assert result["performance_validation"] == "UNVERIFIED"
    assert "preceding_events" in result["error"]["message"]
