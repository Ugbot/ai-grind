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
        assert kwargs == {"max_events": 42, "max_bytes": 4096, "top_n": 3}
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
