"""Station contract validation and optional live native workflow."""

import json
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from devtools_mcp.node import backend
from devtools_mcp.node.station_core import import_envelope
from devtools_mcp.workspace import Workspace


def envelope(root):
    raw = root / "raw.cpuprofile"
    raw.write_text("{}")
    return {
        "protocol_version": 1,
        "engine": "llm-station",
        "run_id": "station-test",
        "suite": "node",
        "tool": "cpu",
        "binary": "/work/test.js",
        "args": [],
        "workspace_id": "/work",
        "duration_seconds": 1.0,
        "exit_code": 0,
        "raw_path": str(raw),
        "stack_samples": [{"frames": ["root", "leaf"], "weight": 3}],
        "total_weight": 3,
        "timestamp_ms": 1000000,
    }


@pytest.mark.parametrize("field,value", [("protocol_version", 2), ("engine", "unknown"), ("run_id", "../escape")])
def test_invalid_envelope_rejected(tmp_path, field, value):
    data = envelope(tmp_path)
    data[field] = value
    with pytest.raises(ValidationError):
        import_envelope(data, tmp_path)


def test_stack_artifact_and_provenance_survive_restart(tmp_path):
    data = envelope(tmp_path)
    stack_file = tmp_path / "stacks.json"
    stack_file.write_text(json.dumps(data.pop("stack_samples")))
    data["stack_samples_path"] = str(stack_file)
    data["git_commit"] = "a" * 40
    result, raw = import_envelope(data, tmp_path)
    ws = Workspace("catalog", "demo", str(tmp_path))
    ws.store_run(result, raw, enrich=False)
    reopened = Workspace("catalog", "demo", str(tmp_path))
    restored = reopened.get_run(result.run_id)
    assert restored.timestamp.timestamp() == 1000
    assert restored.git_commit == "a" * 40
    assert restored.engine == "llm-station"
    assert restored.engine_workspace == "/work"
    assert restored.total_weight == 3


def test_foreign_artifact_and_bad_total_rejected(tmp_path):
    data = envelope(tmp_path)
    with pytest.raises(ValueError, match="outside"):
        import_envelope(data, tmp_path / "different-root")
    data["total_weight"] = 99
    with pytest.raises(ValueError, match="total"):
        import_envelope(data, tmp_path)


async def test_explicit_engine_errors_never_fallback(monkeypatch):
    async def forbidden(**kwargs):
        raise AssertionError("Python backend must not execute")

    monkeypatch.setattr(backend, "run_node", forbidden)
    monkeypatch.setenv("DEVTOOLS_MCP_NODE_ENGINE", "station")
    monkeypatch.delenv("DEVTOOLS_MCP_STATION_CORE", raising=False)
    error, result, _ = await backend.run(tool="cpu", binary="test.js")
    assert "DEVTOOLS_MCP_STATION_CORE" in error
    assert result is None
    monkeypatch.setenv("DEVTOOLS_MCP_NODE_ENGINE", "typo")
    error, result, _ = await backend.run(tool="cpu", binary="test.js")
    assert "Unknown Node engine" in error


async def test_live_native_capture_analyze_compare_restart(tmp_path, monkeypatch):
    executable = os.environ.get("STATION_CORE_TEST_BINARY")
    if not executable:
        pytest.skip("set STATION_CORE_TEST_BINARY to the built native core")
    monkeypatch.setenv("DEVTOOLS_MCP_NODE_ENGINE", "station")
    monkeypatch.setenv("DEVTOOLS_MCP_STATION_CORE", executable)
    script = tmp_path / "profile with spaces.js"
    script.write_text("let total=0; for(let i=0;i<3e7;i++) total+=Math.sqrt(i); console.log(total);\n")
    first_error, first, first_raw = await backend.run(tool="cpu", binary=str(script), timeout=15)
    assert first_error is None, first_error
    second_error, second, second_raw = await backend.run(tool="cpu", binary=str(script), timeout=15)
    assert second_error is None, second_error
    assert first.engine == "llm-station" and first.total_weight > 0
    ws = Workspace("test", "native", str(tmp_path))
    ws.store_run(first, first_raw, enrich=False)
    ws.store_run(second, second_raw, enrich=False)
    reopened = Workspace("test", "native", str(tmp_path))
    restored = reopened.get_run(first.run_id)
    assert restored.total_weight == first.total_weight
    from devtools_mcp.flamegraph import build_call_tree
    from devtools_mcp.flamegraph.tree import function_frame
    from devtools_mcp.index import correlate_runs

    frame = function_frame(restored.stack_samples)
    assert not frame.is_empty()
    reopened.cache_dataframe(restored.run_id, frame)
    assert not correlate_runs(reopened, first.run_id, second.run_id).is_empty()
    assert build_call_tree(restored.stack_samples).total_weight == first.total_weight
    assert Path(reopened.get_raw_path(first.run_id)).is_file()


async def test_live_failed_capture_does_not_execute_twice(tmp_path, monkeypatch):
    executable = os.environ.get("STATION_CORE_TEST_BINARY")
    if not executable:
        pytest.skip("set STATION_CORE_TEST_BINARY")
    monkeypatch.setenv("DEVTOOLS_MCP_NODE_ENGINE", "station")
    monkeypatch.setenv("DEVTOOLS_MCP_STATION_CORE", executable)
    marker = tmp_path / "executions.txt"
    script = tmp_path / "fail.js"
    script.write_text("require('fs').appendFileSync(process.argv[2], 'once\\n'); process.exit(7);\n")
    error, result, _ = await backend.run(tool="cpu", binary=str(script), args=[str(marker)], timeout=5)
    assert error is None, error
    assert result.exit_code == 7
    assert marker.read_text() == "once\n"


async def test_live_timeout_does_not_retry(tmp_path, monkeypatch):
    executable = os.environ.get("STATION_CORE_TEST_BINARY")
    if not executable:
        pytest.skip("set STATION_CORE_TEST_BINARY")
    monkeypatch.setenv("DEVTOOLS_MCP_NODE_ENGINE", "station")
    monkeypatch.setenv("DEVTOOLS_MCP_STATION_CORE", executable)
    marker = tmp_path / "executions.txt"
    script = tmp_path / "timeout.js"
    script.write_text("require('fs').appendFileSync(process.argv[2], 'once\\n'); setInterval(()=>{}, 100);\n")
    error, result, _ = await backend.run(tool="cpu", binary=str(script), args=[str(marker)], timeout=1)
    assert error is not None or result.exit_code != 0
    assert marker.read_text() == "once\n"


async def test_mixed_frame_formats_are_not_reported_as_regressions(tmp_path, monkeypatch):
    from devtools_mcp.tools import analysis_tools

    native, _ = import_envelope(envelope(tmp_path), tmp_path)
    legacy = native.model_copy(update={"run_id": "legacy", "stack_format": "v8-name-v0", "engine": "python"})
    monkeypatch.setattr(analysis_tools, "get_run", lambda ctx, rid, ws: (None, native if rid == "a" else legacy))
    result = await analysis_tools.devtools_compare(None, "a", "b")
    assert "different Node frame formats" in result
