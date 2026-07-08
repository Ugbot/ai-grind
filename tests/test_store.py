"""Tests for persistent run store."""

from __future__ import annotations

from pathlib import Path

from devtools_mcp.models import create_run_base
from devtools_mcp.store.run_store import RunStore
from devtools_mcp.valgrind.models import MemcheckResult


def test_persist_and_load(tmp_path: Path):
    store = RunStore(tmp_path)
    result = MemcheckResult(
        run_id=create_run_base("valgrind", "memcheck", "./app").run_id,
        suite="valgrind",
        tool="memcheck",
        binary="./app",
        label="test run",
        task_key="GR-1",
    )
    store.persist(result, summary="summary text", workspace_id="ws1", workspace_name="default")
    loaded = store.load_run(result.run_id)
    assert loaded is not None
    assert loaded.label == "test run"
    assert loaded.task_key == "GR-1"
    assert store.load_summary(result.run_id) == "summary text"
    assert result.run_id in store.list_run_ids()
    assert store.runs_for_task_key("GR-1") == [result.run_id]
    store.delete_run(result.run_id)
    assert store.load_run(result.run_id) is None
