"""Tests for persistent run store."""

from __future__ import annotations

from pathlib import Path

from devtools_mcp.models import create_run_base
from devtools_mcp.store.run_store import RunStore
from devtools_mcp.valgrind.models import MemcheckResult


def _mk(run_id: str = "", task_key: str = "GR-1") -> MemcheckResult:
    return MemcheckResult(
        run_id=run_id or create_run_base("valgrind", "memcheck", "./app").run_id,
        suite="valgrind",
        tool="memcheck",
        binary="./app",
        label="test run",
        task_key=task_key,
    )


def test_persist_and_load(tmp_path: Path):
    store = RunStore(tmp_path)
    result = _mk()
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


def test_index_db_created_beside_runs(tmp_path: Path):
    store = RunStore(tmp_path)
    store.persist(_mk(), workspace_id="ws1")
    assert (tmp_path / "runs.db").is_file()


def test_index_drives_lookups(tmp_path: Path):
    store = RunStore(tmp_path)
    a = _mk(task_key="GR-1")
    b = _mk(task_key="GR-2")
    store.persist(a, workspace_id="ws1")
    store.persist(b, workspace_id="ws1")
    assert set(store.list_run_ids()) == {a.run_id, b.run_id}
    assert store.runs_for_task_key("gr-1") == [a.run_id]  # case-insensitive
    assert store.runs_for_task_key("GR-2") == [b.run_id]


def test_delete_removes_from_index(tmp_path: Path):
    store = RunStore(tmp_path)
    r = _mk()
    store.persist(r, workspace_id="ws1")
    assert store.delete_run(r.run_id) is True
    assert store.list_run_ids() == []
    assert store.runs_for_task_key("GR-1") == []


def test_disk_scan_fallback_backfills_unindexed_runs(tmp_path: Path):
    # Persist through one store (writes both blobs + index), then drop the index
    # file to simulate runs that predate the index / were written elsewhere.
    first = RunStore(tmp_path)
    r = _mk(task_key="GR-9")
    first.persist(r, workspace_id="ws1")
    first.close()
    (tmp_path / "runs.db").unlink()

    # A fresh store rebuilds the index from disk on first read.
    second = RunStore(tmp_path)
    assert r.run_id in second.list_run_ids()
    assert second.runs_for_task_key("GR-9") == [r.run_id]
