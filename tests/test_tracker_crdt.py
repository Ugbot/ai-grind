"""Tests for the CRDT layer: HLC, op capture, LWW merge, convergence."""

from __future__ import annotations

from pathlib import Path

import pytest

from devtools_mcp.tracker import crdt, criteria, deps, tags, tasks
from devtools_mcp.tracker.commits import link_commit
from devtools_mcp.tracker.db import TrackerDB, open_tracker


@pytest.fixture
def db_a(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "a.db")
    yield tracker
    tracker.close()


@pytest.fixture
def db_b(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "b.db")
    yield tracker
    tracker.close()


def _sync_pair(db_x: TrackerDB, db_y: TrackerDB) -> None:
    """Exchange all ops both ways (the transport, minus HTTP)."""
    crdt.merge_ops(db_y, crdt.ops_after(db_x.conn))
    crdt.merge_ops(db_x, crdt.ops_after(db_y.conn))


def _converged(db_x: TrackerDB, db_y: TrackerDB) -> bool:
    return crdt.canonical_state(db_x.conn) == crdt.canonical_state(db_y.conn)


class TestHLC:
    def test_monotonic_and_unique(self):
        clock = crdt.HLC("aabbccdd11223344")
        stamps = [clock.next_str() for _ in range(1000)]
        assert stamps == sorted(stamps)
        assert len(set(stamps)) == 1000

    def test_observe_advances(self):
        clock = crdt.HLC("aabbccdd11223344")
        clock.observe("9999999999999-00042-deadbeef")
        assert clock.next_str() > "9999999999999-00042-deadbeef"

    def test_observe_malformed_is_ignored(self):
        clock = crdt.HLC("aabbccdd11223344")
        clock.observe("not-a-stamp")
        assert clock.next_str()  # still works


class TestIdentity:
    def test_each_db_gets_distinct_site(self, db_a, db_b):
        assert db_a.site_id != db_b.site_id
        assert len(db_a.site_id) == 32

    def test_watermark_survives_reopen(self, tmp_path):
        first = open_tracker(tmp_path / "w.db")
        tasks.create_project(first, "GR", "Grind")
        top = crdt.latest_hlc(first.conn)
        first.close()
        second = open_tracker(tmp_path / "w.db")
        try:
            assert second.hlc.next_str() > top
        finally:
            second.close()


class TestCapture:
    def test_mutations_produce_ops(self, db_a):
        tasks.create_project(db_a, "GR", "Grind")
        task, _ = tasks.create_task(db_a, "GR", "hello")
        tasks.set_status(db_a, task.key, "in_progress")
        ops = crdt.ops_after(db_a.conn)
        tables = [op["tbl"] for op in ops]
        assert "projects" in tables
        assert tables.count("tasks") >= 2  # create + status update
        task_op = next(op for op in ops if op["tbl"] == "tasks")
        assert task.uid in (task_op["pk"], "")
        assert '"project": "GR"' in task_op["payload"].replace("'", '"') or "GR" in task_op["payload"]

    def test_delete_cascade_does_not_break(self, db_a):
        tasks.create_project(db_a, "GR", "Grind")
        task, _ = tasks.create_task(db_a, "GR", "doomed")
        criteria.add_criterion(db_a, task.id, "c1")
        tags.add_tag(db_a, task.id, "x")
        with db_a.transaction() as conn:
            conn.execute("DELETE FROM tasks WHERE id = ?", (task.id,))
        deletes = [op for op in crdt.ops_after(db_a.conn) if op["op"] == "delete"]
        assert any(op["tbl"] == "tasks" for op in deletes)


class TestMerge:
    def test_one_way_replication(self, db_a, db_b):
        tasks.create_project(db_a, "GR", "Grind")
        epic, _ = tasks.create_task(db_a, "GR", "epic", kind="epic")
        child, _ = tasks.create_task(db_a, "GR", "child", parent_key=epic.key)
        criterion = criteria.add_criterion(db_a, child.id, "works", test_ref="t.py::x")
        criteria.record_result(db_a, criterion.id, "pass")
        tags.add_tag(db_a, child.id, "ported")
        deps.add_dep(db_a, child.key, epic.key)
        link_commit(db_a, child.key, "C:/repo", "a" * 40, "msg")

        counters = crdt.merge_ops(db_b, crdt.ops_after(db_a.conn))
        assert counters["deferred"] == 0
        assert _converged(db_a, db_b)
        replicated = tasks.get_task(db_b.conn, child.key)
        assert replicated.title == "child"
        assert replicated.depth == 1

    def test_idempotent_remerge(self, db_a, db_b):
        tasks.create_project(db_a, "GR", "Grind")
        tasks.create_task(db_a, "GR", "t")
        ops = crdt.ops_after(db_a.conn)
        first = crdt.merge_ops(db_b, ops)
        second = crdt.merge_ops(db_b, ops)
        assert first["new"] > 0
        assert second["new"] == 0
        assert second["applied"] == 0

    def test_no_echo_ops(self, db_a, db_b):
        """Applying remote state must not mint new local ops."""
        tasks.create_project(db_a, "GR", "Grind")
        tasks.create_task(db_a, "GR", "t")
        crdt.merge_ops(db_b, crdt.ops_after(db_a.conn))
        sites = {op["site_id"] for op in crdt.ops_after(db_b.conn)}
        assert sites == {db_a.site_id}

    def test_lww_concurrent_edit(self, db_a, db_b):
        tasks.create_project(db_a, "GR", "Grind")
        task, _ = tasks.create_task(db_a, "GR", "original")
        _sync_pair(db_a, db_b)
        tasks.update_task(db_a, task.key, title="from A")
        tasks.update_task(db_b, task.key, title="from B")  # later wall-clock-ish
        _sync_pair(db_a, db_b)
        assert _converged(db_a, db_b)
        title_a = tasks.get_task(db_a.conn, task.key).title
        assert title_a in ("from A", "from B")  # one deterministic winner everywhere

    def test_key_collision_rekeys_deterministically(self, db_a, db_b):
        tasks.create_project(db_a, "GR", "Grind")
        _sync_pair(db_a, db_b)
        a_task, _ = tasks.create_task(db_a, "GR", "made on A")  # both allocate GR-1
        b_task, _ = tasks.create_task(db_b, "GR", "made on B")
        assert a_task.key == b_task.key == "GR-1"
        _sync_pair(db_a, db_b)
        _sync_pair(db_a, db_b)  # second round propagates any re-key ops
        assert _converged(db_a, db_b)
        keys = {row[0] for row in db_a.conn.execute("SELECT key FROM tasks").fetchall()}
        assert len(keys) == 2  # both tasks survived under distinct keys
        titles = {row[0] for row in db_a.conn.execute("SELECT title FROM tasks").fetchall()}
        assert titles == {"made on A", "made on B"}

    def test_status_and_criteria_flow_across_sites(self, db_a, db_b):
        tasks.create_project(db_a, "GR", "Grind")
        task, _ = tasks.create_task(db_a, "GR", "shared")
        criterion = criteria.add_criterion(db_a, task.id, "done means done")
        _sync_pair(db_a, db_b)
        # B records the test result; A closes after syncing it back.
        b_criterion = criteria.list_criteria(db_b.conn, tasks.get_task(db_b.conn, task.key).id)[0]
        criteria.record_result(db_b, b_criterion.id, "pass")
        _sync_pair(db_a, db_b)
        merged = criteria.list_criteria(db_a.conn, task.id)[0]
        assert merged.last_result == "pass"
        assert merged.id == criterion.id  # same local row, updated in place

    def test_three_replicas_transitive(self, db_a, db_b, tmp_path):
        db_c = open_tracker(tmp_path / "c.db")
        try:
            tasks.create_project(db_a, "GR", "Grind")
            tasks.create_task(db_a, "GR", "origin")
            _sync_pair(db_a, db_b)  # A <-> B
            _sync_pair(db_b, db_c)  # B <-> C (C never talks to A)
            assert _converged(db_a, db_c)
        finally:
            db_c.close()

    def test_batch_bound_enforced(self, db_a):
        from devtools_mcp.tracker.db import TrackerError

        with pytest.raises(TrackerError, match="batch too large"):
            crdt.merge_ops(db_a, [{"hlc": "x"}] * (crdt.MAX_OPS_PER_BATCH + 1))

    def test_deferred_op_recovers_in_later_batch(self, db_a, db_b):
        """A child that arrives before its project must land once the project
        shows up in a *separate, later* batch — not only within one batch's passes."""
        tasks.create_project(db_a, "GR", "Grind")
        task, _ = tasks.create_task(db_a, "GR", "child-first")
        all_ops = crdt.ops_after(db_a.conn)
        project_ops = [op for op in all_ops if op["tbl"] == "projects"]
        task_ops = [op for op in all_ops if op["tbl"] == "tasks"]
        assert project_ops and task_ops

        def _landed(key: str) -> bool:
            return db_b.conn.execute("SELECT 1 FROM tasks WHERE key = ?", (key,)).fetchone() is not None

        # Batch 1: only the task op — its project referent is absent, so it defers.
        first = crdt.merge_ops(db_b, task_ops)
        assert first["deferred"] >= 1
        assert not _landed(task.key)  # not landed yet

        # Batch 2: the project op arrives separately — the backlog must recover.
        second = crdt.merge_ops(db_b, project_ops)
        assert second["recovered"] >= 1
        assert _landed(task.key)
        assert tasks.get_task(db_b.conn, task.key).title == "child-first"

        # A third empty-ish merge makes no further changes and stays converged.
        crdt.merge_ops(db_b, crdt.ops_after(db_a.conn))
        assert _converged(db_a, db_b)


class TestStatus:
    def test_status_shape(self, db_a):
        tasks.create_project(db_a, "GR", "Grind")
        info = crdt.status(db_a)
        assert info["site_id"] == db_a.site_id
        assert info["ops"] >= 1
        assert info["latest_hlc"] is not None
        assert info["peers"] == []
