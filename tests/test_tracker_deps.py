"""Tests for task dependencies and the execution-plan resolver."""

from __future__ import annotations

from pathlib import Path

import pytest

from devtools_mcp.tracker import deps, tasks
from devtools_mcp.tracker.db import TrackerDB, TrackerError, open_tracker


@pytest.fixture
def db(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "tracker.db")
    yield tracker
    tracker.close()


@pytest.fixture
def project(db: TrackerDB):
    return tasks.create_project(db, "GR", "Grind")


def _task(db, title, **kwargs):
    task, _ = tasks.create_task(db, "GR", title, **kwargs)
    return task


class TestEdges:
    def test_add_list_remove(self, db, project):
        a = _task(db, "a")
        b = _task(db, "b")
        assert deps.add_dep(db, b.key, a.key) is True
        assert deps.add_dep(db, b.key, a.key) is False  # idempotent
        assert [t.key for t in deps.deps_of(db.conn, b.id)] == [a.key]
        assert [t.key for t in deps.dependents_of(db.conn, a.id)] == [b.key]
        assert deps.remove_dep(db, b.key, a.key) is True
        assert deps.remove_dep(db, b.key, a.key) is False

    def test_self_dep_rejected(self, db, project):
        a = _task(db, "a")
        with pytest.raises(TrackerError, match="itself"):
            deps.add_dep(db, a.key, a.key)

    def test_cross_project_rejected(self, db, project):
        tasks.create_project(db, "OT", "Other")
        a = _task(db, "a")
        other, _ = tasks.create_task(db, "OT", "elsewhere")
        with pytest.raises(TrackerError, match="one project"):
            deps.add_dep(db, a.key, other.key)

    def test_cycle_rejected(self, db, project):
        a, b, c = _task(db, "a"), _task(db, "b"), _task(db, "c")
        deps.add_dep(db, b.key, a.key)  # b <- a
        deps.add_dep(db, c.key, b.key)  # c <- b
        with pytest.raises(TrackerError, match="cycle"):
            deps.add_dep(db, a.key, c.key)  # a <- c closes the loop

    def test_unblocked_by_closing(self, db, project):
        a, b, c = _task(db, "a"), _task(db, "b"), _task(db, "c")
        deps.add_dep(db, c.key, a.key)
        deps.add_dep(db, c.key, b.key)
        assert deps.unblocked_by_closing(db.conn, a.id) == []  # c still waits on b
        tasks.set_status(db, b.key, "done")
        assert deps.unblocked_by_closing(db.conn, a.id) == [c.key]


class TestResolver:
    def test_plan_classification_and_layers(self, db, project):
        schema = _task(db, "schema")
        parser = _task(db, "parser")
        ui = _task(db, "ui")
        ship = _task(db, "ship")
        deps.add_dep(db, parser.key, schema.key)
        deps.add_dep(db, ui.key, parser.key)
        deps.add_dep(db, ship.key, ui.key)
        plan = deps.resolve_plan(db.conn, "GR")
        assert [t.key for t in plan.ready] == [schema.key]
        blocked_keys = {t.key: b for t, b in plan.blocked}
        assert blocked_keys == {
            parser.key: [schema.key],
            ui.key: [parser.key],
            ship.key: [ui.key],
        }
        assert plan.layers == [[schema.key], [parser.key], [ui.key], [ship.key]]

    def test_closed_deps_are_satisfied(self, db, project):
        a, b = _task(db, "a"), _task(db, "b")
        deps.add_dep(db, b.key, a.key)
        tasks.set_status(db, a.key, "done")
        plan = deps.resolve_plan(db.conn, "GR")
        assert [t.key for t in plan.ready] == [b.key]
        assert plan.blocked == []

    def test_parent_with_open_children_waits(self, db, project):
        epic = _task(db, "epic", kind="epic")
        child = _task(db, "child", parent_key=epic.key)
        plan = deps.resolve_plan(db.conn, "GR")
        assert [t.key for t in plan.waiting_on_children] == [epic.key]
        assert [t.key for t in plan.ready] == [child.key]
        tasks.set_status(db, child.key, "done")
        plan = deps.resolve_plan(db.conn, "GR")
        assert [t.key for t in plan.ready] == [epic.key]

    def test_goal_scopes_the_plan(self, db, project):
        goal = _task(db, "goal")
        needed = _task(db, "needed")
        unrelated = _task(db, "unrelated")
        deps.add_dep(db, goal.key, needed.key)
        plan = deps.resolve_plan(db.conn, "GR", goal_key=goal.key)
        keys = {t.key for t in plan.ready} | {t.key for t, _ in plan.blocked}
        assert keys == {goal.key, needed.key}
        assert unrelated.key not in keys

    def test_goal_includes_subtree_and_dep_subtrees(self, db, project):
        goal = _task(db, "goal", kind="epic")
        sub = _task(db, "sub", parent_key=goal.key)
        dep_parent = _task(db, "dep parent")
        dep_child = _task(db, "dep child", parent_key=dep_parent.key)
        deps.add_dep(db, goal.key, dep_parent.key)
        plan = deps.resolve_plan(db.conn, "GR", goal_key=goal.key)
        all_keys = (
            {t.key for t in plan.ready} | {t.key for t in plan.waiting_on_children} | {t.key for t, _ in plan.blocked}
        )
        assert all_keys == {goal.key, sub.key, dep_parent.key, dep_child.key}

    def test_closed_goal_is_empty_plan(self, db, project):
        goal = _task(db, "goal")
        tasks.set_status(db, goal.key, "done")
        plan = deps.resolve_plan(db.conn, "GR", goal_key=goal.key)
        assert plan.open_count == 0

    def test_diamond_layers(self, db, project):
        base = _task(db, "base")
        left = _task(db, "left")
        right = _task(db, "right")
        top = _task(db, "top")
        deps.add_dep(db, left.key, base.key)
        deps.add_dep(db, right.key, base.key)
        deps.add_dep(db, top.key, left.key)
        deps.add_dep(db, top.key, right.key)
        plan = deps.resolve_plan(db.conn, "GR")
        assert plan.layers == [
            [base.key],
            sorted([left.key, right.key]),
            [top.key],
        ]
