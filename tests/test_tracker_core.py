"""Tests for the tracker domain layer: tasks, hierarchy, gate, tags, commits."""

from __future__ import annotations

import random
import string
from pathlib import Path

import pytest

from devtools_mcp.tracker import criteria, tags, tasks
from devtools_mcp.tracker.commits import link_commit
from devtools_mcp.tracker.db import TrackerDB, TrackerError, open_tracker
from devtools_mcp.tracker.models import MAX_DEPTH


@pytest.fixture
def db(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "tracker.db")
    yield tracker
    tracker.close()


@pytest.fixture
def project(db: TrackerDB):
    return tasks.create_project(db, "GR", "Grind", "test project")


def _rand_title() -> str:
    return "Task " + "".join(random.choices(string.ascii_lowercase, k=8))


class TestProjects:
    def test_create_and_get(self, db, project):
        fetched = tasks.get_project(db.conn, "gr")  # case-insensitive lookup
        assert fetched.key == "GR"
        assert fetched.close_policy == "advisory"

    def test_bad_keys_rejected(self, db):
        for bad in ("a", "1AB", "toolongkey12", "ab cd", ""):
            with pytest.raises(TrackerError):
                tasks.create_project(db, bad, "x")

    def test_duplicate_rejected(self, db, project):
        with pytest.raises(TrackerError, match="already exists"):
            tasks.create_project(db, "GR", "again")

    def test_set_policy(self, db, project):
        updated = tasks.set_policy(db, "GR", "strict")
        assert updated.close_policy == "strict"
        with pytest.raises(TrackerError):
            tasks.set_policy(db, "GR", "bogus")


class TestTaskCreation:
    def test_keys_sequence(self, db, project):
        keys = [tasks.create_task(db, "GR", _rand_title())[0].key for _ in range(5)]
        assert keys == ["GR-1", "GR-2", "GR-3", "GR-4", "GR-5"]

    def test_sequence_survives_delete(self, db, project):
        first, _ = tasks.create_task(db, "GR", "one")
        with db.transaction() as conn:
            conn.execute("DELETE FROM tasks WHERE id = ?", (first.id,))
        second, _ = tasks.create_task(db, "GR", "two")
        assert second.key == "GR-2"  # sequence never reused

    def test_parent_sets_depth(self, db, project):
        epic, _ = tasks.create_task(db, "GR", "epic", kind="epic")
        story, _ = tasks.create_task(db, "GR", "story", kind="story", parent_key=epic.key)
        assert story.parent_id == epic.id
        assert story.depth == 1

    def test_depth_bound_enforced(self, db, project):
        parent_key = None
        for level in range(MAX_DEPTH + 1):  # builds depth 0..5 — all legal
            task, _ = tasks.create_task(db, "GR", f"level {level}", parent_key=parent_key)
            assert task.depth == level
            parent_key = task.key
        with pytest.raises(TrackerError, match="exceed"):
            tasks.create_task(db, "GR", "too deep", parent_key=parent_key)

    def test_validation(self, db, project):
        with pytest.raises(TrackerError):
            tasks.create_task(db, "GR", "")
        with pytest.raises(TrackerError):
            tasks.create_task(db, "GR", "x", priority=9)
        with pytest.raises(TrackerError):
            tasks.create_task(db, "NOPE", "x")

    def test_breakdown(self, db, project):
        epic, _ = tasks.create_task(db, "GR", "epic", kind="epic")
        created = tasks.breakdown(db, epic.key, ["a", "b", "c"])
        assert len(created) == 3
        assert all(task.parent_id == epic.id for task, _ in created)
        assert all(task.kind == "story" for task, _ in created)  # epic -> story default

    def test_breakdown_bounds(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        with pytest.raises(TrackerError):
            tasks.breakdown(db, task.key, [])
        with pytest.raises(TrackerError, match="capped"):
            tasks.breakdown(db, task.key, [f"s{i}" for i in range(21)])


class TestMove:
    def test_reparent(self, db, project):
        a, _ = tasks.create_task(db, "GR", "a")
        b, _ = tasks.create_task(db, "GR", "b")
        moved = tasks.move_task(db, b.key, new_parent_key=a.key)
        assert moved.parent_id == a.id
        assert moved.depth == 1

    def test_reparent_rebases_subtree_depth(self, db, project):
        a, _ = tasks.create_task(db, "GR", "a")
        b, _ = tasks.create_task(db, "GR", "b")
        c, _ = tasks.create_task(db, "GR", "c", parent_key=b.key)
        tasks.move_task(db, b.key, new_parent_key=a.key)
        assert tasks.get_task(db.conn, c.key).depth == 2

    def test_cycle_rejected(self, db, project):
        a, _ = tasks.create_task(db, "GR", "a")
        b, _ = tasks.create_task(db, "GR", "b", parent_key=a.key)
        c, _ = tasks.create_task(db, "GR", "c", parent_key=b.key)
        with pytest.raises(TrackerError, match="cycle"):
            tasks.move_task(db, a.key, new_parent_key=c.key)
        with pytest.raises(TrackerError, match="itself"):
            tasks.move_task(db, a.key, new_parent_key=a.key)

    def test_depth_overflow_rejected(self, db, project):
        chain_top, _ = tasks.create_task(db, "GR", "chain0")
        parent = chain_top
        for level in range(1, MAX_DEPTH + 1):
            parent, _ = tasks.create_task(db, "GR", f"chain{level}", parent_key=parent.key)
        other, _ = tasks.create_task(db, "GR", "other")
        with pytest.raises(TrackerError, match="bound"):
            tasks.move_task(db, chain_top.key, new_parent_key=other.key)

    def test_to_root(self, db, project):
        a, _ = tasks.create_task(db, "GR", "a")
        b, _ = tasks.create_task(db, "GR", "b", parent_key=a.key)
        moved = tasks.move_task(db, b.key, to_root=True)
        assert moved.parent_id is None
        assert moved.depth == 0

    def test_reorder_before_sibling(self, db, project):
        parent, _ = tasks.create_task(db, "GR", "parent")
        first, _ = tasks.create_task(db, "GR", "first", parent_key=parent.key)
        second, _ = tasks.create_task(db, "GR", "second", parent_key=parent.key)
        third, _ = tasks.create_task(db, "GR", "third", parent_key=parent.key)
        tasks.move_task(db, third.key, before_key=first.key)
        ordered = [t.key for t in tasks.children_of(db.conn, parent.id)]
        assert ordered == [third.key, first.key, second.key]

    def test_reorder_non_sibling_rejected(self, db, project):
        a, _ = tasks.create_task(db, "GR", "a")
        b, _ = tasks.create_task(db, "GR", "b", parent_key=a.key)
        c, _ = tasks.create_task(db, "GR", "c")
        with pytest.raises(TrackerError, match="sibling"):
            tasks.move_task(db, b.key, before_key=c.key)


class TestStatusAndGate:
    def test_simple_transition(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        updated, warnings = tasks.set_status(db, task.key, "in_progress")
        assert updated.status == "in_progress"
        assert updated.closed_at is None
        assert warnings == []

    def test_done_sets_closed_at_and_reopen_clears(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        done, _ = tasks.set_status(db, task.key, "done")
        assert done.closed_at is not None
        reopened, _ = tasks.set_status(db, task.key, "open")
        assert reopened.closed_at is None

    def test_advisory_close_warns(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        criteria.add_criterion(db, task.id, "must do the thing")
        updated, warnings = tasks.set_status(db, task.key, "done")
        assert updated.status == "done"
        assert len(warnings) == 2  # unmet + unlinked

    def test_strict_close_rejected_then_override(self, db, project):
        tasks.set_policy(db, "GR", "strict")
        task, _ = tasks.create_task(db, "GR", "t")
        criteria.add_criterion(db, task.id, "must do the thing")
        with pytest.raises(TrackerError, match="Strict close gate"):
            tasks.set_status(db, task.key, "done")
        assert tasks.get_task(db.conn, task.key).status == "open"  # no write happened
        updated, warnings = tasks.set_status(db, task.key, "done", override=True)
        assert updated.status == "done"
        assert len(warnings) == 2

    def test_strict_close_passes_when_criteria_met(self, db, project):
        tasks.set_policy(db, "GR", "strict")
        task, _ = tasks.create_task(db, "GR", "t")
        criterion = criteria.add_criterion(db, task.id, "covered", test_ref="tests/test_x.py::test_y")
        criteria.record_result(db, criterion.id, "pass")
        updated, warnings = tasks.set_status(db, task.key, "done")
        assert updated.status == "done"
        assert warnings == []

    def test_bad_status_rejected(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        with pytest.raises(TrackerError):
            tasks.set_status(db, task.key, "finished")


class TestCriteria:
    def test_crud_and_record(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        criterion = criteria.add_criterion(db, task.id, "works")
        assert criterion.last_result is None
        updated = criteria.update_criterion(db, criterion.id, test_ref="tests/test_a.py::test_b")
        assert updated.test_ref == "tests/test_a.py::test_b"
        recorded = criteria.record_result(db, criterion.id, "fail")
        assert recorded.last_result == "fail"
        assert recorded.last_run_at is not None
        assert criteria.remove_criterion(db, criterion.id) is True
        assert criteria.remove_criterion(db, criterion.id) is False

    def test_gate_classification(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        passing = criteria.add_criterion(db, task.id, "ok", test_ref="t.py::a")
        criteria.record_result(db, passing.id, "pass")
        failing = criteria.add_criterion(db, task.id, "bad", test_ref="t.py::b")
        criteria.record_result(db, failing.id, "fail")
        unlinked = criteria.add_criterion(db, task.id, "floating")
        unmet, no_test = criteria.evaluate_gate(db.conn, task.id)
        assert {c.id for c in unmet} == {failing.id, unlinked.id}
        assert {c.id for c in no_test} == {unlinked.id}


class TestTags:
    def test_add_remove_normalized(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        name = tags.add_tag(db, task.id, "  User Story ")
        assert name == "user-story"
        assert tags.tags_for_task(db.conn, task.id) == ["user-story"]
        assert tags.remove_tag(db, task.id, "USER-STORY") is True
        assert tags.tags_for_task(db.conn, task.id) == []

    def test_bad_name_rejected(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        with pytest.raises(TrackerError):
            tags.add_tag(db, task.id, "!!!")

    def test_rule_needs_condition_and_valid_regex(self, db, project):
        with pytest.raises(TrackerError, match="condition"):
            tags.add_rule(db, "x")
        with pytest.raises(TrackerError, match="regex"):
            tags.add_rule(db, "x", match_regex="[unclosed")
        with pytest.raises(TrackerError, match="kind"):
            tags.add_rule(db, "x", match_kind="bogus")

    def test_kind_rule_applies_on_create(self, db, project):
        tags.add_rule(db, "user-story", match_kind="story")
        task, applied = tasks.create_task(db, "GR", "as a user...", kind="story")
        assert applied == ["user-story"]
        assert tags.tags_for_task(db.conn, task.id) == ["user-story"]
        _, none_applied = tasks.create_task(db, "GR", "plain", kind="task")
        assert none_applied == []

    def test_regex_rule(self, db, project):
        tags.add_rule(db, "performance", match_regex=r"(?i)\bperf\b|\blatency\b")
        _, applied = tasks.create_task(db, "GR", "Fix perf regression in query")
        assert applied == ["performance"]

    def test_parent_kind_rule(self, db, project):
        tags.add_rule(db, "acceptance", match_parent_kind="story")
        story, _ = tasks.create_task(db, "GR", "story", kind="story")
        _, applied = tasks.create_task(db, "GR", "child", parent_key=story.key)
        assert applied == ["acceptance"]
        _, root_applied = tasks.create_task(db, "GR", "rootless")
        assert root_applied == []

    def test_project_scoped_rule(self, db, project):
        other = tasks.create_project(db, "OT", "Other")
        tags.add_rule(db, "gr-only", project_id=project.id, match_kind="task")
        _, applied_gr = tasks.create_task(db, "GR", "t1")
        _, applied_ot = tasks.create_task(db, "OT", "t2")
        assert applied_gr == ["gr-only"]
        assert applied_ot == []
        assert other.key == "OT"

    def test_rule_list_and_remove(self, db, project):
        rule_id = tags.add_rule(db, "x", match_kind="epic")
        rules = tags.list_rules(db.conn)
        assert any(rule["id"] == rule_id for rule in rules)
        assert tags.remove_rule(db, rule_id) is True
        assert tags.remove_rule(db, rule_id) is False


class TestManualCommitLink:
    def test_link_and_dedupe(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        assert link_commit(db, task.key, "C:/repo", "a" * 40, "fix GR-1 things") is True
        assert link_commit(db, task.key, "C:/repo", "a" * 40, "fix GR-1 things") is False

    def test_bad_hash_rejected(self, db, project):
        task, _ = tasks.create_task(db, "GR", "t")
        with pytest.raises(TrackerError, match="hash"):
            link_commit(db, task.key, "C:/repo", "nothex!")
