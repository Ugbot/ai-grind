"""Tests for the recipes domain: register/spec_hash/upsert, run ordering,
stop-on-fail, caching, env, and frame bounds."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from devtools_mcp.recipes import frames, store
from devtools_mcp.recipes.db import RecipesDB, RecipesError, open_recipes
from devtools_mcp.recipes.frames import FRAME_MAX_ROWS
from devtools_mcp.recipes.runner import _workflow_id, run_recipe


@pytest.fixture
def db(tmp_path: Path) -> RecipesDB:
    recipes = open_recipes(tmp_path / "recipes.db")
    yield recipes
    recipes.close()


def _spec(key="demo", kind="test", steps=None, env_axes=None):
    return {
        "key": key,
        "name": key.title(),
        "kind": kind,
        "steps": steps if steps is not None else [{"label": "one", "command": "true"}],
        "env_axes": env_axes or {},
    }


class TestRegister:
    def test_spec_hash_is_stable(self, db):
        a = store.register_recipe(db, _spec())
        b = store.register_recipe(db, _spec())
        assert a.spec_hash == b.spec_hash
        # upsert, not insert: still one row
        assert db.conn.execute("SELECT COUNT(*) FROM recipes").fetchone()[0] == 1

    def test_hash_changes_when_steps_change(self, db):
        a = store.register_recipe(db, _spec())
        b = store.register_recipe(db, _spec(steps=[{"label": "one", "command": "echo changed"}]))
        assert a.spec_hash != b.spec_hash
        assert db.conn.execute("SELECT COUNT(*) FROM recipes").fetchone()[0] == 1

    def test_hash_independent_of_step_key_order(self, db):
        s1 = _spec(steps=[{"label": "a", "command": "true", "timeout": 30}])
        s2 = _spec(steps=[{"timeout": 30, "command": "true", "label": "a"}])
        assert store.register_recipe(db, s1).spec_hash == store.register_recipe(db, s2).spec_hash

    def test_bad_key_rejected(self, db):
        with pytest.raises(RecipesError):
            store.register_recipe(db, _spec(key="has spaces"))

    def test_step_without_command_rejected(self, db):
        with pytest.raises(RecipesError):
            store.register_recipe(db, _spec(steps=[{"label": "x"}]))

    def test_list_and_get(self, db):
        store.register_recipe(db, _spec(key="a", kind="build"))
        store.register_recipe(db, _spec(key="b", kind="test"))
        assert {r.key for r in store.list_recipes(db.conn)} == {"a", "b"}
        assert [r.key for r in store.list_recipes(db.conn, kind="build")] == ["a"]
        assert store.get_recipe(db.conn, "a").kind == "build"
        with pytest.raises(RecipesError):
            store.get_recipe(db.conn, "missing")


class TestRun:
    async def test_runs_steps_in_order_and_persists(self, db):
        store.register_recipe(
            db,
            _spec(steps=[{"label": "first", "command": "true"}, {"label": "second", "command": "true"}]),
        )
        result = await run_recipe(db, "demo")
        assert result.status == "passed"
        assert result.exit_code == 0
        assert [s.status for s in result.steps] == ["passed", "passed"]
        rows = db.conn.execute(
            "SELECT ordinal, label, status FROM run_steps WHERE run_id = ? ORDER BY ordinal",
            (result.run_id,),
        ).fetchall()
        assert [(r["ordinal"], r["label"], r["status"]) for r in rows] == [
            (0, "first", "passed"),
            (1, "second", "passed"),
        ]
        run = store.get_run(db.conn, result.run_id)
        assert run.status == "passed" and run.finished_at is not None

    async def test_stops_on_first_failure(self, db):
        store.register_recipe(
            db,
            _spec(
                steps=[
                    {"label": "ok", "command": "true"},
                    {"label": "boom", "command": "exit 3"},
                    {"label": "never", "command": "true"},
                ]
            ),
        )
        result = await run_recipe(db, "demo")
        assert result.status == "failed"
        assert result.exit_code == 3
        assert [s.status for s in result.steps] == ["passed", "failed", "skipped"]
        # the skipped step was never executed (no exit code recorded)
        assert result.steps[2].exit_code is None

    async def test_cache_hit_does_not_rerun(self, db):
        store.register_recipe(db, _spec(steps=[{"label": "one", "command": "true"}]))
        first = await run_recipe(db, "demo")
        assert not first.cached
        second = await run_recipe(db, "demo")
        assert second.cached
        assert second.run_id == first.run_id
        # only one run row persisted, the second call reused the first
        assert db.conn.execute("SELECT COUNT(*) FROM recipe_runs").fetchone()[0] == 1

    async def test_force_reruns(self, db):
        store.register_recipe(db, _spec(steps=[{"label": "one", "command": "true"}]))
        first = await run_recipe(db, "demo")
        forced = await run_recipe(db, "demo", force=True)
        assert not forced.cached
        assert forced.run_id != first.run_id
        assert db.conn.execute("SELECT COUNT(*) FROM recipe_runs").fetchone()[0] == 2

    async def test_failed_run_is_not_cached(self, db):
        store.register_recipe(db, _spec(steps=[{"label": "boom", "command": "exit 1"}]))
        await run_recipe(db, "demo")
        assert store.cached_run(db, "demo") is None
        again = await run_recipe(db, "demo")
        assert not again.cached  # a failed run is never a cache hit

    async def test_spec_change_invalidates_cache(self, db):
        store.register_recipe(db, _spec(steps=[{"label": "one", "command": "true"}]))
        await run_recipe(db, "demo")
        assert store.cached_run(db, "demo") is not None
        store.register_recipe(db, _spec(steps=[{"label": "one", "command": "echo different"}]))
        assert store.cached_run(db, "demo") is None

    async def test_dry_run_creates_no_row(self, db):
        store.register_recipe(db, _spec(steps=[{"label": "one", "command": "true"}]))
        result = await run_recipe(db, "demo", dry_run=True)
        assert result.dry_run and result.run_id == 0
        assert db.conn.execute("SELECT COUNT(*) FROM recipe_runs").fetchone()[0] == 0

    async def test_env_axes_and_extra_reach_the_shell(self, db):
        store.register_recipe(
            db,
            _spec(steps=[{"label": "echo", "command": "echo axis=$AXIS extra=$EXTRA"}], env_axes={"AXIS": "yes"}),
        )
        result = await run_recipe(db, "demo", env_extra={"EXTRA": "also"})
        assert result.status == "passed"
        assert "axis=yes" in result.steps[0].tail
        assert "extra=also" in result.steps[0].tail

    async def test_missing_recipe_raises(self, db):
        with pytest.raises(RecipesError):
            await run_recipe(db, "nope")


class TestDurability:
    """Prove the DBOS-backed runner resumes without re-running completed steps.

    Each step appends a line to its own marker file, so the number of lines is
    exactly the number of times that step's shell command actually executed.
    `fork_workflow(id, start_step)` restarts the durable workflow from a given
    step, replaying the results of earlier *completed* steps from the DBOS
    checkpoint instead of re-executing them, the same replay DBOS performs when
    recovering an interrupted/crashed workflow. So forking past step 1 must leave
    step 1's marker untouched (not re-run) while step 2 runs again."""

    def test_resume_does_not_rerun_completed_step(self, db, tmp_path):
        from dbos import DBOS

        m1 = tmp_path / "step1.log"
        m2 = tmp_path / "step2.log"
        store.register_recipe(
            db,
            _spec(
                steps=[
                    {"label": "first", "command": f"printf 'x\\n' >> {m1}"},
                    {"label": "second", "command": f"printf 'x\\n' >> {m2}; exit 7"},
                ]
            ),
        )

        # First execution: step 2 fails, so the run fails after both steps run once.
        first = asyncio.run(run_recipe(db, "demo"))
        assert first.status == "failed" and first.exit_code == 7
        assert [s.status for s in first.steps] == ["passed", "failed"]
        assert m1.read_text().count("x") == 1  # step 1 ran exactly once
        assert m2.read_text().count("x") == 1  # step 2 ran exactly once

        # The durable workflow completed under a deterministic, resumable id.
        wfid = _workflow_id(str(db.path), first.run_id)
        step_names = [s["function_name"] for s in DBOS.list_workflow_steps(wfid)]
        assert step_names.count("_exec_step_activity") == 2  # both steps checkpointed

        # Resume from step 2 (function_id 2): step 1 (id 1) is replayed from the
        # checkpoint and MUST NOT re-run; step 2 executes again.
        handle = DBOS.fork_workflow(wfid, 2)
        result = handle.get_result()
        assert result["status"] == "failed" and result["exit_code"] == 7
        assert m1.read_text().count("x") == 1  # step 1 NOT re-executed on resume
        assert m2.read_text().count("x") == 2  # step 2 ran again


class TestFrames:
    async def test_frame_columns_and_bound(self, db):
        store.register_recipe(db, _spec(steps=[{"label": "one", "command": "true"}]))
        result = await run_recipe(db, "demo")
        recipes_df = frames.recipes_frame(db.conn)
        runs_df = frames.runs_frame(db.conn, "demo")
        steps_df = frames.steps_frame(db.conn, result.run_id)
        assert recipes_df.height == 1 and "last_status" in recipes_df.columns
        assert recipes_df["last_status"][0] == "passed"
        assert runs_df.height == 1 and runs_df["passed"][0] == 1
        assert steps_df.height == 1 and steps_df["status"][0] == "passed"
        for frame in (recipes_df, runs_df, steps_df):
            assert frame.height <= FRAME_MAX_ROWS


class TestExecutionSnapshot:
    async def test_cache_uses_effective_explicit_environment(self, db):
        store.register_recipe(db, _spec(env_axes={"AXIS": "one"}))
        first = await run_recipe(db, "demo")
        same = await run_recipe(db, "demo", env_extra={"AXIS": "one"})
        assert same.cached and same.run_id == first.run_id
        changed = await run_recipe(db, "demo", env_extra={"AXIS": "two"})
        assert not changed.cached and changed.run_id != first.run_id
        repeated = await run_recipe(db, "demo", env_extra={"AXIS": "two"})
        assert repeated.cached and repeated.run_id == changed.run_id

    def test_snapshot_freezes_cwd_and_explicit_inputs_only(self, db, tmp_path, monkeypatch):
        import sqlite3

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("AMBIENT_SECRET", "must-not-persist")
        recipe = store.register_recipe(db, _spec(env_axes={"AXIS": "original"}))
        run_id = store.record_run_start(db, recipe, {"EXTRA": "override"})
        snapshot = store.get_execution_snapshot(db.conn, run_id)
        assert snapshot["steps"][0]["cwd"] == str(tmp_path)
        assert snapshot["env"] == {"AXIS": "original", "EXTRA": "override"}
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db.conn.execute("UPDATE recipe_runs SET execution_snapshot = '{}' WHERE id = ?", (run_id,))

    def test_execution_after_edit_uses_original_snapshot(self, db, tmp_path, monkeypatch):
        from devtools_mcp.recipes.runner import _invoke_workflow

        original_dir = tmp_path / "original"
        original_dir.mkdir()
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        monkeypatch.chdir(original_dir)
        recipe = store.register_recipe(
            db, _spec(steps=[{"command": 'printf "%s" "$AXIS/$EXTRA" > result'}], env_axes={"AXIS": "old"})
        )
        run_id = store.record_run_start(db, recipe, {"EXTRA": "saved"})
        store.register_recipe(db, _spec(steps=[{"command": "exit 9"}], env_axes={"AXIS": "new"}))
        monkeypatch.chdir(other_dir)
        result = _invoke_workflow(str(db.path), "demo", run_id, {"EXTRA": "changed"})
        assert result["status"] == "passed"
        assert (original_dir / "result").read_text() == "old/saved"
        assert not (other_dir / "result").exists()

    def test_fork_after_edit_preserves_remaining_original_steps(self, db, tmp_path):
        from dbos import DBOS

        marker = tmp_path / "original.log"
        store.register_recipe(
            db,
            _spec(steps=[{"command": "true"}, {"command": f'echo "$VALUE" >> {marker}'}]),
        )
        first = asyncio.run(run_recipe(db, "demo", env_extra={"VALUE": "original"}))
        store.register_recipe(db, _spec(steps=[{"command": "exit 99"}]))
        result = DBOS.fork_workflow(_workflow_id(str(db.path), first.run_id), 2).get_result()
        assert result["status"] == "passed"
        assert marker.read_text().splitlines() == ["original", "original"]
        assert db.conn.execute("SELECT COUNT(*) FROM run_steps WHERE run_id = ?", (first.run_id,)).fetchone()[0] == 2
