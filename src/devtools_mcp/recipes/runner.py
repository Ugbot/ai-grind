"""Recipe execution engine: run steps sequentially as DURABLE DBOS steps.

External side-effecting sequences use the DBOS @workflow/@step pattern (durable,
SQLite-backed): a recipe's ordered steps run one after another as DBOS steps, so
an interrupted or crashed recipe resumes from its last *completed* step instead
of re-running it. DBOS checkpoints every step's outcome to its own SQLite system
database (the durability layer); the domain SQLite (recipes.db) stays the
human-facing model that the console and `recipe(action=runs|steps)` read.

A run walks the recipe's ordered steps. The first step that exits non-zero fails
the run; every later step is recorded as `skipped` (never executed). Each step's
command is run through the platform shell via the shared `run_capture` helper
(never a raw subprocess), with the environment merged from os.environ + the
recipe's env_axes + any per-run overrides. Full output is persisted once via
`write_raw`; only a bounded `tail` is kept per step.

Caching: if the recipe already has a passed run against its *current* spec_hash
and `force` is False, that prior run is returned untouched — the steps are not
re-executed. Dry runs report the plan without creating a run row.

The DBOS orchestrator (`_recipe_workflow`) and per-step activity
(`_exec_step_activity`) are SYNCHRONOUS on purpose: an async DBOS workflow binds
to the event loop that launched DBOS and cannot be re-invoked from a later loop
(fatal under pytest-asyncio's per-test loops). The sync workflow runs in DBOS's
own executor and wraps the async `run_capture` via `asyncio.run` inside the step.
The public `run_recipe` stays async and offloads the durable workflow with
`asyncio.to_thread`, preserving its signature and every existing call site.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import time
from pathlib import Path

from dbos import DBOS, SetWorkflowID

from devtools_mcp.build.exec import run_capture, tail, write_raw
from devtools_mcp.recipes import store
from devtools_mcp.recipes.db import RecipesDB, open_recipes
from devtools_mcp.recipes.dbos_app import launch_dbos
from devtools_mcp.recipes.models import MAX_STEPS, Recipe, Run, RunResult, RunStep

_POSIX = os.name != "nt"
_TAIL_LINES = 80  # bounded preview kept per step


def _shell_cmd(command: str) -> list[str]:
    """Wrap a shell command string as an argv for `run_capture` (no raw shell)."""
    assert isinstance(command, str) and command, "command must be a non-empty string"
    if _POSIX:
        return ["/bin/sh", "-c", command]
    return ["cmd", "/c", command]


def _step_env(env_axes: dict[str, str], env_extra: dict[str, str] | None) -> dict[str, str]:
    """Full child environment: os.environ ← recipe.env_axes ← env_extra."""
    assert isinstance(env_axes, dict), "env_axes must be a dict"
    merged: dict[str, str] = dict(os.environ)
    merged.update({str(k): str(v) for k, v in env_axes.items()})
    if env_extra:
        merged.update({str(k): str(v) for k, v in env_extra.items()})
    return merged


def _workflow_id(db_path: str, run_id: int) -> str:
    """Deterministic DBOS workflow id for a domain run (ties the two together).

    Includes a hash of the domain-DB path so the id is unique per (database,
    run) — the DBOS system database is process-global and shared, but a domain
    run_id only counts within its own recipes.db, so the path disambiguates
    (e.g. separate temp databases in the test suite). Determinism is what lets a
    crashed run resume/fork under the same id.
    """
    assert run_id > 0, f"bad run id {run_id}"
    tag = hashlib.sha1(str(db_path).encode("utf-8")).hexdigest()[:12]
    return f"recipe-run-{tag}-{run_id}"


# --- DBOS durable units -----------------------------------------------------
# All external side effects (shell execution, domain-DB writes, raw-log file
# writes) live INSIDE @DBOS.step so they are checkpointed exactly once and are
# not repeated when the workflow replays a completed step on recovery/resume.


@DBOS.step()
def _exec_step_activity(
    db_path: str,
    run_id: int,
    ordinal: int,
    label: str,
    command: str,
    cwd: str | None,
    timeout: int,
    env_axes: dict[str, str],
    env_extra: dict[str, str] | None,
) -> dict:
    """Run one recipe step's shell command and persist its outcome (one DBOS step).

    Returns a serializable outcome dict; `text` is the full step output (used by
    the finalize step to assemble the combined raw log).
    """
    assert ordinal >= 0, f"ordinal must be non-negative, got {ordinal}"
    env = _step_env(env_axes, env_extra)
    run_cwd = cwd or os.getcwd()
    start = time.monotonic()
    # run_capture already turns a bad launch/timeout into a non-zero rc without
    # raising; this guard covers anything unexpected (a crash inside the step)
    # so one bad step fails the run cleanly instead of unwinding the workflow.
    try:
        rc, text = asyncio.run(run_capture(_shell_cmd(command), cwd=run_cwd, timeout=timeout, env=env))
    except Exception as exc:  # noqa: BLE001 — a step crash must fail the step, not the run
        rc, text = 1, f"[devtools] step {label!r} crashed: {type(exc).__name__}: {exc}"
    duration_ms = int((time.monotonic() - start) * 1000)
    status = "passed" if rc == 0 else "failed"
    step_tail = tail(text, _TAIL_LINES)
    db = open_recipes(Path(db_path))
    try:
        store.record_step(db, run_id, ordinal, label, command, status, rc, duration_ms, step_tail)
    finally:
        db.close()
    return {
        "ordinal": ordinal,
        "label": label,
        "command": command,
        "status": status,
        "exit_code": rc,
        "duration_ms": duration_ms,
        "tail": step_tail,
        "text": text,
    }


@DBOS.step()
def _finalize_activity(
    db_path: str,
    run_id: int,
    skipped: list[dict],
    run_status: str,
    final_exit: int | None,
    chunks: list[str],
) -> str | None:
    """Record any skipped steps, write the combined raw log, finalize the run row.

    A single DBOS step so the whole finalization is checkpointed atomically and
    not repeated on replay. Returns the raw-log path (or None).
    """
    assert run_id > 0, f"bad run id {run_id}"
    raw_path = write_raw("recipe-", "\n".join(chunks)) or None
    db = open_recipes(Path(db_path))
    try:
        for s in skipped:  # bounded by MAX_STEPS
            store.record_step(db, run_id, s["ordinal"], s["label"], s["command"], "skipped", None, None, "")
        store.record_run_finish(db, run_id, run_status, final_exit, raw_path)
    finally:
        db.close()
    return raw_path


@DBOS.workflow()
def _recipe_workflow(db_path: str, key: str, run_id: int, env_extra: dict[str, str] | None) -> dict:
    """Durable orchestrator: run the recipe's steps one after another, stop on fail.

    Synchronous (see module docstring). Opens its own domain-DB connection from
    `db_path` (a DBOS workflow's args are checkpointed for recovery, so a live
    sqlite connection cannot cross the boundary — only the path can). The run row
    already exists (created by the caller); this fills in the step rows and
    finalizes it, all through checkpointed steps.
    """
    db = open_recipes(Path(db_path))
    try:
        recipe = store.get_recipe(db.conn, key)
        env_axes = recipe.env_axes
        steps = recipe.steps
    finally:
        db.close()
    assert len(steps) <= MAX_STEPS, "recipe step count over bound"

    chunks: list[str] = []
    skipped: list[dict] = []
    failed = False
    final_exit: int | None = 0
    for ordinal, step in enumerate(steps):  # bounded by MAX_STEPS
        if failed:
            skipped.append({"ordinal": ordinal, "label": step.label, "command": step.command})
            continue
        outcome = _exec_step_activity(
            db_path, run_id, ordinal, step.label, step.command, step.cwd, step.timeout, env_axes, env_extra
        )
        chunks.append(f"$ [{ordinal + 1}/{len(steps)}] {step.label}: {step.command}\n{outcome['text']}")
        if outcome["exit_code"] != 0:
            failed = True
            final_exit = outcome["exit_code"]
    run_status = "failed" if failed else "passed"
    raw_path = _finalize_activity(db_path, run_id, skipped, run_status, final_exit, chunks)
    return {"status": run_status, "exit_code": final_exit, "raw_path": raw_path}


def _invoke_workflow(db_path: str, key: str, run_id: int, env_extra: dict[str, str] | None) -> dict:
    """Launch DBOS (idempotent) and run the durable workflow under a fixed id.

    Runs in a worker thread (via `asyncio.to_thread`) so the sync workflow never
    touches the caller's event loop.
    """
    launch_dbos()
    with SetWorkflowID(_workflow_id(db_path, run_id)):
        return _recipe_workflow(db_path, key, run_id, env_extra)


# --- Public API -------------------------------------------------------------


def _prepare(db: RecipesDB, key: str, force: bool) -> tuple[Recipe, Run | None]:
    """Look up the recipe and its cache-hit run (None when forcing / no hit)."""
    assert db.conn is not None, "_prepare on closed db"
    recipe = store.get_recipe(db.conn, key)  # raises RecipesError if missing
    cached = None if force else store.cached_run(db, key)
    return recipe, cached


def _load_run_steps(db: RecipesDB, run_id: int) -> list[RunStep]:
    """Reconstruct per-step outcomes for a run (used for results + cache hits)."""
    assert db.conn is not None, "_load_run_steps on closed db"
    assert run_id > 0, f"bad run id {run_id}"
    rows = db.conn.execute(
        "SELECT ordinal, label, command, status, exit_code, duration_ms, tail "
        "FROM run_steps WHERE run_id = ? ORDER BY ordinal LIMIT ?",
        (run_id, MAX_STEPS),
    ).fetchall()
    return [
        RunStep(
            ordinal=r["ordinal"],
            label=r["label"],
            command=r["command"],
            status=r["status"],
            exit_code=r["exit_code"],
            duration_ms=r["duration_ms"],
            tail=r["tail"] or "",
        )
        for r in rows
    ]


async def run_recipe(
    db: RecipesDB,
    key: str,
    env_extra: dict[str, str] | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> RunResult:
    """Run the recipe named `key`, stopping at the first failing step.

    The step sequence runs as a durable DBOS workflow (see module docstring): an
    interrupted run resumes from its last completed step. Returns a RunResult. On
    a cache hit (a prior passed run against the current spec_hash, and `force` is
    False) the prior run is returned flagged `cached` without re-executing
    anything. With `dry_run=True` nothing runs and no run row is created — the
    planned steps are returned flagged `dry_run`.
    """
    assert db.conn is not None, "run_recipe on closed db"
    assert isinstance(key, str) and key.strip(), "key must be a non-empty string"
    recipe, cached = _prepare(db, key, force)
    if dry_run:
        planned = [RunStep(i, s.label, s.command, "pending") for i, s in enumerate(recipe.steps)]  # bounded
        return RunResult(run_id=0, recipe_key=recipe.key, status="dry-run", exit_code=None, dry_run=True, steps=planned)
    if cached is not None:
        return RunResult(
            run_id=cached.id,
            recipe_key=recipe.key,
            status=cached.status,
            exit_code=cached.exit_code,
            cached=True,
            raw_path=cached.raw_path,
            steps=_load_run_steps(db, cached.id),
        )
    run_id = store.record_run_start(db, recipe)
    try:
        await asyncio.to_thread(_invoke_workflow, str(db.path), key, run_id, env_extra)
    except Exception as exc:  # noqa: BLE001 — DBOS launch/workflow crash must not leak
        # Finalize the run as failed (capturing the error) rather than leaving it
        # stuck 'running' and propagating an unhandled exception to the caller.
        _finalize_crashed_run(db, run_id, exc)
    run = store.get_run(db.conn, run_id)
    return RunResult(
        run_id=run_id,
        recipe_key=recipe.key,
        status=run.status,
        exit_code=run.exit_code,
        raw_path=run.raw_path,
        steps=_load_run_steps(db, run_id),
    )


def _finalize_crashed_run(db: RecipesDB, run_id: int, exc: BaseException) -> None:
    """Record a run as failed after a durable-workflow launch/execution crash.

    Best-effort and never raises: the error text is persisted to a raw log so the
    console/tools show WHY the run failed, and the run row is moved out of
    'running' so it can't hang the UI. Only touches a run still 'running' (a
    workflow that already finalized keeps its real outcome).
    """
    assert db.conn is not None, "_finalize_crashed_run on closed db"
    with contextlib.suppress(Exception):
        if store.get_run(db.conn, run_id).status != "running":
            return
    raw = write_raw("recipe-", f"[devtools] recipe run crashed: {type(exc).__name__}: {exc}") or None
    with contextlib.suppress(Exception):
        store.record_run_finish(db, run_id, "failed", None, raw)


def start_background_run(key: str, env_extra: dict[str, str] | None = None, force: bool = False) -> tuple[int, bool]:
    """Start a run as a background DBOS workflow; return (run_id, cached) at once.

    Used by the dashboard's POST handler so a long recipe doesn't block the HTTP
    response: the run row is created synchronously (so the browser can be
    redirected to a run-detail page that polls) and the durable workflow is
    enqueued via `DBOS.start_workflow` — DBOS runs it in its own executor and the
    call returns immediately. On a cache hit the prior run's id is returned with
    cached=True and no workflow is started.
    """
    assert isinstance(key, str) and key.strip(), "key must be a non-empty string"
    db = open_recipes()
    try:
        recipe, cached = _prepare(db, key, force)
        if cached is not None:
            return cached.id, True
        run_id = store.record_run_start(db, recipe)
        db_path = str(db.path)
    finally:
        db.close()

    try:
        launch_dbos()
        with SetWorkflowID(_workflow_id(db_path, run_id)):
            DBOS.start_workflow(_recipe_workflow, db_path, key, run_id, env_extra)
    except Exception as exc:  # noqa: BLE001 — DBOS unavailable must not 500 the console
        # Record the run as failed so the console redirect lands on a run-detail
        # page that shows a clear failed status (not a run stuck 'running').
        db2 = open_recipes(Path(db_path))
        try:
            _finalize_crashed_run(db2, run_id, exc)
        finally:
            db2.close()
    assert run_id > 0, "background run produced no run id"
    return run_id, False
