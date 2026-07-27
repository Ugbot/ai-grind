"""Domain layer for recipes: register/lookup recipes and record runs.

A recipe's `spec_hash` is the sha256 of the canonical JSON of its executable
spec ({kind, env_axes, steps}). It is what makes a cached run valid: a cached
result is only reused while the recipe's current spec_hash still matches the
hash the run executed against, so editing any step invalidates the cache.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3

from devtools_mcp.recipes.db import RecipesDB, RecipesError, utc_now_iso
from devtools_mcp.recipes.models import DEFAULT_KIND, MAX_STEPS, Recipe, Run, Step

KEY_RE: re.Pattern[str] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
# A recipe kind is a short slug (domain-agnostic: build/test/setup/deploy/…);
# validated for shape, not against a fixed enum, so the engine stays generic.
KIND_RE: re.Pattern[str] = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,31}$")
NAME_MAX: int = 200


def _spec_hash(kind: str, env_axes: dict[str, str], steps: list[dict]) -> str:
    """sha256 of the canonical JSON of the executable spec — the cache key."""
    assert isinstance(env_axes, dict), "env_axes must be a dict"
    assert isinstance(steps, list), "steps must be a list"
    canonical = json.dumps(
        {"kind": kind, "env_axes": env_axes, "steps": steps},
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert len(digest) == 64, f"unexpected digest length {len(digest)}"
    return digest


def _normalize_spec(spec: dict) -> tuple[str, str, str, str, dict[str, str], list[dict], str | None]:
    """Validate + canonicalize a recipe spec dict → tuple of column values.

    Returns (key, name, kind, summary, env_axes, step_dicts, source). Raises
    RecipesError on any malformed field (the caller reports it).
    """
    assert isinstance(spec, dict), "recipe spec must be an object"
    key = str(spec.get("key") or "").strip()
    if not KEY_RE.match(key):
        raise RecipesError(f"invalid recipe key {key!r} (allowed: [A-Za-z0-9][A-Za-z0-9._:-]*, ≤64 chars)")
    name = str(spec.get("name") or key).strip()[:NAME_MAX]
    kind = str(spec.get("kind") or DEFAULT_KIND).strip() or DEFAULT_KIND
    if not KIND_RE.match(kind):
        raise RecipesError(f"invalid recipe kind {kind!r} (allowed: [A-Za-z][A-Za-z0-9_-]*, ≤32 chars)")
    summary = str(spec.get("summary") or "").strip()
    source = spec.get("source")
    source = str(source).strip() if source else None

    env_raw = spec.get("env_axes") or {}
    if not isinstance(env_raw, dict):
        raise RecipesError("env_axes must be an object of NAME->value")
    env_axes = {str(k): str(v) for k, v in env_raw.items()}

    steps_raw = spec.get("steps") or []
    if not isinstance(steps_raw, list):
        raise RecipesError("steps must be a list")
    if not steps_raw:
        raise RecipesError("recipe must have at least one step")
    if len(steps_raw) > MAX_STEPS:
        raise RecipesError(f"too many steps: {len(steps_raw)} > {MAX_STEPS}")
    step_dicts: list[dict] = []
    for i, raw in enumerate(steps_raw):  # bounded by MAX_STEPS check above
        if not isinstance(raw, dict):
            raise RecipesError(f"step {i} must be an object")
        command = str(raw.get("command") or "").strip()
        if not command:
            raise RecipesError(f"step {i} has no command")
        step_dicts.append(Step.from_dict(raw, i).to_dict())
    return key, name, kind, summary, env_axes, step_dicts, source


def register_recipe(db: RecipesDB, spec: dict) -> Recipe:
    """Upsert one recipe from a spec dict (by key). Recomputes spec_hash.

    This is the public seeding API: out-of-tree plugins call it to register their
    build/test/setup/deploy pipelines. Returns the stored Recipe.
    """
    assert db.conn is not None, "register_recipe on closed db"
    key, name, kind, summary, env_axes, step_dicts, source = _normalize_spec(spec)
    spec_hash = _spec_hash(kind, env_axes, step_dicts)
    env_json = json.dumps(env_axes, sort_keys=True)
    steps_json = json.dumps(step_dicts)
    now = utc_now_iso()
    with db.transaction() as conn:
        existing = conn.execute("SELECT id, created_at FROM recipes WHERE key = ?", (key,)).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO recipes (key, name, kind, summary, env_axes, steps, spec_hash, source, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (key, name, kind, summary, env_json, steps_json, spec_hash, source, now, now),
            )
        else:
            conn.execute(
                "UPDATE recipes SET name = ?, kind = ?, summary = ?, env_axes = ?, steps = ?, "
                "spec_hash = ?, source = ?, updated_at = ? WHERE key = ?",
                (name, kind, summary, env_json, steps_json, spec_hash, source, now, key),
            )
    recipe = get_recipe(db.conn, key)
    assert recipe.spec_hash == spec_hash, "spec_hash not persisted"
    return recipe


def get_recipe(conn: sqlite3.Connection, key: str) -> Recipe:
    """Fetch one recipe by key. Raises RecipesError if absent."""
    assert conn is not None, "get_recipe on missing connection"
    normalized = (key or "").strip()
    row = conn.execute("SELECT * FROM recipes WHERE key = ?", (normalized,)).fetchone()
    if row is None:
        raise RecipesError(f"no recipe with key {normalized!r}")
    return Recipe.from_row(row)


def list_recipes(conn: sqlite3.Connection, kind: str | None = None) -> list[Recipe]:
    """All recipes (optionally filtered by kind), newest-updated first."""
    assert conn is not None, "list_recipes on missing connection"
    if kind is not None:
        rows = conn.execute(
            "SELECT * FROM recipes WHERE kind = ? ORDER BY updated_at DESC, id DESC LIMIT ?",
            (kind, MAX_STEPS * 100),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM recipes ORDER BY updated_at DESC, id DESC LIMIT ?",
            (MAX_STEPS * 100,),
        ).fetchall()
    assert len(rows) <= MAX_STEPS * 100, "recipe list over bound"
    return [Recipe.from_row(row) for row in rows]


def record_run_start(db: RecipesDB, recipe: Recipe) -> int:
    """Create a recipe_runs row (status='running') and return its id."""
    assert db.conn is not None, "record_run_start on closed db"
    assert recipe.id > 0, "recipe must be persisted before a run"
    now = utc_now_iso()
    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO recipe_runs (recipe_id, spec_hash, status, started_at) VALUES (?, ?, 'running', ?)",
            (recipe.id, recipe.spec_hash, now),
        )
        run_id = int(cur.lastrowid or 0)
    assert run_id > 0, "record_run_start produced no run id"
    return run_id


def record_step(
    db: RecipesDB,
    run_id: int,
    ordinal: int,
    label: str,
    command: str,
    status: str,
    exit_code: int | None,
    duration_ms: int | None,
    tail: str,
) -> None:
    """Persist one step outcome within a run."""
    assert db.conn is not None, "record_step on closed db"
    assert ordinal >= 0, f"ordinal must be non-negative, got {ordinal}"
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO run_steps (run_id, ordinal, label, command, exit_code, status, duration_ms, tail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, ordinal, label, command, exit_code, status, duration_ms, tail or ""),
        )


def record_run_finish(
    db: RecipesDB,
    run_id: int,
    status: str,
    exit_code: int | None,
    raw_path: str | None,
) -> None:
    """Finalize a run row with its terminal status, exit code, and raw log path."""
    assert db.conn is not None, "record_run_finish on closed db"
    assert run_id > 0, f"bad run id {run_id}"
    with db.transaction() as conn:
        conn.execute(
            "UPDATE recipe_runs SET status = ?, exit_code = ?, raw_path = ?, finished_at = ? WHERE id = ?",
            (status, exit_code, raw_path, utc_now_iso(), run_id),
        )


def get_run(conn: sqlite3.Connection, run_id: int) -> Run:
    """Fetch one run by id. Raises RecipesError if absent."""
    assert conn is not None, "get_run on missing connection"
    row = conn.execute("SELECT * FROM recipe_runs WHERE id = ?", (run_id,)).fetchone()
    if row is None:
        raise RecipesError(f"no run with id {run_id}")
    return Run.from_row(row)


def cached_run(db: RecipesDB, key: str) -> Run | None:
    """Latest passed run whose spec_hash matches the recipe's current spec_hash.

    None when there is no such run (recipe never passed, or its spec changed
    since the last pass) — the signal to (re)run.
    """
    assert db.conn is not None, "cached_run on closed db"
    recipe = get_recipe(db.conn, key)
    row = db.conn.execute(
        "SELECT * FROM recipe_runs WHERE recipe_id = ? AND spec_hash = ? AND status = 'passed' "
        "ORDER BY id DESC LIMIT 1",
        (recipe.id, recipe.spec_hash),
    ).fetchone()
    return Run.from_row(row) if row is not None else None
