"""Assemble recipes page data for the visualization terminal.

Mirrors tracker_data.py: recipes list → per-recipe run history → run detail.
"""

from __future__ import annotations

from devtools_mcp.recipes import frames, store
from devtools_mcp.recipes.db import RecipesDB, RecipesError
from devtools_mcp.viz import render

MAX_RUN_STEPS: int = 500


def recipes_overview(db: RecipesDB) -> str:
    """All recipes as cards (name, kind, last-run status, step/run counts)."""
    assert db.conn is not None, "recipes overview on closed db"
    rows = frames.recipes_frame(db.conn).to_dicts()
    return render.recipes_page(rows)


def recipe_runs_page(db: RecipesDB, key: str) -> str | None:
    """One recipe: metadata, its steps, and run history. None if unknown."""
    assert db.conn is not None, "recipe_runs_page on closed db"
    try:
        recipe = store.get_recipe(db.conn, key)
    except RecipesError:
        return None
    runs_table = render.table_from_df(frames.runs_frame(db.conn, recipe.key))
    recipe_dict = {
        "key": recipe.key,
        "name": recipe.name,
        "kind": recipe.kind,
        "summary": recipe.summary,
        "spec_hash": recipe.spec_hash,
        "steps": [{"label": s.label, "command": s.command, "cwd": s.cwd} for s in recipe.steps],
    }
    return render.recipe_runs_page(recipe_dict, runs_table)


def run_detail_page(db: RecipesDB, run_id: int) -> str | None:
    """One run: header + per-step table + output tails. None if unknown."""
    assert db.conn is not None, "run_detail_page on closed db"
    try:
        run = store.get_run(db.conn, run_id)
    except RecipesError:
        return None
    recipe_row = db.conn.execute("SELECT key FROM recipes WHERE id = ?", (run.recipe_id,)).fetchone()
    recipe_key = recipe_row["key"] if recipe_row else "?"
    step_rows = [
        dict(r)
        for r in db.conn.execute(
            "SELECT ordinal, label, command, status, exit_code, duration_ms, tail "
            "FROM run_steps WHERE run_id = ? ORDER BY ordinal LIMIT ?",
            (run_id, MAX_RUN_STEPS),
        ).fetchall()
    ]
    run_dict = {
        "run_id": run.id,
        "status": run.status,
        "exit_code": run.exit_code,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "raw_path": run.raw_path,
    }
    return render.recipe_run_page(run_dict, step_rows, recipe_key)
