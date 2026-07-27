"""Smoke tests for the recipes console pages (render + data assembly)."""

from __future__ import annotations

from pathlib import Path

import pytest

from devtools_mcp.recipes import store
from devtools_mcp.recipes.db import RecipesDB, open_recipes
from devtools_mcp.recipes.runner import run_recipe
from devtools_mcp.viz import recipes_data


@pytest.fixture
def db(tmp_path: Path) -> RecipesDB:
    recipes = open_recipes(tmp_path / "recipes.db")
    yield recipes
    recipes.close()


def test_overview_empty(db):
    html = recipes_data.recipes_overview(db)
    assert "Recipes" in html and "No recipes yet" in html


async def test_pages_render_after_a_run(db):
    store.register_recipe(
        db,
        {
            "key": "ci",
            "name": "CI pipeline",
            "kind": "test",
            "summary": "lint then test",
            "steps": [{"label": "lint", "command": "true"}, {"label": "test", "command": "true"}],
        },
    )
    result = await run_recipe(db, "ci")

    overview = recipes_data.recipes_overview(db)
    assert "ci" in overview and "/recipes/ci/run" in overview

    runs_page = recipes_data.recipe_runs_page(db, "ci")
    assert runs_page is not None and "CI pipeline" in runs_page
    assert recipes_data.recipe_runs_page(db, "missing") is None

    run_page = recipes_data.run_detail_page(db, result.run_id)
    assert run_page is not None and "lint" in run_page and "test" in run_page
    assert recipes_data.run_detail_page(db, 999999) is None
