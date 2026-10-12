"""General recipe/pipeline engine for devtools-mcp.

A *recipe* is an ordered list of steps (shell commands) run one after another
with stop-on-failure semantics; every run and every step outcome is cached in
SQLite. The domain is deliberately generic, build, test, setup, deploy, or any
other sequence of commands.

Public API (how out-of-tree plugins seed recipes)::

    from devtools_mcp.recipes import register_recipe, open_recipes

    db = open_recipes()
    register_recipe(db, {"key": "ci", "kind": "test", "steps": [
        {"label": "lint", "command": "ruff check src/"},
        {"label": "test", "command": "pytest -q"},
    ]})

MCP-facing tools live in devtools_mcp.tools.recipe_tools; this package is the
domain layer.
"""

from devtools_mcp.recipes.db import RecipesDB, RecipesError, open_recipes
from devtools_mcp.recipes.runner import run_recipe
from devtools_mcp.recipes.store import register_recipe

__all__ = ["RecipesDB", "RecipesError", "open_recipes", "register_recipe", "run_recipe"]
