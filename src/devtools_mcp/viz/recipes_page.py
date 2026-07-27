"""The /recipes console page, registered through the viz page registry.

This is the REFERENCE implementation of an out-of-tree-style console page: the
recipes tab is no longer hard-wired into viz/server.py's dispatch, it is a
:class:`~devtools_mcp.viz.pages.VizPage` registered on import, proving a plugin
can contribute a tab + GET/POST routes the same way. The rendering itself still
lives in ``viz/recipes_data.py`` / ``viz/render.py`` (unchanged); this module is
just the routing glue (GET subpaths, the POST that starts a run) that used to sit
inline in the server.

Handlers open their own recipes DB (the server no longer threads one in) and
return HTML strings or :class:`VizResponse` objects — the registry contract.
"""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import parse_qs, unquote

from devtools_mcp.recipes import RecipesError, open_recipes
from devtools_mcp.viz import recipes_data, render
from devtools_mcp.viz.pages import VizPage, VizResponse, register_page


def _with_recipes[T](fn: Callable[..., T]) -> T:
    """Open the recipes DB, run `fn(db)`, always close (mirrors server helper)."""
    db = open_recipes()
    try:
        return fn(db)
    finally:
        db.close()


def _not_found() -> VizResponse:
    return VizResponse(body=render.page("not found", "<p>404</p>"), status=404)


def render_index() -> str:
    """GET /recipes — the recipe catalog."""
    return _with_recipes(recipes_data.recipes_overview)


def handle_get(rest: list[str], query: dict[str, list[str]]) -> VizResponse | None:
    """GET /recipes/<rest...> — run detail (/recipes/run/<id>) or one recipe."""
    if rest[:1] == ["run"] and len(rest) == 2:
        try:
            run_id = int(unquote(rest[1]))
        except ValueError:
            return _not_found()
        body = _with_recipes(lambda db: recipes_data.run_detail_page(db, run_id))
        return VizResponse(body=body) if body else _not_found()
    if len(rest) == 1:
        key = unquote(rest[0])
        body = _with_recipes(lambda db: recipes_data.recipe_runs_page(db, key))
        return VizResponse(body=body) if body else _not_found()
    return _not_found()


def handle_post(rest: list[str], body: str) -> VizResponse | None:
    """POST /recipes/<key>/run — start a background run, then 303 to its detail.

    A bad/missing recipe key (or any recipe-domain error) returns a clean 400
    page, never a 500. The run itself executes on the DBOS durable workflow; if
    that cannot launch, the runner records the run as failed rather than crashing
    the handler.
    """
    if len(rest) == 2 and rest[1] == "run":
        from devtools_mcp.recipes import runner

        key = unquote(rest[0])
        fields = parse_qs(body)
        force = (fields.get("force") or ["0"])[0] in ("1", "true", "on")
        try:
            run_id, _cached = runner.start_background_run(key, force=force)
        except RecipesError as exc:
            return VizResponse(body=render.page("recipe error", f"<p>⛔ {render._h(str(exc))}</p>"), status=400)
        return VizResponse.redirect(f"/recipes/run/{run_id}")
    return _not_found()


RECIPES_PAGE = VizPage(
    name="recipes",
    prefix="recipes",
    label="Recipes",
    render=render_index,
    get=handle_get,
    post=handle_post,
)

register_page(RECIPES_PAGE)
