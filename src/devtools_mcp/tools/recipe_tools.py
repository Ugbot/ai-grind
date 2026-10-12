"""Recipe tools: a general recipe/pipeline engine backed by SQLite.

A recipe is an ordered list of steps (shell commands) run one after another with
stop-on-failure semantics; runs and step outcomes are cached, and a passed run
is reused while the recipe's spec is unchanged. Domain-agnostic: build, test,
setup, deploy, any sequence.

Action-multiplexed like tracker_*(): one `recipe` tool, an `action` parameter
per verb. Every response is bounded markdown, full tables live in the DB and
are paged via the runs/steps actions.
"""

from __future__ import annotations

import json

from mcp.server.fastmcp import Context

from devtools_mcp.formatters import format_dataframe
from devtools_mcp.recipes import frames, runner, store
from devtools_mcp.recipes.db import RecipesDB, RecipesError
from devtools_mcp.recipes.models import RunResult
from devtools_mcp.server import get_app_ctx, mcp

MAX_SEED_RECIPES: int = 200
STEP_LINE_MAX: int = 100


def _recipes(ctx: Context) -> RecipesDB:
    """The lazily-opened recipes database from the app context."""
    db = get_app_ctx(ctx).get_recipes()
    assert db is not None and db.conn is not None, "recipes db unavailable"
    return db


def _parse_json(raw: str | None, what: str) -> object | None:
    """Parse a JSON string arg, raising RecipesError on malformed input."""
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RecipesError(f"{what} is not valid JSON: {exc}") from exc


def _build_spec(
    spec: str | None,
    key: str | None,
    name: str | None,
    kind: str | None,
    summary: str | None,
    steps: str | None,
    env_axes: str | None,
    source: str | None,
) -> dict:
    """Assemble a recipe spec dict from a full JSON blob or individual args."""
    if spec is not None:
        parsed = _parse_json(spec, "spec")
        if not isinstance(parsed, dict):
            raise RecipesError("spec must be a JSON object")
        return parsed
    out: dict = {}
    if key:
        out["key"] = key
    if name:
        out["name"] = name
    if kind:
        out["kind"] = kind
    if summary:
        out["summary"] = summary
    if source:
        out["source"] = source
    steps_val = _parse_json(steps, "steps")
    if steps_val is not None:
        out["steps"] = steps_val
    env_val = _parse_json(env_axes, "env_axes")
    if env_val is not None:
        out["env_axes"] = env_val
    return out


def _recipe_line(recipe) -> str:
    return f"`{recipe.key}` ({recipe.kind}, {len(recipe.steps)} steps) {recipe.name}"


def _recipe_detail(db: RecipesDB, key: str) -> str:
    recipe = store.get_recipe(db.conn, key)
    parts = [f"**{recipe.key}**: {recipe.name}", ""]
    parts.append(f"kind: {recipe.kind} | steps: {len(recipe.steps)} | spec_hash: `{recipe.spec_hash[:12]}`")
    if recipe.summary:
        parts += ["", recipe.summary]
    if recipe.env_axes:
        parts.append("env: " + ", ".join(f"`{k}={v}`" for k, v in list(recipe.env_axes.items())[:20]))
    if recipe.steps:
        parts += ["", f"**Steps ({len(recipe.steps)}):**"]
        for i, step in enumerate(recipe.steps[:STEP_LINE_MAX]):
            cwd = f" (cwd {step.cwd})" if step.cwd else ""
            parts.append(f"{i + 1}. **{step.label}**{cwd}: `{step.command}`")
    cached = store.cached_run(db, recipe.key)
    parts += ["", f"cache: {'run #' + str(cached.id) + ' passed' if cached else 'cold (will run on next `run`)'}"]
    parts += [f"created {recipe.created_at} | updated {recipe.updated_at}"]
    return "\n".join(parts)


def _run_summary(result: RunResult) -> str:
    """Bounded markdown summary of a RunResult."""
    if result.dry_run:
        head = f"**Dry run, `{result.recipe_key}`** ({len(result.steps)} steps, nothing executed)"
    elif result.cached:
        head = f"**`{result.recipe_key}`, cached** (run #{result.run_id}, status **{result.status}**, not re-run)"
    else:
        head = f"**`{result.recipe_key}`, {result.status}** (run #{result.run_id}, exit {result.exit_code})"
    marks = {"passed": "[x]", "failed": "[!]", "skipped": "[-]", "pending": "[ ]", "running": "[>]"}
    lines = [head, ""]
    for step in result.steps[:STEP_LINE_MAX]:
        mark = marks.get(step.status, "[ ]")
        dur = f" ({step.duration_ms}ms)" if step.duration_ms is not None else ""
        exit_note = "" if step.exit_code in (None, 0) else f" exit {step.exit_code}"
        lines.append(f"{mark} {step.ordinal + 1}. {step.label}{dur}{exit_note}")
        if step.status == "failed" and step.tail:
            tail_preview = "\n".join(step.tail.splitlines()[-8:])
            lines.append(f"```\n{tail_preview}\n```")
    if len(result.steps) > STEP_LINE_MAX:
        lines.append(f"... {len(result.steps) - STEP_LINE_MAX} more steps")
    if result.raw_path:
        lines.append(f"\nfull output: `{result.raw_path}`")
    return "\n".join(lines)


@mcp.tool()
async def recipe(
    ctx: Context,
    action: str,
    key: str | None = None,
    name: str | None = None,
    kind: str | None = None,
    summary: str | None = None,
    steps: str | None = None,
    env_axes: str | None = None,
    source: str | None = None,
    spec: str | None = None,
    specs: str | None = None,
    env: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    run_id: int | None = None,
    limit: int = 50,
) -> str:
    """General recipe/pipeline engine: ordered shell steps, stop-on-fail, cached.

    A recipe is an ordered list of steps run one after another; the first
    non-zero step fails the run and the rest are skipped. A passed run is cached
    and reused while the recipe's spec is unchanged (pass force=True to re-run).
    Domain-agnostic, build, test, setup, deploy, any sequence.

    Actions:
        register, upsert one recipe. Either spec (a full JSON object with
                   key/name/kind/summary/env_axes/steps) OR individual args:
                   key + steps (a JSON array of {label, command, cwd?, timeout?}),
                   plus optional name/kind/summary/env_axes (JSON object).
        list: all recipes (optional kind filter)
        get: key: recipe detail (steps, env, spec_hash, cache state)
        run: key: run the recipe. env (JSON object) adds per-run env vars;
                   dry_run reports the plan without executing; force ignores the
                   cache. Returns a bounded per-step summary + run id.
        runs: recipe run history (optional key filter)
        steps: run_id: the per-step outcomes of one run
        seed: specs: a JSON array of recipe specs (e.g. `ct seed` output);
                   registers them all.
    """
    db = _recipes(ctx)
    try:
        if action == "register":
            built = _build_spec(spec, key, name, kind, summary, steps, env_axes, source)
            recipe_row = store.register_recipe(db, built)
            return f"Registered {_recipe_line(recipe_row)}, spec_hash `{recipe_row.spec_hash[:12]}`"
        if action == "list":
            recipes = store.list_recipes(db.conn, kind)
            if not recipes:
                return "No recipes yet. Register one with action='register'."
            lines = [f"- {_recipe_line(r)}" for r in recipes[:200]]
            return f"**Recipes ({len(recipes)}):**\n" + "\n".join(lines)
        if action == "get":
            if not key:
                return "get needs key"
            return _recipe_detail(db, key)
        if action == "run":
            if not key:
                return "run needs key"
            env_extra = _parse_json(env, "env")
            if env_extra is not None and not isinstance(env_extra, dict):
                return "env must be a JSON object of NAME->value"
            extra = {str(k): str(v) for k, v in env_extra.items()} if isinstance(env_extra, dict) else None
            result = await runner.run_recipe(db, key, env_extra=extra, dry_run=dry_run, force=force)
            return _run_summary(result)
        if action == "runs":
            df = frames.runs_frame(db.conn, key)
            df = df.head(max(1, min(limit, 200)))
            return format_dataframe(df, title="Recipe runs", max_rows=limit)
        if action == "steps":
            if not run_id:
                return "steps needs run_id"
            df = frames.steps_frame(db.conn, run_id)
            return format_dataframe(df, title=f"Run #{run_id} steps", max_rows=max(1, min(limit, 200)))
        if action == "seed":
            if not specs:
                return "seed needs specs (a JSON array of recipe specs)"
            parsed = _parse_json(specs, "specs")
            if not isinstance(parsed, list):
                return "specs must be a JSON array of recipe objects"
            if len(parsed) > MAX_SEED_RECIPES:
                return f"too many recipes: {len(parsed)} > {MAX_SEED_RECIPES}"
            registered: list[str] = []
            for item in parsed[:MAX_SEED_RECIPES]:  # bounded
                if not isinstance(item, dict):
                    return "each seed entry must be a JSON object"
                registered.append(store.register_recipe(db, item).key)
            return f"Seeded {len(registered)} recipe(s): " + ", ".join(f"`{k}`" for k in registered)
        return f"Unknown action {action!r}. One of: register, list, get, run, runs, steps, seed"
    except RecipesError as exc:
        return f"Error: {exc}"
