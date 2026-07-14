"""The `plan` MCP tool: ask the configured planner to sequence skills toward a
goal. Routes through the pluggable seam (native → platform → local), so it works
offline via the local GOAP backend and upgrades to a premium backend when set.

Planning is strictly optional: with DEVTOOLS_MCP_PLANNER=none it reports that no
planner is available and the agent should dispatch from the router index instead.
"""

from __future__ import annotations

import json

from mcp.server.fastmcp import Context

from devtools_mcp.planning import PlannerError, resolve
from devtools_mcp.server import mcp


def _as_dict(raw: str | None, field: str) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PlannerError(f"{field} must be JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise PlannerError(f"{field} must be a JSON object")
    return data


@mcp.tool()
async def plan(
    ctx: Context,
    goal: str,
    world: str | None = None,
    mode: str = "high",
    layered: bool = False,
) -> str:
    """Plan a sequence of skills that reaches a goal state, using skills' GOAP
    descriptors (their ```goap blocks). Goal/world are JSON objects of state
    facts, e.g. goal='{"tests_passing": true}', world='{"spec_written": false}'.

    Returns the ordered skill names to run. `layered=true` asks a premium backend
    for parallel Kahn waves (the free local backend returns the flat order).
    Backend is chosen by DEVTOOLS_MCP_PLANNER (none|local|platform|<url>); with
    'none' there is no planner and you should dispatch from the router index.
    """
    try:
        goal_state = _as_dict(goal, "goal")
        if not goal_state:
            return "plan needs a non-empty goal JSON object, e.g. '{\"tests_passing\": true}'"
        world_state = _as_dict(world, "world")
        planner = resolve()
        if planner is None:
            return "No planner available (DEVTOOLS_MCP_PLANNER=none). Dispatch a skill from the router index."
        result = planner.plan(goal_state, world_state, mode, layered)
    except PlannerError as exc:
        return f"Error: {exc}"
    if not result.ok:
        return f"No plan ({result.backend}): {result.message}"
    header = f"**Plan** ({result.backend}, mode={mode}):"
    if result.layers:
        waves = "\n".join(f"  wave {i + 1}: {', '.join(w)}" for i, w in enumerate(result.layers))
        body = f"{header}\n{waves}"
    else:
        body = f"{header} " + " → ".join(result.steps) if result.steps else f"{header} (already satisfied — empty plan)"
    return body + (f"\n_{result.message}_" if result.message else "")
