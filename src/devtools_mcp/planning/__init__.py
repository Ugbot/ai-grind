"""Pluggable planner seam for the MCP server.

`resolve()` returns a `Planner` (or None if planning is disabled) chosen from
`DEVTOOLS_MCP_PLANNER`: `none` | `local` | `platform` | a URL. Every backend
speaks one contract — `plan(goal, world, mode, layered) -> PlanResult` — so the
internal (in-process GOAP), external (a URL), and platform planners are
interchangeable. Nothing in the router/dynamic-skills path imports this package:
the seam is additive and severable, and the system works fully without it.
"""

from __future__ import annotations

from devtools_mcp.planning.planner import PLAN_STEPS_MAX, PlannerError, PlanResult, resolve

__all__ = ["PLAN_STEPS_MAX", "PlanResult", "PlannerError", "resolve"]
