"""Vendored GOAP (Goal-Oriented Action Planning) core.

A regressive-A* planner: given a world state, a set of Actions (each with
preconditions + effects + cost), and a goal state, it returns an ordered list of
actions that reach the goal. Pure stdlib, no I/O.

Vendored from `aibywire/work_runner/goap` (upstream: agentix/GOAP, MIT). Kept
faithful to upstream; the only changes are a relative import fix and dead-code
cleanup. See skills/THIRD_PARTY_SKILLS.md for attribution.
"""

from __future__ import annotations

from devtools_mcp.goap.action import Action, EffectReference, reference
from devtools_mcp.goap.algo.astar import PathNotFoundException
from devtools_mcp.goap.planner import PlanStep, RegressivePlanner

__all__ = [
    "Action",
    "EffectReference",
    "PathNotFoundException",
    "PlanStep",
    "RegressivePlanner",
    "reference",
]
