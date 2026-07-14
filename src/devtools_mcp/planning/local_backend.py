"""In-process GOAP planner over the local skill descriptors — the free/offline
'internal' backend. Builds Action subclasses from each skill's ```goap block,
scales cost by the active power mode, and runs the vendored regressive planner.
"""

from __future__ import annotations

from devtools_mcp.goap import Action, PathNotFoundException, RegressivePlanner
from devtools_mcp.planning.planner import PLAN_STEPS_MAX, PlanResult

_UNSET = object()  # sentinel for goal keys absent from the world (never equal to a goal value)
_LOW_COST_FACTOR = 0.5  # low mode favours cheaper/faster plans


def _build_actions(descriptors: dict[str, dict], mode: str) -> list[Action]:
    """One Action subclass per skill descriptor, cost scaled by power mode."""
    actions: list[Action] = []
    factor = _LOW_COST_FACTOR if mode == "low" else 1.0
    for name, goap in descriptors.items():
        preconditions = dict(goap.get("preconditions") or {})
        effects = dict(goap.get("effects") or {})
        if not effects:
            continue  # an action with no effect can never advance a goal
        base_cost = float(goap.get("cost", 1.0))
        cls = type(
            f"Skill_{name}",
            (Action,),
            {
                "preconditions": preconditions,
                "effects": effects,
                "cost": max(0.0, base_cost) * factor,
                "skill_name": name,
            },
        )
        actions.append(cls())
    return actions


def _seed_world(world: dict, goal: dict) -> dict:
    """Ensure every goal key exists in the world (the vendored search indexes goal
    keys directly), defaulting missing ones to an unsatisfied sentinel."""
    seeded = dict(world)
    for key in goal:
        seeded.setdefault(key, _UNSET)
    return seeded


class LocalPlanner:
    """Resolve skill descriptors from the local library + live store, then plan."""

    def plan(self, goal: dict, world: dict, mode: str, layered: bool) -> PlanResult:
        assert isinstance(goal, dict) and goal, "goal must be a non-empty dict"
        assert isinstance(world, dict), "world must be a dict"
        descriptors = self._descriptors()
        if not descriptors:
            return PlanResult(False, "local", message="no skills declare a ```goap block to plan over")
        actions = _build_actions(descriptors, mode)
        try:
            steps = RegressivePlanner(_seed_world(world, goal), actions).find_plan(goal)
        except (PathNotFoundException, KeyError):
            return PlanResult(False, "local", message="no skill path to the goal from the given world")
        names = [getattr(step.action, "skill_name", type(step.action).__name__) for step in steps]
        assert len(names) <= PLAN_STEPS_MAX, f"plan exceeded {PLAN_STEPS_MAX} steps"
        message = "Kahn layering is a premium feature — returning the flat order" if layered else ""
        return PlanResult(True, "local", steps=names, layers=None, message=message)

    @staticmethod
    def _descriptors() -> dict[str, dict]:
        from devtools_mcp.skilldocs import router
        from devtools_mcp.skilldocs.store import SkillDocStore

        store = SkillDocStore()
        try:
            return {entry.name: entry.goap for entry in router.collect_skills(store) if entry.goap}
        finally:
            store.close()
