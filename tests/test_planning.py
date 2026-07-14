"""Pluggable planner seam + vendored GOAP core (local backend)."""

from __future__ import annotations

import json

import pytest

from devtools_mcp.goap import Action, PathNotFoundException, RegressivePlanner
from devtools_mcp.planning import PlannerError, resolve
from devtools_mcp.planning.local_backend import LocalPlanner
from devtools_mcp.skilldocs.store import SkillDocStore

# -- vendored core ------------------------------------------------------------


def _chain_actions():
    class Spec(Action):
        effects = {"spec_written": True}

    class Tdd(Action):
        preconditions = {"spec_written": True}
        effects = {"tests_passing": True}

    class Verify(Action):
        preconditions = {"tests_passing": True}
        effects = {"verified": True}

    return [Spec(), Tdd(), Verify()]


def test_core_plans_in_dependency_order():
    world = {"spec_written": False, "tests_passing": False, "verified": False}
    steps = RegressivePlanner(world, _chain_actions()).find_plan({"verified": True})
    assert [type(s.action).__name__ for s in steps] == ["Spec", "Tdd", "Verify"]


def test_core_unreachable_raises():
    # every key is produced by some action, but the precondition chain can't be
    # satisfied (B only ever sets y=False) -> the search exhausts -> PathNotFound.
    class A(Action):
        preconditions = {"y": True}
        effects = {"x": True}

    class B(Action):
        effects = {"y": False}

    with pytest.raises(PathNotFoundException):
        RegressivePlanner({"x": False}, [A(), B()]).find_plan({"x": True})


# -- backend resolution -------------------------------------------------------


def test_resolve_none_disables(monkeypatch):
    monkeypatch.setenv("DEVTOOLS_MCP_PLANNER", "none")
    assert resolve() is None


def test_resolve_local_default(monkeypatch):
    monkeypatch.delenv("DEVTOOLS_MCP_PLANNER", raising=False)
    assert isinstance(resolve(), LocalPlanner)


def test_resolve_bad_value(monkeypatch):
    monkeypatch.setenv("DEVTOOLS_MCP_PLANNER", "bogus")
    with pytest.raises(PlannerError):
        resolve()


# -- local backend over live-skill descriptors --------------------------------


def _seed_goap_skills(skills_root_missing: str, monkeypatch):
    """Create three chained live skills carrying ```goap blocks."""
    monkeypatch.setenv("DEVTOOLS_MCP_SKILLS_ROOT", skills_root_missing)  # no static library
    store = SkillDocStore()
    chain = [
        ("spec-driven-development", {}, {"spec_written": True}),
        ("tdd", {"spec_written": True}, {"tests_passing": True}),
        ("verify", {"tests_passing": True}, {"verified": True}),
    ]
    for name, pre, eff in chain:
        goap = json.dumps({"preconditions": pre, "effects": eff, "cost": 1.0})
        store.create(name, f"---\nname: {name}\ndescription: d\n---\n\nbody\n```goap\n{goap}\n```\n")
    store.close()


def test_local_plan_orders_chain(tmp_path, monkeypatch):
    _seed_goap_skills(str(tmp_path / "none"), monkeypatch)
    result = LocalPlanner().plan({"verified": True}, {"spec_written": False}, "high", False)
    assert result.ok and result.steps == ["spec-driven-development", "tdd", "verify"]


def test_local_plan_unreachable(tmp_path, monkeypatch):
    _seed_goap_skills(str(tmp_path / "none"), monkeypatch)
    result = LocalPlanner().plan({"deployed": True}, {}, "high", False)
    assert not result.ok and "no skill path" in result.message


def test_local_layered_is_premium_flat(tmp_path, monkeypatch):
    _seed_goap_skills(str(tmp_path / "none"), monkeypatch)
    result = LocalPlanner().plan({"verified": True}, {}, "high", True)
    assert result.ok and result.layers is None and "premium" in result.message


def test_local_no_goap_skills(tmp_path, monkeypatch):
    monkeypatch.setenv("DEVTOOLS_MCP_SKILLS_ROOT", str(tmp_path / "none"))
    result = LocalPlanner().plan({"verified": True}, {}, "high", False)
    assert not result.ok and "goap" in result.message.lower()


def test_low_mode_cheaper_than_high(tmp_path, monkeypatch):
    from devtools_mcp.planning.local_backend import _build_actions

    descriptors = {"a": {"effects": {"x": True}, "cost": 2.0}}
    low = _build_actions(descriptors, "low")[0].cost
    high = _build_actions(descriptors, "high")[0].cost
    assert low < high
