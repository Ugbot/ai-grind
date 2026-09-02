"""Planner protocol, result type, and backend resolution."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

PLAN_STEPS_MAX: int = 64  # hard bound on a returned plan


class PlannerError(Exception):
    """Expected/reportable planning failure, not a bug."""


@dataclass
class PlanResult:
    """Outcome of a plan request. `steps` is the ordered skill names; `layers` is
    the Kahn-wave grouping when a premium backend produced one, else None."""

    ok: bool
    backend: str
    steps: list[str] = field(default_factory=list)
    layers: list[list[str]] | None = None
    message: str = ""

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "backend": self.backend,
            "steps": self.steps,
            "layers": self.layers,
            "message": self.message,
        }


@runtime_checkable
class Planner(Protocol):
    def plan(self, goal: dict, world: dict, mode: str, layered: bool) -> PlanResult: ...


def _load_native() -> Planner | None:
    """The closed-source C++ accelerator, if its licensed wheel is installed.

    Optional import: absent by default, so the free path never depends on it.
    """
    try:
        from devtools_mcp_native import planner as native  # type: ignore
    except ImportError:
        return None
    return native.NativePlanner()  # pragma: no cover - wheel not present in OSS


def resolve() -> Planner | None:
    """Pick a planner backend. `none` disables planning entirely (returns None).

    Order for the default/unset case: native (closed wheel) → local (always).
    Platform selection is explicit (`DEVTOOLS_MCP_PLANNER=platform`) or a URL.
    """
    choice = os.environ.get("DEVTOOLS_MCP_PLANNER", "").strip().lower()
    if choice == "none":
        return None
    if choice in ("", "local"):
        return _load_native() or _local()
    if choice == "platform":
        from devtools_mcp.planning.remote_backend import RemotePlanner

        return RemotePlanner("platform")
    if choice.startswith(("http://", "https://")):
        from devtools_mcp.planning.remote_backend import RemotePlanner

        return RemotePlanner(choice)
    raise PlannerError(f"Unknown DEVTOOLS_MCP_PLANNER={choice!r}: none|local|platform|<url>")


def _local() -> Planner:
    from devtools_mcp.planning.local_backend import LocalPlanner

    return LocalPlanner()
