"""Dataclasses for recipe rows and run results.

`Step`, `Recipe`, and `Run` mirror the three tables; `RunStep` and `RunResult`
are the in-memory shapes the runner returns. Validation constants (kinds,
statuses) live here so the domain layer and the MCP tools share one vocabulary.

Recipes use plain dataclasses (the spec's choice) rather than the tracker's
pydantic models, the domain is small and the JSON spec is validated in store.py.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

RUN_STATUSES: tuple[str, ...] = ("pending", "running", "passed", "failed", "cancelled")
STEP_STATUSES: tuple[str, ...] = ("pending", "running", "passed", "failed", "skipped")
TERMINAL_RUN_STATUSES: tuple[str, ...] = ("passed", "failed", "cancelled")
DEFAULT_KIND: str = "task"
DEFAULT_STEP_TIMEOUT: int = 600  # seconds per step; a step may override
MAX_STEPS: int = 200  # a single recipe cannot exceed this many steps


@dataclass
class Step:
    """One command in a recipe: a shell command run in `cwd` with `timeout` secs."""

    label: str
    command: str
    cwd: str | None = None
    timeout: int = DEFAULT_STEP_TIMEOUT

    def to_dict(self) -> dict:
        out: dict = {"label": self.label, "command": self.command, "timeout": self.timeout}
        if self.cwd is not None:
            out["cwd"] = self.cwd
        return out

    @classmethod
    def from_dict(cls, data: dict, ordinal: int) -> Step:
        assert isinstance(data, dict), f"step {ordinal} must be an object, got {type(data)}"
        command = str(data.get("command") or "").strip()
        assert command, f"step {ordinal} has no command"
        label = str(data.get("label") or "").strip() or f"step {ordinal + 1}"
        cwd_raw = data.get("cwd")
        cwd = str(cwd_raw) if cwd_raw else None
        timeout = int(data.get("timeout") or DEFAULT_STEP_TIMEOUT)
        assert timeout > 0, f"step {ordinal} timeout must be positive, got {timeout}"
        return cls(label=label, command=command, cwd=cwd, timeout=timeout)


@dataclass
class Recipe:
    """A registered pipeline: an ordered list of steps addressed by `key`."""

    id: int
    key: str
    name: str
    kind: str
    summary: str
    env_axes: dict[str, str]
    steps: list[Step]
    spec_hash: str
    source: str | None
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Recipe:
        assert row is not None, "Recipe.from_row given None"
        env_axes = json.loads(row["env_axes"] or "{}")
        raw_steps = json.loads(row["steps"] or "[]")
        assert isinstance(env_axes, dict), "env_axes must decode to an object"
        assert isinstance(raw_steps, list), "steps must decode to a list"
        steps = [Step.from_dict(s, i) for i, s in enumerate(raw_steps[:MAX_STEPS])]  # bounded
        return cls(
            id=row["id"],
            key=row["key"],
            name=row["name"],
            kind=row["kind"],
            summary=row["summary"],
            env_axes={str(k): str(v) for k, v in env_axes.items()},
            steps=steps,
            spec_hash=row["spec_hash"],
            source=row["source"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@dataclass
class Run:
    """One execution of a recipe."""

    id: int
    recipe_id: int
    spec_hash: str
    status: str
    exit_code: int | None
    raw_path: str | None
    started_at: str
    finished_at: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Run:
        assert row is not None, "Run.from_row given None"
        assert row["status"] in RUN_STATUSES, f"bad run status in db: {row['status']}"
        return cls(
            id=row["id"],
            recipe_id=row["recipe_id"],
            spec_hash=row["spec_hash"],
            status=row["status"],
            exit_code=row["exit_code"],
            raw_path=row["raw_path"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )


@dataclass
class RunStep:
    """The recorded outcome of one step within a run."""

    ordinal: int
    label: str
    command: str
    status: str
    exit_code: int | None = None
    duration_ms: int | None = None
    tail: str = ""


@dataclass
class RunResult:
    """What `run_recipe` returns: the run row id + per-step outcomes + flags."""

    run_id: int
    recipe_key: str
    status: str
    exit_code: int | None
    cached: bool = False
    dry_run: bool = False
    raw_path: str | None = None
    steps: list[RunStep] = field(default_factory=list)
