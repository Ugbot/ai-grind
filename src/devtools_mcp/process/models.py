"""Models for bounded process-group inputs and outcome evidence."""

from __future__ import annotations

import math
import os
import uuid
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from devtools_mcp.evidence import FileBinding
from devtools_mcp.models import RunBase


class ProcessSpec(BaseModel):
    """One exact launch. Environment override values are never evidence fields."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    argv: tuple[str, ...]
    cwd: Path
    env: dict[str, str] = Field(default_factory=dict, exclude=True)
    fixture_path: Path | None = None
    free_space_path: Path | None = None
    bindings: tuple[FileBinding, ...] = ()
    task_key: str = ""
    parent_run_id: str = ""
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    capability: Literal["posix_process_group"] = "posix_process_group"

    @field_validator("argv", mode="before")
    @classmethod
    def bounded_argv(cls, value: object) -> object:
        if not isinstance(value, (list, tuple)) or not 1 <= len(value) <= 256:
            raise ValueError("argv must contain 1..256 strings")
        if any(not isinstance(s, str) or not s or "\0" in s or len(s.encode()) > 16_384 for s in value):
            raise ValueError("argv entries must be nonempty bounded strings without NUL")
        if sum(len(s.encode()) for s in value) > 128 << 10:
            raise ValueError("argv exceeds 128 KiB")
        return value

    @field_validator("env", mode="before")
    @classmethod
    def bounded_env(cls, value: object) -> object:
        if not isinstance(value, dict) or len(value) > 128:
            raise ValueError("env must be a map with at most 128 overrides")
        size = 0
        for key, item in value.items():
            if not isinstance(key, str) or not isinstance(item, str):
                raise ValueError("environment keys and values must be strings")
            if not key or "=" in key or "\0" in key or "\0" in item or len(key) > 256 or len(item.encode()) > 16_384:
                raise ValueError("invalid bounded environment override")
            size += len(key.encode()) + len(item.encode())
        if size > 128 << 10:
            raise ValueError("environment overrides exceed 128 KiB")
        return dict(value)

    @field_validator("cwd", "fixture_path", "free_space_path")
    @classmethod
    def absolute_path(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or len(os.fsencode(value)) > 4096 or "\0" in str(value)):
            raise ValueError("paths must be absolute, bounded, and without NUL")
        return value

    @field_validator("run_id")
    @classmethod
    def canonical_uuid(cls, value: str) -> str:
        if str(uuid.UUID(value)) != value:
            raise ValueError("run_id must be a canonical UUID")
        return value

    @model_validator(mode="after")
    def bounded_identity(self) -> ProcessSpec:
        if len(self.bindings) > 64 or len(self.task_key) > 256 or len(self.parent_run_id) > 256:
            raise ValueError("too many bindings or oversized identity")
        assert len(self.argv) > 0
        assert len(self.env) <= 128
        return self


class SupervisionLimits(BaseModel):
    """Sampling thresholds are monitoring, not kernel-enforced resource quotas."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    timeout_seconds: float = 60.0
    sample_interval_seconds: float = 0.2
    max_samples: int = 1000
    observation_timeout_seconds: float = 2.0
    term_grace_seconds: float = 1.0
    kill_grace_seconds: float = 1.0
    reap_timeout_seconds: float = 1.0
    drain_timeout_seconds: float = 1.0
    max_log_bytes: int = 8 << 20
    tail_bytes: int = 8192
    max_rss_bytes: int | None = None
    max_fixture_bytes: int | None = None
    max_fixture_entries: int | None = None
    min_free_bytes: int | None = None
    inventory_max_entries: int = 10_000
    inventory_max_depth: int = 32
    inventory_max_bytes: int = 1 << 40
    max_observation_bytes: int = 4 << 20
    max_binding_bytes: int = 64 << 20

    @model_validator(mode="after")
    def finite_bounds(self) -> SupervisionLimits:
        for name in (
            "timeout_seconds",
            "sample_interval_seconds",
            "observation_timeout_seconds",
            "term_grace_seconds",
            "kill_grace_seconds",
            "reap_timeout_seconds",
            "drain_timeout_seconds",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or not 0 < value <= 86400:
                raise ValueError(f"{name} must be positive, finite, and at most one day")
        bounds = {
            "max_samples": 1_000_000,
            "max_log_bytes": 1 << 40,
            "tail_bytes": 1 << 20,
            "inventory_max_entries": 1_000_000,
            "inventory_max_bytes": 1 << 40,
            "max_observation_bytes": 32 << 20,
            "max_binding_bytes": 1 << 40,
        }
        for name, maximum in bounds.items():
            if not 0 < getattr(self, name) <= maximum:
                raise ValueError(f"invalid {name}")
        for name in ("max_rss_bytes", "max_fixture_bytes", "max_fixture_entries", "min_free_bytes"):
            value = getattr(self, name)
            if value is not None and not 0 <= value <= 1 << 50:
                raise ValueError(f"invalid {name}")
        if not 0 <= self.inventory_max_depth <= 128:
            raise ValueError("invalid inventory_max_depth")
        assert self.max_samples > 0
        assert self.max_log_bytes > 0
        return self


class SignalAttempt(BaseModel):
    target: Literal["group", "child"]
    signal: str
    sent: bool = False
    error: str = ""


class ProcessOutcome(RunBase):
    """Strict acceptance and references, including interrupted/unknown attempts."""

    suite: str = "process"
    tool: str = "supervise"
    exit_code: int | None = None  # type: ignore[assignment]
    schema_version: int = 1
    capability: str = "posix_process_group"
    reason: str = "prepared"
    pid: int | None = None
    pgid: int | None = None
    child_reaped: bool = False
    group_state: Literal["empty", "present", "unknown"] = "unknown"
    threshold_breach: str = ""
    observation_errors: list[str] = Field(default_factory=list)
    cleanup_errors: list[str] = Field(default_factory=list)
    publication_errors: list[str] = Field(default_factory=list)
    output_errors: list[str] = Field(default_factory=list)
    signal_attempts: list[SignalAttempt] = Field(default_factory=list)
    sample_count: int = 0
    peak_rss_bytes: int = 0
    peak_fixture_bytes: int = 0
    peak_fixture_entries: int = 0
    minimum_free_bytes: int | None = None
    log_bytes: int = 0
    output_bytes_seen: int = 0
    tail: str = ""
    log_path: str = ""
    manifest_path: str = ""
    launch_path: str = ""
    progress_path: str = ""
    result_path: str = ""
    limits: SupervisionLimits = Field(default_factory=SupervisionLimits)

    @property
    def success(self) -> bool:
        return (
            self.reason == "exited"
            and self.exit_code == 0
            and self.child_reaped
            and self.group_state == "empty"
            and not self.threshold_breach
            and not self.observation_errors
            and not self.cleanup_errors
            and not self.publication_errors
            and not self.output_errors
        )
