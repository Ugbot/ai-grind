"""Model-only tests for bounded process inputs and outcome acceptance."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from devtools_mcp.process.models import ProcessOutcome, ProcessSpec, SupervisionLimits


def test_models_bound_inputs(tmp_path: Path):
    for args in ({"argv": []}, {"argv": [1]}, {"argv": ["a\0b"]}, {"argv": ["x"], "env": {"K": 1}}):
        with pytest.raises(ValidationError):
            ProcessSpec(cwd=tmp_path, **args)
    for args in ({"timeout_seconds": float("inf")}, {"max_samples": 0}, {"max_log_bytes": True}):
        with pytest.raises(ValidationError):
            SupervisionLimits(**args)
    spec = ProcessSpec(argv=[sys.executable, "-c", "pass"], cwd=tmp_path, env={"SECRET": "hidden"})
    assert "hidden" not in spec.model_dump_json()
    assert spec.argv[0] == sys.executable


def test_unknown_cannot_pass():
    result = ProcessOutcome(run_id="id", binary="python", reason="exited", child_reaped=True, group_state="empty")
    assert result.exit_code is None
    assert not result.success
    result.exit_code = 0
    assert result.success
    result.group_state = "unknown"
    assert not result.success
