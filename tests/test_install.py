"""Tests for the install module and the wired backend install specs."""

from __future__ import annotations

import sys

import pytest

from devtools_mcp.install import format_plan, resolve_platform, run_steps, steps_for
from devtools_mcp.registry import (
    InstallSpec,
    InstallStep,
    get_backend,
    list_backends,
    load_backends,
)

_SPEC = InstallSpec(
    platforms={
        "windows": [
            InstallStep(kind="winget", argv=["winget", "install", "Tool.X"], description="X via winget"),
            InstallStep(kind="shell", argv=["setup.exe", "/quiet"], description="finish setup", elevation=True),
        ],
        "linux": [
            InstallStep(kind="apt", argv=["apt-get", "install", "-y", "x"], description="X via apt", elevation=True),
        ],
    },
    note="restart your shell afterwards",
    url="https://example.com/x",
)


class TestResolvePlatform:
    def test_current_platform_maps(self):
        platform = resolve_platform()
        expected = {"win32": "windows", "linux": "linux", "darwin": "darwin"}.get(sys.platform, "")
        assert platform == expected


class TestStepsFor:
    def test_explicit_platform(self):
        steps = steps_for(_SPEC, platform="linux")
        assert len(steps) == 1
        assert steps[0].kind == "apt"

    def test_unsupported_platform_empty(self):
        assert steps_for(_SPEC, platform="darwin") == []


class TestFormatPlan:
    def test_commands_verbatim_with_admin_marker(self):
        text = format_plan("x", steps_for(_SPEC, platform="windows"), note=_SPEC.note, url=_SPEC.url)
        assert "`winget install Tool.X`" in text
        assert "`setup.exe /quiet` [admin]" in text
        assert "[admin] steps need an elevated shell" in text
        assert "restart your shell" in text
        assert "https://example.com/x" in text

    def test_empty_steps_points_at_url(self):
        text = format_plan("x", [], url="https://example.com/x")
        assert "No install commands" in text
        assert "https://example.com/x" in text

    def test_download_step_rendered(self):
        step = InstallStep(kind="download", argv=["https://host/f.exe", "C:/tools/f.exe"], description="fetch f")
        text = format_plan("x", [step])
        assert "download https://host/f.exe -> C:/tools/f.exe" in text


class TestRunSteps:
    async def test_success_then_stop_on_failure(self):
        ok = InstallStep(kind="shell", argv=[sys.executable, "-c", "print('hello')"], description="ok step")
        bad = InstallStep(kind="shell", argv=[sys.executable, "-c", "raise SystemExit(3)"], description="bad step")
        never = InstallStep(kind="shell", argv=[sys.executable, "-c", "print('never')"], description="unreached")
        results = await run_steps([ok, bad, never], timeout=60)
        assert len(results) == 2  # stopped at the failure
        assert results[0][1] == 0
        assert "hello" in results[0][2]
        assert results[1][1] == 3

    async def test_missing_binary_reports_127(self):
        step = InstallStep(kind="shell", argv=["no-such-binary-xyz"], description="missing")
        results = await run_steps([step], timeout=10)
        assert results[0][1] == 127

    async def test_empty_steps_assert(self):
        with pytest.raises(AssertionError):
            await run_steps([], timeout=10)


class TestWiredSpecs:
    def test_every_wired_spec_is_sane(self):
        load_backends()
        wired = [(s, get_backend(s).install) for s in list_backends() if get_backend(s).install is not None]
        assert wired, "at least one backend must declare an install spec"
        suites = {s for s, _ in wired}
        assert {"renderdoc", "cdb", "etw", "py", "valgrind"} <= suites
        for suite, spec in wired:
            for platform, steps in spec.platforms.items():
                for step in steps:
                    assert step.argv, f"{suite}/{platform}: empty argv"
                    assert step.description, f"{suite}/{platform}: missing description"
                    joined = " ".join(step.argv)
                    for meta in ("&&", "||", ";", "|", ">", "<", "`", "$("):
                        assert meta not in joined, f"{suite}/{platform}: shell metacharacter {meta!r} in argv"

    def test_renderdoc_spec_shape(self):
        load_backends()
        spec = get_backend("renderdoc").install
        assert spec is not None
        windows = spec.platforms["windows"]
        assert windows[0].argv[:3] == ["winget", "install", "--id"]
        assert "BaldurKarlsson.RenderDoc" in windows[0].argv
