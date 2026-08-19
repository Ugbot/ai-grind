"""Tests for registry modularization: capability derivation, manifest loading,
failure isolation, install specs, and format_check surfacing."""

from __future__ import annotations

import pytest

from devtools_mcp.registry import (
    _BACKEND_MODULES,
    BackendSpec,
    InstalledTool,
    InstallSpec,
    InstallStep,
    ToolRegistry,
    capability_matrix,
    failed_backends,
    get_backend,
    list_backends,
    load_backends,
    register_backend,
)


def _spec(suite: str, **overrides) -> BackendSpec:
    fields = dict(
        suite=suite,
        tools=["run"],
        detect=lambda: [],
        run=lambda **kw: None,
        df_builders={"_default": lambda r: None},
        format_summary=lambda r: "",
    )
    fields.update(overrides)
    return BackendSpec(**fields)


_INSTALL = InstallSpec(
    platforms={
        "windows": [InstallStep(kind="winget", argv=["winget", "install", "X"], description="install X")],
    },
)


class TestCapabilities:
    def test_base_capabilities(self):
        caps = _spec("s1").capabilities()
        assert caps == frozenset({"detect", "run", "frames", "summary"})

    def test_stacks_adds_flamegraph(self):
        caps = _spec("s2", stacks=lambda r: []).capabilities()
        assert "flamegraph" in caps

    def test_install_adds_install(self):
        caps = _spec("s3", install=_INSTALL).capabilities()
        assert "install" in caps

    def test_format_details_adds_details(self):
        caps = _spec("s4", format_details=lambda r: "").capabilities()
        assert "details" in caps

    def test_matrix_covers_registered_suites(self):
        load_backends()
        matrix = capability_matrix()
        assert set(matrix) == set(list_backends())
        assert all("run" in caps for caps in matrix.values())


class TestRegistration:
    def test_duplicate_registration_asserts(self):
        load_backends()
        existing = list_backends()[0]
        with pytest.raises(AssertionError, match="duplicate"):
            register_backend(_spec(existing))

    def test_empty_suite_asserts(self):
        with pytest.raises(AssertionError):
            register_backend(_spec(""))

    def test_empty_tools_asserts(self):
        with pytest.raises(AssertionError):
            register_backend(_spec("s5", tools=[]))


class TestLoadBackends:
    def test_manifest_loads_all(self):
        load_backends()
        loaded = set(list_backends())
        # Every manifest module registered its suite (none failed).
        manifest_suites = {module.split(".")[1] for module in _BACKEND_MODULES}
        assert manifest_suites <= loaded
        assert not failed_backends()

    def test_idempotent(self):
        load_backends()
        before = set(list_backends())
        load_backends()
        assert set(list_backends()) == before

    def test_broken_backend_degrades(self, monkeypatch):
        import devtools_mcp.registry as reg

        monkeypatch.setattr(reg, "_BACKEND_MODULES", ("devtools_mcp.no_such.backend",))
        monkeypatch.setattr(reg, "_FAILED_BACKENDS", {})
        reg.load_backends()  # must not raise
        failed = reg.failed_backends()
        assert "devtools_mcp.no_such.backend" in failed
        assert "ModuleNotFoundError" in failed["devtools_mcp.no_such.backend"]


class TestInstallStepValidation:
    def test_bad_kind_asserts(self):
        with pytest.raises(AssertionError, match="kind"):
            InstallStep(kind="curlbash", argv=["x"], description="bad")

    def test_empty_argv_asserts(self):
        with pytest.raises(AssertionError):
            InstallStep(kind="apt", argv=[], description="bad")

    def test_spec_requires_platform(self):
        with pytest.raises(AssertionError):
            InstallSpec(platforms={})


class TestFormatCheck:
    def test_check_shows_capabilities_and_install_hint(self):
        load_backends()
        registry = ToolRegistry()
        # One available tool from a real suite, plus nothing for the rest,
        # suites with install specs should be called out in the hint line.
        suite = list_backends()[0]
        registry.tools = {f"{suite}:x": InstalledTool(suite, "x", "/bin/x", "1.0")}
        text = registry.format_check()
        assert f"**{suite}:**" in text
        assert "**Not installed:**" in text
        missing = [s for s in list_backends() if s != suite]
        installable = [s for s in missing if get_backend(s).install is not None]
        if installable:
            assert "devtools_install" in text
            assert installable[0] in text.split("devtools_install", 1)[1]

    def test_check_shows_failed_backends(self, monkeypatch):
        import devtools_mcp.registry as reg

        monkeypatch.setattr(reg, "_FAILED_BACKENDS", {"devtools_mcp.ghost.backend": "ImportError: nope"})
        registry = ToolRegistry()
        registry.tools = {"perf:stat": InstalledTool("perf", "stat", "/usr/bin/perf", "6.1")}
        text = registry.format_check()
        assert "Failed to load" in text
        assert "devtools_mcp.ghost.backend" in text
