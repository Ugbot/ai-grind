"""Tests for the plugin health surface, version-compat gating, and the
recipe runner/console error paths added in the hardening pass."""

from __future__ import annotations

import importlib.metadata
from pathlib import Path

import pytest

import devtools_mcp.registry as reg
from devtools_mcp.recipes import store
from devtools_mcp.recipes.db import RecipesDB, open_recipes
from devtools_mcp.recipes.runner import run_recipe

# --- fakes for entry-point loading -----------------------------------------


class _FakeMeta:
    def __init__(self, requires: list[str]) -> None:
        self._requires = requires

    def get_all(self, name: str):
        return self._requires if name == "Requires-Dist" else []


class _FakeDist:
    def __init__(self, requires: list[str]) -> None:
        self.metadata = _FakeMeta(requires)


class _FakeEP:
    def __init__(self, name: str, requires: list[str], on_load=None) -> None:
        self.name = name
        self.dist = _FakeDist(requires)
        self.loaded = False
        self._on_load = on_load

    def load(self):
        self.loaded = True
        if self._on_load is not None:
            return self._on_load()
        return object()


# --- version-compat ---------------------------------------------------------


class TestVersionCompat:
    def test_spec_incompat_detects_mismatch(self):
        assert reg._spec_incompat(">=999", "0.2.0") is not None
        assert reg._spec_incompat(">=0.1", "0.2.0") is None
        assert reg._spec_incompat("", "0.2.0") is None  # no declaration
        assert reg._spec_incompat(">=1", "") is None  # unknown host version → don't block

    def test_host_incompat_reason_from_dist_metadata(self):
        too_new = _FakeEP("p", ["devtools-mcp>=999", "httpx>=0.28"])
        compatible = _FakeEP("q", ["devtools-mcp>=0.1"])
        none_declared = _FakeEP("r", ["httpx>=0.28"])
        assert reg.host_incompat_reason(too_new) is not None
        assert reg.host_incompat_reason(compatible) is None
        assert reg.host_incompat_reason(none_declared) is None

    def test_attr_incompat_reason(self):
        class _Mod:
            __devtools_mcp_requires__ = ">=999"

        assert reg._attr_incompat_reason(_Mod()) is not None
        assert reg._attr_incompat_reason(object()) is None  # no attr


class TestLoaderSkipsIncompatible:
    def test_incompatible_tool_plugin_skipped_not_loaded(self, monkeypatch):
        bad = _FakeEP("badplug", ["devtools-mcp>=999"])

        def fake_entry_points(*, group: str):
            return [bad] if group == "devtools_mcp.mcp_tools" else []

        monkeypatch.setattr(importlib.metadata, "entry_points", fake_entry_points)
        monkeypatch.setattr(reg, "_FAILED_TOOL_PLUGINS", {})
        monkeypatch.setattr(reg, "_LOADED_TOOL_PLUGINS", set())

        reg.load_tool_plugins()  # must not raise

        failed = reg.failed_tool_plugins()
        assert "tools:badplug" in failed
        assert "skipped" in failed["tools:badplug"] and "requires" in failed["tools:badplug"]
        assert not bad.loaded  # gated BEFORE import — never executed
        assert "tools:badplug" not in reg.loaded_tool_plugins()

    def test_broken_tool_plugin_degrades(self, monkeypatch):
        def boom():
            raise RuntimeError("kaboom")

        bad = _FakeEP("brokenplug", [], on_load=boom)

        def fake_entry_points(*, group: str):
            return [bad] if group == "devtools_mcp.mcp_tools" else []

        monkeypatch.setattr(importlib.metadata, "entry_points", fake_entry_points)
        monkeypatch.setattr(reg, "_FAILED_TOOL_PLUGINS", {})
        monkeypatch.setattr(reg, "_LOADED_TOOL_PLUGINS", set())

        reg.load_tool_plugins()  # must not raise
        assert "RuntimeError" in reg.failed_tool_plugins()["tools:brokenplug"]


# --- the plugins tool -------------------------------------------------------


class TestPluginsTool:
    async def test_list_reports_loaded_surface(self):
        from devtools_mcp.tools.plugins_tools import plugins

        out = await plugins(action="list")
        assert "Plugin surface" in out
        assert "Backends" in out and "valgrind" in out
        assert "Console pages" in out and "recipes" in out  # migrated page shows up

    async def test_status_reports_failures(self, monkeypatch):
        from devtools_mcp.tools import plugins_tools

        monkeypatch.setattr(
            plugins_tools, "failed_tool_plugins", lambda: {"tools:ghost": "skipped: requires devtools-mcp>=999"}
        )
        out = await plugins_tools.plugins(action="status")
        assert "Health" in out
        assert "tools:ghost" in out and "requires devtools-mcp>=999" in out

    async def test_status_clean_when_no_failures(self, monkeypatch):
        from devtools_mcp.tools import plugins_tools
        from devtools_mcp.viz import pages

        monkeypatch.setattr(plugins_tools, "failed_backends", lambda: {})
        monkeypatch.setattr(plugins_tools, "failed_tool_plugins", lambda: {})
        monkeypatch.setattr(pages, "failed_viz_pages", lambda: {})
        out = await plugins_tools.plugins(action="status")
        assert "loaded cleanly" in out

    async def test_unknown_action(self):
        from devtools_mcp.tools.plugins_tools import plugins

        assert "Unknown action" in await plugins(action="bogus")


# --- recipe runner / console error paths ------------------------------------


@pytest.fixture
def db(tmp_path: Path) -> RecipesDB:
    recipes = open_recipes(tmp_path / "recipes.db")
    yield recipes
    recipes.close()


def _register(db: RecipesDB, steps):
    store.register_recipe(db, {"key": "demo", "name": "Demo", "kind": "test", "steps": steps})


class TestRunnerErrorPaths:
    async def test_workflow_crash_finalizes_failed(self, db, monkeypatch):
        """A DBOS launch/workflow crash records the run failed, never propagates."""
        import devtools_mcp.recipes.runner as runner_mod

        _register(db, [{"label": "one", "command": "true"}])

        def boom(*args, **kwargs):
            raise RuntimeError("DBOS unavailable")

        monkeypatch.setattr(runner_mod, "_invoke_workflow", boom)
        result = await run_recipe(db, "demo")
        assert result.status == "failed"
        run = store.get_run(db.conn, result.run_id)
        assert run.status == "failed" and run.finished_at is not None
        assert run.raw_path  # the error was captured to a raw log

    async def test_step_crash_fails_step_not_run(self, db, monkeypatch):
        """An unexpected error inside a step becomes a failed step, run finalized."""
        import devtools_mcp.recipes.runner as runner_mod

        async def boom(*args, **kwargs):
            raise RuntimeError("exec exploded")

        _register(db, [{"label": "one", "command": "true"}])
        monkeypatch.setattr(runner_mod, "run_capture", boom)
        result = await run_recipe(db, "demo", force=True)
        assert result.status == "failed"
        assert result.steps[0].status == "failed"
        assert "crashed" in result.steps[0].tail

    def test_console_post_bad_key_is_clean_400(self):
        from devtools_mcp.viz import recipes_page

        resp = recipes_page.handle_post(["no-such-recipe", "run"], "")
        assert resp is not None and resp.status == 400
        assert "⛔" in resp.body  # a rendered error page, not a 500


class TestRegisterValidation:
    def test_bad_kind_rejected(self, db):
        from devtools_mcp.recipes.db import RecipesError

        with pytest.raises(RecipesError, match="kind"):
            store.register_recipe(db, {"key": "k", "kind": "bad kind!", "steps": [{"command": "true"}]})

    def test_empty_steps_rejected(self, db):
        from devtools_mcp.recipes.db import RecipesError

        with pytest.raises(RecipesError, match="at least one step"):
            store.register_recipe(db, {"key": "k", "kind": "test", "steps": []})
