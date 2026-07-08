"""Tests for live skills: CRDT store, convergence, materialization, HTTP sync,
and the skill_live MCP tool."""

from __future__ import annotations

from pathlib import Path

import pytest

import devtools_mcp.server  # noqa: F401  (registers backends)
from devtools_mcp.skilldocs import SkillDocError, SkillDocStore
from devtools_mcp.skilldocs import store as store_mod
from devtools_mcp.skilldocs.sync import sync_once
from devtools_mcp.viz.server import VizServer
from devtools_mcp.workspace import AppContext

SKILL = """---
name: {name}
description: A living test skill that agents keep improving.
---

# {name}

## Rules
- rule one
"""


def _content(name: str = "live-test") -> str:
    return SKILL.format(name=name)


@pytest.fixture
def store(tmp_path: Path) -> SkillDocStore:
    s = SkillDocStore(root=tmp_path / "docs-a")
    yield s
    s.close()


@pytest.fixture
def store_b(tmp_path: Path) -> SkillDocStore:
    s = SkillDocStore(root=tmp_path / "docs-b")
    yield s
    s.close()


class TestStore:
    def test_create_get_roundtrip(self, store):
        path = store.create("live-test", _content())
        assert store.get_text("live-test") == _content()
        assert path is not None and path.name == "SKILL.md"
        assert path.read_text(encoding="utf-8") == _content()

    def test_create_requires_matching_frontmatter(self, store):
        with pytest.raises(SkillDocError, match="frontmatter"):
            store.create("live-test", "# no frontmatter at all\n")
        with pytest.raises(SkillDocError, match="frontmatter"):
            store.create("live-test", _content("other-name"))

    def test_bad_names_rejected(self, store):
        for bad in ("", "has space", "x", "-lead"):
            with pytest.raises(SkillDocError, match="Bad skill name"):
                store.create(bad, _content(bad))

    def test_uppercase_name_normalized(self, store):
        store.create("LIVE-TEST", _content("live-test"))
        assert store.exists("live-test")

    def test_duplicate_create_rejected(self, store):
        store.create("live-test", _content())
        with pytest.raises(SkillDocError, match="already exists"):
            store.create("live-test", _content())

    def test_append_and_patch(self, store):
        store.create("live-test", _content())
        store.append("live-test", "- rule two\n")
        assert store.get_text("live-test").endswith("- rule one\n- rule two\n")
        store.patch("live-test", "- rule one", "- rule one (amended)")
        assert "- rule one (amended)" in store.get_text("live-test")

    def test_patch_requires_unique_match(self, store):
        store.create("live-test", _content())
        with pytest.raises(SkillDocError, match="not found"):
            store.patch("live-test", "nonexistent", "x")
        store.append("live-test", "- rule one\n")  # now ambiguous
        with pytest.raises(SkillDocError, match="matches 2 times"):
            store.patch("live-test", "- rule one", "x")

    def test_persistence_across_reopen(self, tmp_path):
        first = SkillDocStore(root=tmp_path / "docs")
        first.create("live-test", _content())
        first.append("live-test", "- persisted\n")
        first.close()
        second = SkillDocStore(root=tmp_path / "docs")
        try:
            assert "- persisted" in second.get_text("live-test")
        finally:
            second.close()

    def test_compaction_preserves_content(self, tmp_path, monkeypatch):
        monkeypatch.setattr(store_mod, "UPDATES_COMPACT_AT", 5)
        s = SkillDocStore(root=tmp_path / "docs")
        try:
            s.create("live-test", _content())
            for i in range(8):
                s.append("live-test", f"- line {i}\n")
            rows = s.conn.execute("SELECT COUNT(*) FROM skill_updates WHERE name='live-test'").fetchone()[0]
            assert rows <= 5
            text = s.get_text("live-test")
            for i in range(8):
                assert f"- line {i}" in text
        finally:
            s.close()

    def test_unknown_skill_errors(self, store):
        with pytest.raises(SkillDocError, match="No live skill"):
            store.get_text("live-test")

    def test_delete_removes_log_and_file(self, store):
        path = store.create("live-test", _content())
        assert path is not None and path.is_file()
        assert store.delete("live-test") is True
        assert not path.exists()
        assert not store.exists("live-test")
        with pytest.raises(SkillDocError, match="No live skill"):
            store.delete("live-test")


class TestConvergence:
    def test_concurrent_edits_merge(self, store, store_b):
        store.create("live-test", _content())
        store_b.apply("live-test", store.diff("live-test", None))
        # concurrent divergent edits
        store.append("live-test", "- from machine A\n")
        store_b.append("live-test", "- from machine B\n")
        store_b.apply("live-test", store.diff("live-test", store_b.state("live-test")))
        store.apply("live-test", store_b.diff("live-test", store.state("live-test")))
        a, b = store.get_text("live-test"), store_b.get_text("live-test")
        assert a == b
        assert "- from machine A" in a and "- from machine B" in a

    def test_concurrent_patches_both_survive(self, store, store_b):
        store.create("live-test", _content())
        store_b.apply("live-test", store.diff("live-test", None))
        store.patch("live-test", "# live-test", "# live-test (A header)")
        store_b.patch("live-test", "- rule one", "- rule one (B rule)")
        store_b.apply("live-test", store.diff("live-test", store_b.state("live-test")))
        store.apply("live-test", store_b.diff("live-test", store.state("live-test")))
        text = store.get_text("live-test")
        assert text == store_b.get_text("live-test")
        assert "(A header)" in text and "(B rule)" in text

    def test_materialize_skipped_until_valid(self, store):
        # a doc created remotely could arrive in fragments; simulate a doc
        # whose text has no valid frontmatter yet
        from pycrdt import Doc, Text

        doc = Doc()
        doc["content"] = t = Text()
        t += "partial body without frontmatter"
        path = store.apply("live-test", doc.get_update())
        assert path is None  # stored but not materialized
        assert store.get_text("live-test") == "partial body without frontmatter"


class TestHttpSync:
    @pytest.fixture
    def served(self, tmp_path, monkeypatch):
        """A VizServer whose skilldoc store lives in the isolated data root."""
        srv = VizServer(AppContext())
        url = srv.start(port=0)
        yield url
        srv.stop()

    def test_push_pull_roundtrip(self, served, tmp_path):
        local = SkillDocStore(root=tmp_path / "local-store")
        try:
            local.create("live-test", _content())
            counters = sync_once(local, served)
            assert counters["pushed"] == 1
            # the server store (data root) now has it; a second machine pulls
            other = SkillDocStore(root=tmp_path / "other-store")
            try:
                counters2 = sync_once(other, served)
                assert counters2["pulled"] == 1
                assert other.get_text("live-test") == _content()
            finally:
                other.close()
        finally:
            local.close()

    def test_bidirectional_convergence_via_server(self, served, tmp_path):
        a = SkillDocStore(root=tmp_path / "a-store")
        b = SkillDocStore(root=tmp_path / "b-store")
        try:
            a.create("live-test", _content())
            sync_once(a, served)
            sync_once(b, served)
            a.append("live-test", "- A's improvement\n")
            b.append("live-test", "- B's improvement\n")
            sync_once(a, served)
            sync_once(b, served)
            sync_once(a, served)  # pick up B's change relayed via the server
            ta, tb = a.get_text("live-test"), b.get_text("live-test")
            assert ta == tb
            assert "- A's improvement" in ta and "- B's improvement" in ta
        finally:
            a.close()
            b.close()

    def test_sync_unreachable_peer_errors(self, tmp_path):
        s = SkillDocStore(root=tmp_path / "s")
        try:
            with pytest.raises(SkillDocError, match="unreachable"):
                sync_once(s, "http://127.0.0.1:9")
        finally:
            s.close()


class TestSkillLiveTool:
    async def _call(self, arguments: dict) -> str:
        from mcp.shared.memory import create_connected_server_and_client_session

        from devtools_mcp.server import mcp

        async with create_connected_server_and_client_session(mcp, raise_exceptions=True) as session:
            result = await session.call_tool("skill_live", arguments)
            return result.content[0].text

    async def test_create_list_get_patch(self):
        created = await self._call({"action": "create", "name": "live-test", "content": _content()})
        assert "Created live skill **live-test**" in created and "SKILL.md" in created
        listed = await self._call({"action": "list"})
        assert "live-test" in listed
        patched = await self._call({"action": "patch", "name": "live-test", "old": "- rule one", "new": "- rule 1"})
        assert "Patched **live-test**" in patched
        got = await self._call({"action": "get", "name": "live-test"})
        assert "- rule 1" in got

    async def test_errors_are_strings(self):
        text = await self._call({"action": "get", "name": "live-test"})
        assert text.startswith("Error:")
        bad = await self._call({"action": "create", "name": "live-test", "content": "nope"})
        assert bad.startswith("Error:")
