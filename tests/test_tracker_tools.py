"""End-to-end tests for the tracker_* MCP tools via in-memory sessions.

Each tool call opens a fresh session/AppContext, so any state visible across
calls proves the on-disk SQLite persistence works.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from devtools_mcp.server import mcp
from devtools_mcp.tracker.db import ENV_DB_PATH


@pytest.fixture(autouse=True)
def isolated_tracker_db(tmp_path: Path, monkeypatch):
    """Point every tool call in a test at its own temp tracker database."""
    monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "tracker.db"))


async def _call_tool(name: str, arguments: dict | None = None) -> str:
    """Call an MCP tool via in-memory session and return the text result."""
    async with create_connected_server_and_client_session(mcp, raise_exceptions=True) as session:
        result = await session.call_tool(name, arguments or {})
        return result.content[0].text


async def _list_tools() -> set[str]:
    async with create_connected_server_and_client_session(mcp, raise_exceptions=True) as session:
        result = await session.list_tools()
        return {t.name for t in result.tools}


class TestRegistration:
    async def test_tracker_tools_listed(self):
        names = await _list_tools()
        expected = {
            "tracker_project",
            "tracker_task",
            "tracker_status",
            "tracker_criteria",
            "tracker_tag",
            "tracker_commits",
            "tracker_deps",
            "tracker_issue",
            "tracker_query",
            "tracker_sync",
        }
        assert expected <= names


class TestProjectTool:
    async def test_create_list_get(self):
        text = await _call_tool(
            "tracker_project",
            {"action": "create", "key": "GR", "name": "Grind"},
        )
        assert "Created project **GR**" in text
        listed = await _call_tool("tracker_project", {"action": "list"})
        assert "GR" in listed and "Grind" in listed
        got = await _call_tool("tracker_project", {"action": "get", "key": "GR"})
        assert "policy=advisory" in got

    async def test_bad_key_is_error_string(self):
        text = await _call_tool("tracker_project", {"action": "create", "key": "x", "name": "n"})
        assert text.startswith("Error:")


class TestTaskLifecycle:
    async def test_full_lifecycle_advisory(self):
        await _call_tool("tracker_project", {"action": "create", "key": "GR", "name": "Grind"})
        created = await _call_tool(
            "tracker_task",
            {"action": "create", "project": "GR", "title": "Build the thing", "kind": "epic"},
        )
        assert "GR-1" in created
        broke = await _call_tool(
            "tracker_task",
            {"action": "breakdown", "key": "GR-1", "subtasks": ["part one", "part two"]},
        )
        assert "GR-2" in broke and "GR-3" in broke and "(story" in broke
        await _call_tool(
            "tracker_criteria",
            {"action": "add", "key": "GR-2", "text": "it works", "test_ref": "tests/test_x.py::test_works"},
        )
        recorded = await _call_tool("tracker_criteria", {"action": "record", "criterion_id": 1, "result": "pass"})
        assert "**pass**" in recorded
        closed = await _call_tool("tracker_status", {"key": "GR-2", "status": "done"})
        assert "[done]" in closed
        assert "Warnings" not in closed
        detail = await _call_tool("tracker_task", {"action": "get", "key": "GR-1"})
        assert "Children (2)" in detail

    async def test_strict_gate_via_tools(self):
        await _call_tool(
            "tracker_project",
            {"action": "create", "key": "ST", "name": "Strict", "close_policy": "strict"},
        )
        await _call_tool("tracker_task", {"action": "create", "project": "ST", "title": "gated"})
        await _call_tool("tracker_criteria", {"action": "add", "key": "ST-1", "text": "must pass"})
        rejected = await _call_tool("tracker_status", {"key": "ST-1", "status": "done"})
        assert rejected.startswith("Error:") and "Strict close gate" in rejected
        forced = await _call_tool("tracker_status", {"key": "ST-1", "status": "done", "override": True})
        assert "[done]" in forced and "Warnings" in forced

    async def test_advisory_close_warns(self):
        await _call_tool("tracker_project", {"action": "create", "key": "AD", "name": "Adv"})
        await _call_tool("tracker_task", {"action": "create", "project": "AD", "title": "t"})
        await _call_tool("tracker_criteria", {"action": "add", "key": "AD-1", "text": "c"})
        closed = await _call_tool("tracker_status", {"key": "AD-1", "status": "done"})
        assert "[done]" in closed and "Warnings" in closed

    async def test_move_and_tags(self):
        await _call_tool("tracker_project", {"action": "create", "key": "MV", "name": "Move"})
        await _call_tool("tracker_task", {"action": "create", "project": "MV", "title": "a"})
        await _call_tool("tracker_task", {"action": "create", "project": "MV", "title": "b"})
        moved = await _call_tool("tracker_task", {"action": "move", "key": "MV-2", "parent": "MV-1"})
        assert "depth 1" in moved
        tagged = await _call_tool("tracker_tag", {"action": "add", "key": "MV-2", "tag": "Needs Review"})
        assert "`needs-review`" in tagged


class TestTagRulesViaTools:
    async def test_rule_applies_at_creation(self):
        await _call_tool("tracker_project", {"action": "create", "key": "TR", "name": "Tags"})
        await _call_tool("tracker_tag", {"action": "rule_add", "tag": "user-story", "match_kind": "story"})
        created = await _call_tool(
            "tracker_task",
            {"action": "create", "project": "TR", "title": "As a user...", "kind": "story"},
        )
        assert "auto-tags: user-story" in created
        rules = await _call_tool("tracker_tag", {"action": "rule_list"})
        assert "user-story" in rules


class TestQueryTool:
    async def _seed(self):
        await _call_tool("tracker_project", {"action": "create", "key": "QQ", "name": "Query"})
        await _call_tool(
            "tracker_task",
            {"action": "create", "project": "QQ", "title": "epic", "kind": "epic"},
        )
        await _call_tool("tracker_task", {"action": "breakdown", "key": "QQ-1", "subtasks": ["s1", "s2", "s3"]})

    async def test_tasks_view_and_filters(self):
        await self._seed()
        table = await _call_tool("tracker_query", {"view": "tasks", "project": "QQ"})
        assert "QQ-1" in table and "QQ-4" in table
        stories = await _call_tool("tracker_query", {"view": "tasks", "project": "QQ", "kind": "story"})
        assert "QQ-2" in stories
        assert "| epic |" not in stories  # the epic row itself is filtered out

    async def test_tree_view(self):
        await self._seed()
        tree = await _call_tool("tracker_query", {"view": "tree", "project": "QQ"})
        assert "QQ-1 (epic)" in tree
        assert "  [ ] QQ-2" in tree  # indented child

    async def test_rollup_view(self):
        await self._seed()
        rollup = await _call_tool("tracker_query", {"view": "rollup", "project": "QQ"})
        assert "epic" in rollup and "story" in rollup

    async def test_schema_and_limit(self):
        await self._seed()
        schema = await _call_tool("tracker_query", {"view": "tasks", "columns": ["schema"]})
        assert "`key`" in schema and "`status`" in schema
        paged = await _call_tool("tracker_query", {"view": "tasks", "project": "QQ", "limit": 2})
        assert "Rows 1-2 of 4" in paged

    async def test_unknown_view(self):
        text = await _call_tool("tracker_query", {"view": "bogus"})
        assert "Unknown view" in text


class TestCommitsTool:
    async def test_manual_link_and_detail(self):
        await _call_tool("tracker_project", {"action": "create", "key": "CM", "name": "Commits"})
        await _call_tool("tracker_task", {"action": "create", "project": "CM", "title": "t"})
        linked = await _call_tool(
            "tracker_commits",
            {"action": "link", "key": "CM-1", "repo": "C:/repo", "commit": "a" * 40, "message": "fix CM-1"},
        )
        assert "Linked" in linked
        again = await _call_tool(
            "tracker_commits",
            {"action": "link", "key": "CM-1", "repo": "C:/repo", "commit": "a" * 40},
        )
        assert "already linked" in again
        detail = await _call_tool("tracker_task", {"action": "get", "key": "CM-1"})
        assert "aaaaaaaaaaaa" in detail


class TestDepsTool:
    async def _seed(self):
        await _call_tool("tracker_project", {"action": "create", "key": "DP", "name": "Deps"})
        for title in ("schema", "parser", "ship"):
            await _call_tool("tracker_task", {"action": "create", "project": "DP", "title": title})
        await _call_tool("tracker_deps", {"action": "add", "key": "DP-2", "depends_on": "DP-1"})
        await _call_tool("tracker_deps", {"action": "add", "key": "DP-3", "depends_on": "DP-2"})

    async def test_add_list_and_cycle_error(self):
        await self._seed()
        listed = await _call_tool("tracker_deps", {"action": "list", "key": "DP-2"})
        assert "waits on" in listed and "DP-1" in listed
        assert "blocks" in listed and "DP-3" in listed
        cycle = await _call_tool("tracker_deps", {"action": "add", "key": "DP-1", "depends_on": "DP-3"})
        assert cycle.startswith("Error:") and "cycle" in cycle

    async def test_resolve_plan(self):
        await self._seed()
        plan = await _call_tool("tracker_deps", {"action": "resolve", "project": "DP"})
        assert "Ready now (1)" in plan and "DP-1" in plan
        assert "Blocked (2)" in plan
        assert "`DP-2` ← waiting on `DP-1`" in plan
        assert "Order" in plan

    async def test_close_reports_unblocked(self):
        await self._seed()
        closed = await _call_tool("tracker_status", {"key": "DP-1", "status": "done"})
        assert "Now unblocked:" in closed and "DP-2" in closed
        plan = await _call_tool("tracker_deps", {"action": "resolve", "key": "DP-3"})
        assert "(goal `DP-3`)" in plan
        assert "Ready now (1)" in plan and "DP-2" in plan
