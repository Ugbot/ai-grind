"""Exercise the Codex launch command and project-map's explicit root contract."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parents[1]


async def test_codex_stdio_command_launches_from_plugin_root(tmp_path):
    if not shutil.which("uv"):
        pytest.skip("uv is required to test the plugin launch command")
    manifest = json.loads((ROOT / ".codex-plugin/plugin.json").read_text())
    config = json.loads((ROOT / manifest["mcpServers"]).read_text())["mcpServers"]["devtools-mcp"]
    assert "${" not in json.dumps(config)
    cwd = (ROOT / config["cwd"]).resolve()
    assert (cwd / "pyproject.toml").is_file()
    env = dict(
        os.environ,
        PYTHONPATH=str(ROOT / "src"),
        UV_PROJECT_ENVIRONMENT=sys.prefix,
        UV_NO_SYNC="1",
        UV_CACHE_DIR=str(tmp_path / "uv-cache"),
        DEVTOOLS_MCP_DBOS_DB=str(tmp_path / "child-dbos.sqlite"),
        DEVTOOLS_MCP_DASHBOARD="0",
    )
    params = StdioServerParameters(command=config["command"], args=config["args"], cwd=str(cwd), env=env)
    async with stdio_client(params) as (reader, writer), ClientSession(reader, writer) as session:
        hello = await session.initialize()
        assert hello.serverInfo.name == "devtools-mcp"
        names = {tool.name for tool in (await session.list_tools()).tools}
        assert {"devtools_check", "devtools_run", "tracker_task"} <= names


def test_project_map_explicitly_disables_legacy_mcp_fallback():
    root = ROOT / "plugins/project-map"
    manifest = json.loads((root / ".codex-plugin/plugin.json").read_text())
    assert manifest["mcpServers"] == {}
    assert (root / manifest["skills"]).is_dir()
    assert (root / ".mcp.json").is_file()  # Claude's config remains separate.


def test_project_map_indexes_explicit_project_not_process_cwd(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    (target / "README.md").write_text("# kumquat_unique_target\n")
    other = tmp_path / "other"
    other.mkdir()
    script = ROOT / "plugins/project-map/skills/project-map/scripts/kb_mcp.py"
    subprocess.run(
        [sys.executable, str(script.with_name("kb.py")), "--root", str(target), "init"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "kb_index", "arguments": {}}},
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "kb_search", "arguments": {"query": "kumquat_unique_target"}},
        },
    ]
    result = subprocess.run(
        [sys.executable, str(script)],
        input="".join(json.dumps(request) + "\n" for request in requests),
        capture_output=True,
        text=True,
        cwd=other,
        env=dict(os.environ, CLAUDE_PROJECT_DIR=str(target)),
        timeout=30,
        check=True,
    )
    replies = [json.loads(line) for line in result.stdout.splitlines()]
    assert all(not reply["result"]["isError"] for reply in replies), replies
    assert "README.md" in replies[-1]["result"]["content"][0]["text"]
    assert not (other / ".claude/kb").exists()
