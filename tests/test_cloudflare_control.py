"""Offline Cloudflare config/status contracts; never call real auth or resources."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills/authored/skills/devtools/cloudflare-control"


@pytest.fixture
def control():
    spec = importlib.util.spec_from_file_location("cloudflare_control_test", SKILL / "scripts/cloudflare_control.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_config_is_secret_free_and_portable(control):
    environment = {
        "AWS_ACCESS_KEY_ID": "PRIVATE-KEY",
        "AWS_SECRET_ACCESS_KEY": "PRIVATE-SECRET",
        "AWS_SESSION_TOKEN": "PRIVATE-TOKEN",
    }
    default = control.s3_config("a" * 32, "integration-test", "default", environment)
    encoded = json.dumps(default)
    assert not any(secret in encoded for secret in environment.values())
    assert default["endpoint"] == f"https://{'a' * 32}.r2.cloudflarestorage.com"
    assert default["region"] == "auto" and default["path_style"] is True
    assert default["credentials_present"] and default["session_token_present"]
    assert default["remote_verified"] is False
    eu = control.s3_config("b" * 32, "other-bucket", "eu", {})
    assert eu["endpoint"] == f"https://{'b' * 32}.eu.r2.cloudflarestorage.com"
    assert not eu["credentials_present"]


@pytest.mark.parametrize(
    "account,bucket,jurisdiction",
    [
        ("INVALID", "bucket-name", "default"),
        ("a" * 32, "BadName", "default"),
        ("a" * 32, "-bad", "default"),
        ("a" * 32, "a.b", "default"),
        ("a" * 32, "x" * 64, "default"),
        ("a" * 32, "bucket-name", "unknown"),
    ],
)
def test_invalid_config_refuses(control, account, bucket, jurisdiction):
    with pytest.raises(control.ControlError):
        control.s3_config(account, bucket, jurisdiction, {})


def test_provider_routing_preserves_project_configuration(control, tmp_path):
    assert control.choose_provider("auto", tmp_path) == "cf"
    (tmp_path / "wrangler.toml").write_text("name='existing'", encoding="utf-8")
    assert control.choose_provider("auto", tmp_path) == "wrangler"
    (tmp_path / "cloudflare.config.ts").write_text("export default {};", encoding="utf-8")
    assert control.choose_provider("auto", tmp_path) == "cf"
    assert control.choose_provider("wrangler", tmp_path) == "wrangler"


@pytest.mark.parametrize("provider", ["cf", "wrangler"])
def test_status_allowlists_accounts_and_redacts_secret_values(control, provider):
    value = {
        "accounts": [{"id": "a" * 32, "name": "account PRIVATE"}],
        "email": "private@example.test",
        "token": "RAW-UNKNOWN-SECRET",
        "scopes": ["raw-scope"],
        "authSource": "/private/auth-store",
    }
    value.update(
        {"authenticated": True, "tokenValid": True}
        if provider == "cf"
        else {"loggedIn": True, "authType": "OAuth Token"}
    )
    result = control.decode_status(json.dumps(value).encode(), {"CLOUDFLARE_API_TOKEN": "PRIVATE"}, provider)
    output = json.dumps(result)
    assert result["accounts"] == [{"id": "a" * 32, "name": "account [REDACTED]"}]
    assert not any(
        secret in output for secret in ("PRIVATE", "private@example.test", "RAW-UNKNOWN", "raw-scope", "auth-store")
    )
    assert result["mcp_authenticated"] == result["s3_credentials"] == "not_checked"


@pytest.mark.parametrize(
    "value,provider",
    [
        ({"authenticated": False}, "cf"),
        ({"authenticated": True, "tokenValid": False}, "cf"),
        ({"loggedIn": False}, "wrangler"),
        ({"loggedIn": True, "accounts": "wrong"}, "wrangler"),
        ({"loggedIn": True, "accounts": [{"id": "invalid"}]}, "wrangler"),
        ({"loggedIn": True, "accounts": [], "authType": []}, "wrangler"),
    ],
)
def test_status_does_not_infer_auth_from_exit_zero(control, value, provider):
    with pytest.raises(control.ControlError):
        control.decode_status(json.dumps(value).encode(), {}, provider)


def test_bad_status_json_and_budget_refuse(control):
    for output in (b"not json", b"[", b"x" * (control.MAX_OUTPUT_BYTES + 1)):
        with pytest.raises(control.ControlError):
            control.decode_status(output, {}, "cf")


def test_cli_resolution_keeps_spaces_as_single_arguments(control, monkeypatch, tmp_path):
    script = tmp_path / "official CLI.js"
    script.write_text("// fixture only", encoding="utf-8")
    monkeypatch.setattr(control.shutil, "which", lambda name: "/node with spaces" if name == "node" else None)
    assert control.status_argv("cf", "cf", str(script), "node") == ["/node with spaces", str(script), "auth", "whoami"]
    with pytest.raises(control.ControlError, match="cli_missing"):
        control.status_argv("wrangler", "missing", "", "node")
    monkeypatch.setattr(control.shutil, "which", lambda _: "C:/tools/cf.cmd")
    with pytest.raises(control.ControlError, match="use_cli_js"):
        control.status_argv("cf", "cf", "", "node")


def fake_command(tmp_path, body):
    script = tmp_path / "fake official CLI.py"
    script.write_text(body, encoding="utf-8")
    return [sys.executable, str(script), "auth", "whoami"]


async def test_fake_cli_status_works_from_other_directory(control, tmp_path):
    command = fake_command(tmp_path, 'print(\'{"authenticated":true,"tokenValid":true,"accounts":[]}\')\n')
    other = tmp_path / "other working directory"
    other.mkdir()
    raw = await control.run_status(command, 5, other)
    assert control.decode_status(raw, {}, "cf")["authenticated"] is True


@pytest.mark.parametrize("channel", ["stdout", "stderr", "both"])
async def test_fake_cli_flood_and_timeout_are_bounded(control, tmp_path, channel):
    if channel == "both":
        body = (
            "import sys, threading\n"
            'def flood(stream):\n    stream.write("PRIVATE" * 50000)\n    stream.flush()\n'
            "threads = [threading.Thread(target=flood, args=(stream,)) for stream in (sys.stdout, sys.stderr)]\n"
            "for thread in threads: thread.start()\n"
            "for thread in threads: thread.join()\n"
        )
    else:
        body = f'import sys\nsys.{channel}.write("PRIVATE" * 50000)\nsys.{channel}.flush()\n'
    flood = fake_command(tmp_path, body)
    with pytest.raises(control.ControlError, match="tool_output_limit"):
        await asyncio.wait_for(control.run_status(flood, 5), 7)
    stalled = fake_command(tmp_path, "import time\ntime.sleep(30)\n")
    with pytest.raises(control.ControlError, match="tool_timeout"):
        await asyncio.wait_for(control.run_status(stalled, 0.05), 7)


async def test_timeout_drains_buffered_pipes_before_reaping(control, tmp_path):
    body = (
        "import sys, time\n"
        'sys.stdout.write("x" * 32000)\nsys.stdout.flush()\n'
        'sys.stderr.write("y" * 32000)\nsys.stderr.flush()\ntime.sleep(30)\n'
    )
    command = fake_command(tmp_path, body)
    with pytest.raises(control.ControlError, match="tool_timeout"):
        await asyncio.wait_for(control.run_status(command, 0.1), 7)


async def test_failed_cli_returns_no_raw_error_or_secret(control, monkeypatch, tmp_path, capsys):
    command = fake_command(tmp_path, 'import sys\nsys.stderr.write("PRIVATE-TOKEN")\nsys.exit(3)\n')
    monkeypatch.setattr(control, "status_argv", lambda *_: command)
    assert await control.main(["auth-status", "--provider", "cf", "--project", str(tmp_path)]) == 1
    result = capsys.readouterr()
    assert "PRIVATE" not in result.out + result.err
    assert json.loads(result.out) == {"status": "error", "code": "official_auth_status_failed"}


async def test_cancellation_reaps_direct_child(control, monkeypatch, tmp_path):
    original = control.asyncio.create_subprocess_exec
    children = []

    async def capture(*args, **kwargs):
        child = await original(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(control.asyncio, "create_subprocess_exec", capture)
    command = fake_command(tmp_path, "import time\ntime.sleep(30)\n")
    task = asyncio.create_task(control.run_status(command, 10))
    for _ in range(100):
        if children:
            break
        await asyncio.sleep(0.01)
    assert children
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 7)
    assert children[0].returncode is not None


def test_agent_and_official_source_registration_are_explicit():
    role = tomllib.loads((SKILL / "agents/cloudflare-controller.toml").read_text(encoding="utf-8"))
    assert role["name"] == "cloudflare-controller"
    assert (ROOT / "skills/authored/agents/integration/cloudflare-controller.md").is_file()
    sources = tomllib.loads((ROOT / "skills/sources.toml").read_text(encoding="utf-8"))["item"]
    official = [entry for entry in sources if entry.get("category") == "cloudflare"]
    assert len(official) == 19
    assert all(entry["src"].startswith("~/.agents/skills/") for entry in official)
    assert len({Path(entry["src"]).name for entry in official}) == 19
