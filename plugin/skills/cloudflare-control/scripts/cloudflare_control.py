# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Secret-free R2 test configuration and bounded official Cloudflare CLI status."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
import shutil
import signal
import subprocess
from pathlib import Path
from typing import Any

MAX_OUTPUT_BYTES = 64 * 1024
MAX_ACCOUNTS = 100
SECRET_ENV = (
    "CLOUDFLARE_API_TOKEN",
    "CLOUDFLARE_API_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
)


class ControlError(ValueError):
    """Public error codes never contain subprocess output or credential values."""


def s3_config(account: str, bucket: str, jurisdiction: str, environment: dict[str, str]) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{32}", account):
        raise ControlError("invalid_account_id")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", bucket):
        raise ControlError("invalid_bucket_name")
    if jurisdiction not in ("default", "eu"):
        raise ControlError("unsupported_jurisdiction")
    assert len(account) == 32 and 3 <= len(bucket) <= 63
    suffix = "r2.cloudflarestorage.com" if jurisdiction == "default" else "eu.r2.cloudflarestorage.com"
    result = {
        "schema": "ai-grind.r2-s3-config.v1",
        "endpoint": f"https://{account}.{suffix}",
        "bucket": bucket,
        "region": "auto",
        "path_style": True,
        "credential_environment": {
            "access_key": "AWS_ACCESS_KEY_ID",
            "secret_key": "AWS_SECRET_ACCESS_KEY",
            "optional_session_token": "AWS_SESSION_TOKEN",
        },
        "credentials_present": all(
            bool(environment.get(name)) for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
        ),
        "session_token_present": bool(environment.get("AWS_SESSION_TOKEN")),
        "remote_verified": False,
    }
    assert result["region"] == "auto" and result["remote_verified"] is False
    return result


def status_argv(provider: str, executable_name: str, javascript: str, node: str) -> list[str]:
    if provider not in ("cf", "wrangler"):
        raise ControlError("invalid_provider")
    if javascript:
        script, executable = Path(javascript).expanduser(), shutil.which(node)
        if not script.is_file() or executable is None:
            raise ControlError("cli_or_node_missing")
        command = [executable, str(script)]
    else:
        executable = shutil.which(executable_name)
        if executable is None:
            raise ControlError("cli_missing")
        if Path(executable).suffix.lower() in (".cmd", ".bat"):
            raise ControlError("use_cli_js_with_node_on_windows")
        command = [executable]
    assert command and all(isinstance(part, str) for part in command)
    command.extend(["whoami", "--json"] if provider == "wrangler" else ["auth", "whoami"])
    assert command[-2:] in (["whoami", "--json"], ["auth", "whoami"])
    return command


def choose_provider(provider: str, project: Path) -> str:
    if provider != "auto":
        return provider
    assert project.is_dir()
    wrangler = any((project / name).is_file() for name in ("wrangler.jsonc", "wrangler.json", "wrangler.toml"))
    selected = "wrangler" if wrangler and not (project / "cloudflare.config.ts").is_file() else "cf"
    assert selected in ("cf", "wrangler")
    return selected


async def _read_limited(stream: asyncio.StreamReader) -> bytes:
    chunks: list[bytes] = []
    size = 0
    for _ in range(MAX_OUTPUT_BYTES + 1):
        chunk = await stream.read(min(4096, MAX_OUTPUT_BYTES + 1 - size))
        if not chunk:
            assert size <= MAX_OUTPUT_BYTES
            return b"".join(chunks)
        size += len(chunk)
        if size > MAX_OUTPUT_BYTES:
            raise ControlError("tool_output_limit")
        chunks.append(chunk)
    raise ControlError("tool_output_limit")


async def _stop(process: asyncio.subprocess.Process) -> None:
    assert process.pid > 0
    if os.name == "nt":
        with contextlib.suppress(OSError, TimeoutError):
            killer = await asyncio.create_subprocess_exec(
                "taskkill",
                "/PID",
                str(process.pid),
                "/T",
                "/F",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                await asyncio.wait_for(killer.wait(), 5)
            finally:
                if killer.returncode is None:
                    killer.kill()
                    await killer.wait()
    else:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
    await asyncio.wait_for(process.wait(), 5)
    assert process.returncode is not None


async def _discard(stream: asyncio.StreamReader) -> None:
    # No retained bytes. Cleanup owns this pipe after its capped reader ends;
    # the cleanup deadline and read count bound even a surviving writer.
    assert stream is not None
    for _ in range(MAX_OUTPUT_BYTES + 1):
        if not await stream.read(4096):
            assert stream.at_eof()
            return
    raise ControlError("child_cleanup_output_limit")


async def _cleanup_status(process: asyncio.subprocess.Process) -> None:
    assert process.stdout is not None and process.stderr is not None
    drainers = [asyncio.create_task(_discard(process.stdout)), asyncio.create_task(_discard(process.stderr))]
    try:
        async with asyncio.timeout(5):
            await _stop(process)  # readers stay active while killed pipes drain
            await asyncio.gather(*drainers)
        assert process.returncode is not None
    finally:
        for task in drainers:
            task.cancel()
        await asyncio.gather(*drainers, return_exceptions=True)


async def run_status(command: list[str], timeout: float, cwd: Path | None = None) -> bytes:
    if not 0 < timeout <= 60:
        raise ControlError("invalid_timeout")
    assert command[-2:] in (["whoami", "--json"], ["auth", "whoami"])
    environment = dict(os.environ)
    environment.update({"CI": "true", "WRANGLER_SEND_METRICS": "false", "NO_COLOR": "1", "DEBUG": ""})
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=cwd,
        env=environment,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=os.name != "nt",
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    assert process.stdout is not None and process.stderr is not None
    tasks = [
        asyncio.create_task(_read_limited(process.stdout)),
        asyncio.create_task(_read_limited(process.stderr)),
        asyncio.create_task(process.wait()),
    ]
    finished = False
    try:
        async with asyncio.timeout(timeout):
            output, _, code = await asyncio.gather(*tasks)
        if code != 0:
            raise ControlError("official_auth_status_failed")
        finished = True
        return output
    except TimeoutError as error:
        raise ControlError("tool_timeout") from error
    finally:
        # Stop and join the original readers before assigning each pipe to its
        # discard reader. A capped/paused pipe must drain during wait(), or
        # asyncio's subprocess transport can retain it after the child exits.
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if process.returncode is None or not finished:
            await _cleanup_status(process)


def decode_status(output: bytes, environment: dict[str, str], provider: str) -> dict[str, Any]:
    assert isinstance(output, bytes)
    if provider not in ("cf", "wrangler"):
        raise ControlError("invalid_provider")
    if len(output) > MAX_OUTPUT_BYTES:
        raise ControlError("tool_output_limit")
    try:
        value = json.loads(output)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ControlError("invalid_status_json") from error
    if not isinstance(value, dict):
        raise ControlError("invalid_status_json")
    logged_in = value.get("authenticated") if provider == "cf" else value.get("loggedIn")
    if logged_in is not True or (provider == "cf" and value.get("tokenValid") is not True):
        raise ControlError("not_authenticated")
    accounts = value.get("accounts")
    if not isinstance(accounts, list) or len(accounts) > MAX_ACCOUNTS:
        raise ControlError("invalid_account_list")
    clean = []
    secrets = [environment[name] for name in SECRET_ENV if environment.get(name)]
    for item in accounts:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            raise ControlError("invalid_account_list")
        account_id, name = item["id"], item.get("name", "")
        if not re.fullmatch(r"[0-9a-f]{32}", account_id) or not isinstance(name, str) or len(name) > 128:
            raise ControlError("invalid_account_list")
        for secret in secrets:
            name = name.replace(secret, "[REDACTED]")
        clean.append({"id": account_id, "name": name})
    auth_types = {
        "OAuth Token": "oauth",
        "User API Token": "user_api_token",
        "Account API Token": "account_api_token",
        "Global API Key": "global_api_key",
    }
    auth_type = value.get("authType", "unknown")
    if not isinstance(auth_type, str):
        raise ControlError("invalid_auth_type")
    assert len(clean) <= MAX_ACCOUNTS
    return {
        "status": "ok",
        "provider": f"official_{provider}",
        "authenticated": True,
        "auth_type": auth_types.get(auth_type, "unknown"),
        "accounts": clean,
        "mcp_authenticated": "not_checked",
        "s3_credentials": "not_checked",
    }


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    config = commands.add_parser("s3-config", help="prepare non-secret configuration; no remote I/O")
    config.add_argument("--account", required=True)
    config.add_argument("--bucket", required=True)
    config.add_argument("--jurisdiction", choices=("default", "eu"), default="default")
    status = commands.add_parser("auth-status", help="explicit official CLI read; may refresh its own OAuth")
    status.add_argument("--provider", choices=("auto", "cf", "wrangler"), default="auto")
    status.add_argument("--project", type=Path, default=Path.cwd())
    status.add_argument("--cf", default="cf")
    status.add_argument("--cf-js", default="")
    status.add_argument("--wrangler", default="wrangler")
    status.add_argument("--wrangler-js", default="")
    status.add_argument("--node", default="node")
    status.add_argument("--timeout", type=float, default=20)
    args = parser.parse_args(argv)
    try:
        if args.action == "s3-config":
            result = s3_config(args.account, args.bucket, args.jurisdiction, dict(os.environ))
        else:
            if not args.project.is_dir():
                raise ControlError("project_directory_missing")
            provider = choose_provider(args.provider, args.project)
            executable = args.cf if provider == "cf" else args.wrangler
            javascript = args.cf_js if provider == "cf" else args.wrangler_js
            command = status_argv(provider, executable, javascript, args.node)
            result = decode_status(await run_status(command, args.timeout, args.project), dict(os.environ), provider)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (ControlError, OSError) as error:
        code = str(error) if isinstance(error, ControlError) else "tool_unavailable"
        print(json.dumps({"status": "error", "code": code}))
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
