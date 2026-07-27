"""devtools_install: per-OS install commands for backend tool dependencies."""

from __future__ import annotations

import os

from mcp.server.fastmcp import Context

from devtools_mcp.install import format_plan, run_steps, steps_for
from devtools_mcp.registry import ToolRegistry, get_backend, list_backends
from devtools_mcp.server import get_app_ctx, mcp

_ALLOW_ENV = "DEVTOOLS_MCP_ALLOW_INSTALL"


def _suites_with_specs() -> list[str]:
    return sorted(s for s in list_backends() if get_backend(s).install is not None or get_backend(s).tool_installs)


@mcp.tool()
async def devtools_install(ctx: Context, suite: str, tool: str = "", execute: bool = False, timeout: int = 900) -> str:
    """Show (or run) the install commands for a tool suite's underlying tool.

    Default is a dry-run plan: the exact per-OS commands (winget/apt/pip/
    download) to run in your own shell. execute=True runs them from the server
    process — only allowed when DEVTOOLS_MCP_ALLOW_INSTALL=1 — then re-detects
    the suite and reports the availability delta.

    Args:
        suite: Backend suite name (see devtools_check for the list)
        tool: Specific tool within the suite, for suites whose tools install
              separately (e.g. suite="debug", tool="debugpy")
        execute: Run the steps instead of printing them (env-gated)
        timeout: Max seconds per step when executing (default 900)
    """
    try:
        backend = get_backend(suite)
    except KeyError:
        return f"Unknown suite '{suite}'. Suites with install specs: {', '.join(_suites_with_specs()) or 'none'}"

    spec = backend.tool_installs.get(tool) if tool else backend.install
    if spec is None and tool:
        per_tool = sorted(backend.tool_installs)
        return (
            f"No install spec for tool '{tool}' in suite '{suite}'. "
            f"Tools with install specs: {', '.join(per_tool) or 'none'}"
        )
    if spec is None:
        if backend.tool_installs:
            per_tool = sorted(backend.tool_installs)
            return (
                f"Suite '{suite}' installs per tool — pass tool=. " f"Tools with install specs: {', '.join(per_tool)}"
            )
        return (
            f"Suite '{suite}' declares no install spec. "
            f"Suites with install specs: {', '.join(_suites_with_specs()) or 'none'}"
        )

    steps = steps_for(spec)
    if not execute:
        return format_plan(suite, steps, note=spec.note, url=spec.url)
    if not steps:
        return format_plan(suite, steps, note=spec.note, url=spec.url)

    if os.environ.get(_ALLOW_ENV, "0") != "1":
        return (
            f"execute=True is disabled: set {_ALLOW_ENV}=1 on the server to allow it, "
            "or run the dry-run commands yourself:\n\n" + format_plan(suite, steps, note=spec.note, url=spec.url)
        )

    results = await run_steps(steps, timeout=timeout)
    parts = [f"**Install '{suite}'** — {len(results)}/{len(steps)} step(s) attempted:", ""]
    for step, code, output in results:
        status = "ok" if code == 0 else f"FAILED (exit {code})"
        command = f"download {step.argv[0]}" if step.kind == "download" else " ".join(step.argv)
        parts.append(f"- `{command}` — {status}")
        if code != 0:
            parts.append(f"```\n{output}\n```")
            if step.elevation:
                parts.append("This step needs an elevated shell (Administrator / sudo).")

    app = get_app_ctx(ctx)
    registry = app.registry
    assert isinstance(registry, ToolRegistry), f"app context registry missing: {type(registry)}"
    await registry.detect_suite(suite)
    now_available = registry.is_available(suite)
    parts.append("")
    parts.append(f"**{suite} now available:** {now_available}")
    if spec.note:
        parts.append(f"Note: {spec.note}")
    return "\n".join(parts)
