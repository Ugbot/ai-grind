"""Bounded formatting for debug snapshots, stops, and plan reports.

The no-token-flood rule: every tool returns a short summary + run_id;
drill-down happens through devtools_query on the stored frames.
"""

from __future__ import annotations

from devtools_mcp.debug.models import (
    DebugCapabilities,
    DebugSnapshot,
    PlanReport,
    Variable,
)

_MAX_SUMMARY_FRAMES = 5
_MAX_SUMMARY_VARS = 12
_MAX_SUMMARY_CHANGES = 5
_MAX_VALUE_CHARS = 80


def _clip(value: str, limit: int = _MAX_VALUE_CHARS) -> str:
    value = value.replace("\n", "\\n")
    return value if len(value) <= limit else value[: limit - 1] + "…"


def format_stop_summary(snapshot: DebugSnapshot) -> str:
    """The one-screen stop report returned by every execution verb."""
    parts: list[str] = []
    location = ""
    if snapshot.threads and snapshot.threads[0].frames:
        location = snapshot.threads[0].frames[0].location
    reason = snapshot.stop_reason or "stopped"
    parts.append(f"**Stopped ({reason})** at {location or 'unknown'}  — stop #{snapshot.stop_seq}")
    if snapshot.hit_breakpoint_ids:
        parts.append(f"Hit breakpoint(s): {', '.join(map(str, snapshot.hit_breakpoint_ids))}")
    if snapshot.exception is not None:
        header = snapshot.exception.exception_id or "exception"
        parts.append(f"**Exception:** {header} — {_clip(snapshot.exception.description, 200)}")

    if snapshot.threads and snapshot.threads[0].frames:
        parts.append("")
        parts.append("**Stack (top):**")
        for frame in snapshot.threads[0].frames[:_MAX_SUMMARY_FRAMES]:
            parts.append(f"  {frame.index}: {frame.location}")
        hidden = len(snapshot.threads[0].frames) - _MAX_SUMMARY_FRAMES
        if hidden > 0:
            parts.append(f"  … {hidden} more frames (devtools_query the run for all)")

    top_vars = [v for v in snapshot.variables if v.depth == 0][:_MAX_SUMMARY_VARS]
    if top_vars:
        parts.append("")
        parts.append("**Locals:**")
        for var in top_vars:
            type_note = f" ({var.type})" if var.type else ""
            parts.append(f"  {var.name}{type_note} = {_clip(var.value)}")
        hidden = len([v for v in snapshot.variables if v.depth == 0]) - len(top_vars)
        if hidden > 0:
            parts.append(f"  … {hidden} more (plus nested; devtools_query the run)")

    if snapshot.watches:
        parts.append("")
        parts.append("**Watches:**")
        for watch in snapshot.watches:
            if watch.error:
                parts.append(f"  {watch.expression} → error: {_clip(watch.error)}")
            else:
                parts.append(f"  {watch.expression} = {_clip(watch.value)}")

    if snapshot.changes:
        shown = snapshot.changes[:_MAX_SUMMARY_CHANGES]
        rendered = []
        for change in shown:
            if change.kind == "changed":
                rendered.append(f"`{change.path}` {_clip(change.old, 24)}→{_clip(change.new, 24)}")
            elif change.kind == "added":
                rendered.append(f"`{change.path}` +{_clip(change.new, 24)}")
            else:
                rendered.append(f"`{change.path}` removed")
        more = f" (+{len(snapshot.changes) - len(shown)} more)" if len(snapshot.changes) > len(shown) else ""
        parts.append("")
        parts.append(f"**Δ since last stop:** {len(snapshot.changes)} — " + ", ".join(rendered) + more)

    if len(snapshot.threads) > 1:
        parts.append("")
        parts.append(f"Threads: {len(snapshot.threads)} (stopped thread listed first)")

    parts.append("")
    parts.append(f"**Snapshot run_id:** `{snapshot.run_id}` — drill in with devtools_query")
    return "\n".join(parts)


def format_variables_summary(variables: list[Variable], title: str = "Variables") -> str:
    parts = [f"**{title}:** {len(variables)} node(s)"]
    for var in variables[: _MAX_SUMMARY_VARS * 2]:
        indent = "  " * (var.depth + 1)
        type_note = f" ({var.type})" if var.type else ""
        expandable = " [+]" if var.ref > 0 else ""
        parts.append(f"{indent}{var.name}{type_note} = {_clip(var.value)}{expandable}")
    hidden = len(variables) - _MAX_SUMMARY_VARS * 2
    if hidden > 0:
        parts.append(f"  … {hidden} more (devtools_query the run)")
    return "\n".join(parts)


def format_capabilities(adapter: str, caps: DebugCapabilities) -> str:
    supported = []
    missing = []
    for name, value in caps.model_dump().items():
        if name == "exception_filters":
            continue
        (supported if value else missing).append(name)
    parts = [f"**Adapter `{adapter}` capabilities:**"]
    parts.append(f"  supported: {', '.join(sorted(supported)) or 'none'}")
    if missing:
        parts.append(f"  not supported: {', '.join(sorted(missing))}")
    if caps.exception_filters:
        parts.append(f"  exception filters: {', '.join(caps.exception_filters)}")
    return "\n".join(parts)


def format_plan_report(report: PlanReport) -> str:
    parts = [f"**Plan finished** — {len(report.stops)} stop(s), halted by: {report.halted}"]
    if report.until_value:
        parts.append(f"`until` evaluated truthy: {_clip(report.until_value)}")
    parts.append("")
    parts.append("| # | reason | location | watches | Δ | run_id |")
    parts.append("|---|--------|----------|---------|---|--------|")
    for row in report.stops:
        parts.append(
            f"| {row.stop_seq} | {row.reason} | {_clip(row.location, 48)} "
            f"| {_clip(row.watches, 48)} | {row.change_count} | `{row.run_id[:8]}` |"
        )
    parts.append("")
    parts.append(f"Session state: {report.session_state} — continue interactively with debug()")
    return "\n".join(parts)
