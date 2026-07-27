"""DebugPlan executor: multi-stop capture in one tool call.

A plan reuses the interactive machinery exactly — every stop flows through
the same StopProcessor — the executor just decides which verb to issue
next and when to hand control back to the model. Waiting is tree-aware
via DebugSessionManager.wait_for_stop, because multi-session adapters
(js-debug) stop on child sessions, never the root.
"""

from __future__ import annotations

import time

from devtools_mcp.debug.models import (
    DebugPlan,
    PlanReport,
    PlanStopRow,
    SessionState,
)
from devtools_mcp.debug.session import DebugSession, DebugSessionManager

_STOP_WAIT_SECONDS = 30.0


async def apply_plan_setup(session: DebugSession, plan: DebugPlan) -> list[str]:
    """Apply a plan's breakpoints and watches to a session. Returns
    human-readable problems (empty = clean)."""
    problems: list[str] = []
    by_source: dict[str, list] = {}
    functions = []
    for spec in plan.breakpoints:
        if spec.function and not spec.source:
            functions.append(spec)
        elif spec.source:
            by_source.setdefault(spec.source, []).append(spec)
        else:
            problems.append(f"breakpoint needs source+line or function: {spec}")
    for source, specs in by_source.items():
        try:
            states = await session.set_breakpoints(source, specs)
            for state in states:
                if not state.verified and state.message:
                    problems.append(f"{source}:{state.line} not verified: {state.message}")
        except Exception as exc:  # noqa: BLE001
            problems.append(f"setBreakpoints({source}) failed: {exc}")
    if functions:
        try:
            await session.set_function_breakpoints(functions)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"function breakpoints failed: {exc}")
    for expression in plan.watches:
        error = session.add_watch(expression)
        if error:
            problems.append(f"watch '{expression}': {error}")
    return problems


async def run_plan(
    manager: DebugSessionManager,
    session_id: str,
    plan: DebugPlan,
    start_running: bool = True,
) -> PlanReport:
    """Drive the session tree through the plan. Leaves the tree live
    wherever the plan halts."""
    report = PlanReport()
    deadline = time.monotonic() + plan.time_budget_s

    if session_id not in manager.trees:
        report.halted = "no_session"
        return report

    focused = manager.resolve(session_id)
    if start_running and focused.session.state == SessionState.stopped:
        await focused.session.continue_()

    last_run_id = ""  # don't record the same stop twice
    while len(report.stops) < plan.max_stops:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            report.halted = "time_budget"
            break
        node = await manager.wait_for_stop(session_id, timeout=min(remaining, _STOP_WAIT_SECONDS))
        if node is None:
            continue  # still running inside the budget — keep waiting
        session = node.session
        if session.state == SessionState.terminated:
            report.halted = "terminated"
            break

        snapshot = session.last_snapshot
        if snapshot is not None and snapshot.run_id != last_run_id:
            last_run_id = snapshot.run_id
            location = ""
            if snapshot.threads and snapshot.threads[0].frames:
                location = snapshot.threads[0].frames[0].location
            watch_bits = [
                f"{w.expression}={w.value}" if not w.error else f"{w.expression}=<err>" for w in snapshot.watches
            ]
            report.stops.append(
                PlanStopRow(
                    stop_seq=snapshot.stop_seq,
                    reason=snapshot.stop_reason,
                    location=location,
                    watches="; ".join(watch_bits),
                    change_count=len(snapshot.changes),
                    run_id=snapshot.run_id,
                )
            )

        if plan.until:
            frame_id = None
            if snapshot is not None and snapshot.threads and snapshot.threads[0].frames:
                frame_id = snapshot.threads[0].frames[0].id
            result = await session.evaluate(plan.until, frame_id=frame_id, context="watch")
            if not result.error and _truthy(result.value):
                report.halted = "until"
                report.until_value = result.value
                break

        if len(report.stops) >= plan.max_stops:
            report.halted = "max_stops"
            break

        try:
            if plan.per_stop == "step":
                await session.step("over")
            elif plan.per_stop == "step_into":
                await session.step("into")
            elif plan.per_stop == "finish":
                await session.step("out")
            else:
                await session.continue_()
        except Exception as exc:  # noqa: BLE001
            report.halted = f"verb_failed: {exc}"
            break

    if not report.halted:
        report.halted = "max_stops"
    report.session_state = manager.resolve(session_id).session.state.value if session_id in manager.trees else "gone"
    return report


_FALSY = {"false", "0", "none", "null", "nil", "undefined", "", "''", '""', "()"}


def _truthy(value: str) -> bool:
    return value.strip().lower() not in _FALSY
