"""StopProcessor: turn one stop into one stored DebugSnapshot.

On every stop the server does the full state walk (threads → stack →
scopes → variables → watches), diffs against the previous stop, and
stores the snapshot as a queryable run — so the model never issues
threads/stackTrace/scopes/variables calls itself.
"""

from __future__ import annotations

import uuid
from collections import deque

from devtools_mcp.debug.models import (
    MAX_CHILDREN_PER_CONTAINER,
    MAX_FRAMES_PER_STOP,
    MAX_THREADS_CAPTURED,
    MAX_VARIABLE_DEPTH,
    MAX_VARIABLE_NODES,
    DebugSnapshot,
    ExceptionInfo,
    StopInfo,
    VarChange,
    Variable,
    WatchResult,
)
from devtools_mcp.debug.session import DebugSession

_SCOPE_PRIORITY = ("locals", "local", "arguments", "globals")
_MAX_DIFF_CHANGES = 64


def _scope_slug(name: str) -> str:
    return name.strip().lower()


async def expand_variables(
    session: DebugSession,
    root_ref: int,
    scope: str,
    max_depth: int = MAX_VARIABLE_DEPTH,
    max_nodes: int = MAX_VARIABLE_NODES,
) -> list[Variable]:
    """Breadth-first expansion of a variables container into flattened,
    path-keyed nodes. Bounded by depth and total node count."""
    collected: list[Variable] = []
    queue: deque[tuple[int, str, int]] = deque([(root_ref, "", 0)])
    while queue and len(collected) < max_nodes:
        ref, prefix, depth = queue.popleft()
        try:
            children = await session.variables(ref, count=MAX_CHILDREN_PER_CONTAINER)
        except Exception:  # noqa: BLE001 — a bad container must not sink the snapshot
            continue
        for child in children:
            if len(collected) >= max_nodes:
                break
            path = f"{prefix}.{child.name}" if prefix else child.name
            collected.append(
                Variable(
                    path=path,
                    name=child.name,
                    type=child.type,
                    value=child.value,
                    ref=child.ref,
                    depth=depth,
                    scope=scope,
                )
            )
            if child.ref > 0 and depth < max_depth:
                queue.append((child.ref, path, depth + 1))
    return collected


def diff_snapshots(prev: DebugSnapshot | None, cur: DebugSnapshot) -> list[VarChange]:
    """Variable + watch deltas between two consecutive stops, by path."""
    if prev is None:
        return []
    before: dict[str, str] = {f"{v.scope}:{v.path}": v.value for v in prev.variables}
    before.update({f"watch:{w.expression}": w.value for w in prev.watches if not w.error})
    after: dict[str, str] = {f"{v.scope}:{v.path}": v.value for v in cur.variables}
    after.update({f"watch:{w.expression}": w.value for w in cur.watches if not w.error})

    changes: list[VarChange] = []
    for key, new_value in after.items():
        if len(changes) >= _MAX_DIFF_CHANGES:
            break
        display = key.split(":", 1)[1]
        if key not in before:
            changes.append(VarChange(path=display, kind="added", new=new_value))
        elif before[key] != new_value:
            changes.append(VarChange(path=display, kind="changed", old=before[key], new=new_value))
    for key, old_value in before.items():
        if len(changes) >= _MAX_DIFF_CHANGES:
            break
        if key not in after:
            changes.append(VarChange(path=key.split(":", 1)[1], kind="removed", old=old_value))
    return changes


class StopProcessor:
    """Builds and stores one DebugSnapshot from a stopped session. Every
    phase is tolerant: a failed sub-query degrades that section, never
    the whole capture."""

    async def process(self, session: DebugSession, stop: StopInfo) -> DebugSnapshot:
        session.stop_seq += 1
        snapshot = DebugSnapshot(
            run_id=str(uuid.uuid4()),
            tool="stop",
            binary=getattr(session, "binary", "") or "unknown",
            session_id=session.session_id,
            adapter=session.adapter_name,
            node_id=getattr(getattr(session, "node", None), "node_id", "") or session.session_id,
            stop_seq=session.stop_seq,
            stop_reason=stop.reason,
            thread_id=stop.thread_id,
            hit_breakpoint_ids=stop.hit_breakpoint_ids,
            batch_id=session.session_id,
            parent_run_id=session.last_snapshot.run_id if session.last_snapshot else "",
            raw_output=session.output_tail(),
        )

        # Threads + stack of the stopped thread (full), others get 1 frame.
        try:
            threads = await session.threads()
        except Exception:  # noqa: BLE001
            threads = []
        stopped_tid = stop.thread_id if stop.thread_id is not None else (threads[0].thread_id if threads else None)
        for thread in threads[:MAX_THREADS_CAPTURED]:
            is_stopped_thread = thread.thread_id == stopped_tid
            levels = MAX_FRAMES_PER_STOP if is_stopped_thread else 1
            try:
                thread.frames = await session.stack_trace(thread.thread_id, levels=levels)
            except Exception:  # noqa: BLE001
                thread.frames = []
            thread.stop_reason = stop.reason if is_stopped_thread else ""
        # Stopped thread first — formatters and df builders rely on it.
        threads.sort(key=lambda t: t.thread_id != stopped_tid)
        snapshot.threads = threads

        # Variables: priority scopes of the top frame, flattened.
        top_frame = None
        if threads and threads[0].frames:
            top_frame = threads[0].frames[0]
        if top_frame is not None:
            try:
                scopes = await session.scopes(top_frame.id)
            except Exception:  # noqa: BLE001
                scopes = []
            scopes = [s for s in scopes if not s.expensive]
            scopes.sort(
                key=lambda s: (
                    _SCOPE_PRIORITY.index(_scope_slug(s.name))
                    if _scope_slug(s.name) in _SCOPE_PRIORITY
                    else len(_SCOPE_PRIORITY)
                )
            )
            budget = MAX_VARIABLE_NODES
            for scope in scopes:
                if budget <= 0:
                    break
                variables = await expand_variables(session, scope.ref, scope=_scope_slug(scope.name), max_nodes=budget)
                snapshot.variables.extend(variables)
                budget -= len(variables)

        # Watches — evaluated server-side, errors captured not raised.
        frame_id = top_frame.id if top_frame is not None else None
        for expression in session.watches:
            try:
                result = await session.evaluate(expression, frame_id=frame_id, context="watch")
                snapshot.watches.append(
                    WatchResult(
                        expression=expression,
                        value=result.value,
                        type=result.type,
                        error=result.error,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                snapshot.watches.append(WatchResult(expression=expression, error=str(exc)))

        # Breakpoint states as last confirmed.
        for states in session.breakpoints.values():
            snapshot.breakpoints.extend(states)

        if stop.reason == "exception":
            snapshot.exception = await self._exception_info(session, stopped_tid, stop)

        snapshot.changes = diff_snapshots(session.last_snapshot, snapshot)
        session.last_snapshot = snapshot

        sink = getattr(session, "snapshot_sink", None)
        if sink is not None:
            try:
                sink(snapshot)
            except Exception as exc:  # noqa: BLE001 — storage failure must not wedge the session
                session.append_output(f"[snapshot store failed: {exc}]")
        return snapshot

    async def _exception_info(self, session: DebugSession, thread_id: int | None, stop: StopInfo) -> ExceptionInfo:
        info = ExceptionInfo(description=stop.description)
        conn = getattr(session, "conn", None)
        if conn is None or thread_id is None:
            return info
        try:
            body = await conn.request("exceptionInfo", {"threadId": thread_id})
        except Exception:  # noqa: BLE001 — optional request, many adapters lack it
            return info
        info.exception_id = body.get("exceptionId", "")
        info.description = body.get("description", "") or info.description
        info.break_mode = body.get("breakMode", "")
        details = body.get("details") or {}
        info.stack = details.get("stackTrace", "") or ""
        return info
