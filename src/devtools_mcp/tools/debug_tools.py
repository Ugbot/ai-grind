"""Debug tools: debug_start, debug, debug_inspect, debug_stop.

One tool surface for every language. Sessions route through the unified
debug layer (devtools_mcp.debug): DAP adapters (debugpy, lldb-dap,
js-debug, kotlin, java via jdt.ls) today, non-DAP implementations (SAP
ADT) behind the same DebugSession interface.

Server-side heavy lifting: every stop auto-captures a snapshot (stack +
locals + watches + diff vs the previous stop) stored as a queryable run,
execution verbs return that summary in the same call, so stepping through
code is one round trip per stop.
"""

from __future__ import annotations

import re
import uuid

from mcp.server.fastmcp import Context

from devtools_mcp.debug.adapters import resolve_adapter, resolve_session_provider
from devtools_mcp.debug.dap_session import DapSession, UnsupportedCapability
from devtools_mcp.debug.formatters import (
    format_capabilities,
    format_plan_report,
    format_stop_summary,
    format_variables_summary,
)
from devtools_mcp.debug.models import (
    AttachConfig,
    BreakpointSpec,
    DebugPlan,
    DebugSnapshot,
    LaunchConfig,
    SessionState,
    StopInfo,
    ThreadInfo,
    Variable,
    WatchResult,
)
from devtools_mcp.debug.plans import apply_plan_setup, run_plan
from devtools_mcp.debug.protocol import AdapterCrashed
from devtools_mcp.debug.session import DebugSession, DebugSessionManager, SessionNode
from devtools_mcp.debug.snapshot import StopProcessor, diff_snapshots, expand_variables
from devtools_mcp.models import StackSample
from devtools_mcp.server import get_app_ctx, mcp

_RUN_STATES = {SessionState.stopped, SessionState.terminated}


# ---------------------------------------------------------------------------
# Unified (DAP) path
# ---------------------------------------------------------------------------


def _bp_spec_from_dict(raw: dict) -> BreakpointSpec:
    return BreakpointSpec(
        source=str(raw.get("source", "") or raw.get("file", "")),
        line=raw.get("line"),
        function=raw.get("function"),
        condition=raw.get("condition"),
        hit_condition=raw.get("hit_condition") or raw.get("hitCondition"),
        log_message=raw.get("log_message") or raw.get("logMessage"),
    )


def _bp_spec_from_location(
    location: str,
    condition: str | None,
    hit_condition: str | None,
    log_message: str | None,
) -> BreakpointSpec:
    """'file:line' → source breakpoint; anything else → function breakpoint
    (same disambiguation the LLDB path uses: ':<digits>' at the end wins,
    so C++ 'Class::method' stays a function name)."""
    file_line = re.fullmatch(r"(.+):(\d+)", location) if not location.startswith("0x") else None
    if file_line:
        return BreakpointSpec(
            source=file_line.group(1),
            line=int(file_line.group(2)),
            condition=condition,
            hit_condition=hit_condition,
            log_message=log_message,
        )
    return BreakpointSpec(
        function=location,
        condition=condition,
        hit_condition=hit_condition,
        log_message=log_message,
    )


async def _await_stop(manager: DebugSessionManager, session_id: str, timeout: float) -> str:
    """Tree-aware wait for the next stop; returns its auto-captured summary.
    Multi-session adapters (js-debug) stop on child sessions, so this waits
    on the whole tree, not one session's state."""
    node = await manager.wait_for_stop(session_id, timeout=timeout)
    if node is None:
        return (
            f"**Running**: no stop within {timeout:.0f}s. "
            'Use debug(action="pause") to interrupt, or debug_inspect(what="output") for program output.'
        )
    session = node.session
    if session.state == SessionState.terminated:
        exit_code = getattr(session, "_exit_code", None)
        code_note = f" (exit code {exit_code})" if exit_code is not None else ""
        tail = session.output_tail()
        # The root holds launch output when children did the running.
        if not tail and session_id in manager.trees:
            tail = manager.trees[session_id].session.output_tail()
        tail_block = f"\n\n**Output tail:**\n```\n{tail}\n```" if tail else ""
        return f"**Process terminated**{code_note}.{tail_block}"
    if session.last_snapshot is not None:
        summary = format_stop_summary(session.last_snapshot)
        if node.node_id != session_id:
            summary = f"(stopped in child `{node.node_id}`{': ' + node.label if node.label else ''})\n" + summary
        return summary
    return "**Stopped** (no snapshot captured)"


def _stopped_thread_id(session: DebugSession) -> int | None:
    if session.selected_thread_id is not None:
        return session.selected_thread_id
    if session.last_stop is not None and session.last_stop.thread_id is not None:
        return session.last_stop.thread_id
    if session.last_snapshot is not None and session.last_snapshot.threads:
        return session.last_snapshot.threads[0].thread_id
    return None


def _current_frame_id(session: DebugSession, frame_index: int | None = None) -> int | None:
    """Frame handle for evaluation: explicit index > selected frame > top frame."""
    snapshot = session.last_snapshot
    frames = snapshot.threads[0].frames if snapshot is not None and snapshot.threads else []
    if frame_index is not None:
        if 0 <= frame_index < len(frames):
            return frames[frame_index].id
        return None
    if session.selected_frame_id is not None:
        return session.selected_frame_id
    return frames[0].id if frames else None


async def _resolve_variable_container(session: DebugSession, path: str, frame_id: int | None) -> tuple[int, str] | str:
    """Walk scopes → variables along a dotted path. Returns (parent_ref,
    leaf_name) for the final segment, or an error string."""
    if frame_id is None:
        return "no stopped frame to resolve variables in"
    segments = [s for s in path.split(".") if s]
    if not segments:
        return f"bad variable path: {path!r}"
    scopes = await session.scopes(frame_id)
    parent_ref = None
    for scope in scopes:
        children = await session.variables(scope.ref)
        if any(v.name == segments[0] for v in children):
            parent_ref = scope.ref
            break
    if parent_ref is None:
        return f"variable '{segments[0]}' not found in any scope"
    current_ref = parent_ref
    for depth, segment in enumerate(segments):
        children = await session.variables(current_ref)
        match = next((v for v in children if v.name == segment), None)
        if match is None:
            return f"'{segment}' not found under '{'.'.join(segments[:depth]) or 'scope'}'"
        if depth == len(segments) - 1:
            return (current_ref, segment)
        if match.ref <= 0:
            return f"'{segment}' is not expandable (no children)"
        current_ref = match.ref
    return f"could not resolve path {path!r}"


def _new_inspect_snapshot(session: DebugSession, what: str) -> DebugSnapshot:
    return DebugSnapshot(
        run_id=str(uuid.uuid4()),
        tool=what,
        binary=getattr(session, "binary", "") or "unknown",
        session_id=session.session_id,
        adapter=session.adapter_name,
        node_id=getattr(getattr(session, "node", None), "node_id", "") or session.session_id,
        stop_seq=session.stop_seq,
        batch_id=session.session_id,
    )


def _session_header(session_id: str, session: DebugSession, node_count: int = 1) -> str:
    caps = session.capabilities
    highlights = []
    if caps.conditional_breakpoints:
        highlights.append("conditional bps")
    if caps.log_points:
        highlights.append("logpoints")
    if caps.set_variable:
        highlights.append("set_variable")
    if caps.read_memory:
        highlights.append("memory")
    if caps.disassemble:
        highlights.append("disassembly")
    caps_note = f", {', '.join(highlights)}" if highlights else ""
    children = f"\n**Child sessions:** {node_count - 1}" if node_count > 1 else ""
    return (
        f"**Debug session started** ({session.adapter_name}{caps_note})\n"
        f"**Session ID:** `{session_id}`{children}\n\n"
    )


async def _session_start(  # noqa: PLR0912  # launch/attach/plan routing is a flat decision table
    ctx: Context,
    session: DebugSession,
    target: str,
    args: list[str] | None,
    pid: int | None,
    attach_host: str,
    attach_port: int | None,
    cwd: str,
    env: dict[str, str] | None,
    stop_on_entry: bool,
    breakpoints: list[dict] | None,
    watches: list[str] | None,
    plan: dict | None,
    extra: dict | None,
    timeout: float,
    workspace_id: str | None,
) -> str:
    app = get_app_ctx(ctx)
    manager: DebugSessionManager = app.get_debug_manager()

    session_id = session.session_id
    node = manager.register_root(session)
    session.node = node

    problems: list[str] = []
    for raw in breakpoints or []:
        spec = _bp_spec_from_dict(raw)
        if not spec.source and not spec.function:
            problems.append(f"breakpoint needs source+line or function: {raw}")
            continue
        await session.add_breakpoint(spec)
    for expression in watches or []:
        error = session.add_watch(expression)
        if error:
            problems.append(f"watch '{expression}': {error}")

    plan_obj: DebugPlan | None = None
    if plan:
        try:
            plan_obj = DebugPlan.model_validate(plan)
        except Exception as exc:  # noqa: BLE001
            await manager.stop_tree(session_id)
            return f"Invalid plan: {exc}"
        problems.extend(await apply_plan_setup(session, plan_obj))

    attaching = pid is not None or attach_port is not None
    try:
        if attaching:
            await session.attach(
                AttachConfig(
                    pid=pid,
                    host=attach_host,
                    port=attach_port,
                    program=target,
                    extra=extra or {},
                )
            )
        else:
            await session.launch(
                LaunchConfig(
                    program=target,
                    args=args or [],
                    cwd=cwd,
                    env=env or {},
                    stop_on_entry=stop_on_entry,
                    extra=extra or {},
                )
            )
    except Exception as exc:  # noqa: BLE001  # surface the failure, clean up the tree
        await manager.stop_tree(session_id)
        return f"Failed to start {session.adapter_name} session: {exc}"

    header = _session_header(session_id, session, len(node.walk()))
    if problems:
        header += "**Setup issues:**\n" + "\n".join(f"- {p}" for p in problems) + "\n\n"

    if plan_obj is not None:
        report = await run_plan(manager, session_id, plan_obj)
        return header + format_plan_report(report)

    expect_stop = stop_on_entry or bool(breakpoints)
    wait = timeout if expect_stop else min(timeout, 3.0)
    return header + await _await_stop(manager, session_id, wait)


async def _dap_action(  # noqa: PLR0911  # pLR0912  # one flat verb table by design
    session: DebugSession,
    manager: DebugSessionManager,
    node: SessionNode,
    session_id: str,
    action: str,
    location: str | None,
    condition: str | None,
    hit_condition: str | None,
    log_message: str | None,
    expression: str | None,
    breakpoint_id: int | None,
    variable: str | None,
    value: str | None,
    thread_id: int | None,
    frame_index: int | None,
    instruction: bool,
    raw_command: str | None,
    filters: list[str] | None,
    watch: str | None,
    plan: dict | None,
    timeout: float,
) -> str:
    granularity = "instruction" if instruction else "statement"

    if action in ("run", "continue"):
        await session.continue_(thread_id)
        return await _await_stop(manager, session_id, timeout)

    if action == "pause":
        await session.pause(thread_id)
        return await _await_stop(manager, session_id, min(timeout, 10.0))

    if action == "step":
        await session.step("into", thread_id, granularity)
        return await _await_stop(manager, session_id, timeout)

    if action == "next":
        await session.step("over", thread_id, granularity)
        return await _await_stop(manager, session_id, timeout)

    if action == "finish":
        await session.step("out", thread_id, granularity)
        return await _await_stop(manager, session_id, timeout)

    if action == "kill":
        count = await manager.stop_tree(session_id, terminate=True)
        return f"**Session terminated:** `{session_id}` ({count} node(s) disconnected)"

    if action == "breakpoint":
        if not location:
            return "Missing `location` for breakpoint action (file:line or function name)"
        spec = _bp_spec_from_location(location, condition, hit_condition, log_message)
        states = await session.add_breakpoint(spec)
        lines = [f"**Breakpoint set:** {location}"]
        for state in states:
            mark = "verified" if state.verified else f"pending{': ' + state.message if state.message else ''}"
            where = state.function or f"{state.source}:{state.line}"
            lines.append(f"  [{state.id}] {where}, {mark}")
        return "\n".join(lines)

    if action == "breakpoint_delete":
        if breakpoint_id is None:
            return "Missing `breakpoint_id` for breakpoint_delete action"
        removed = await session.remove_breakpoint(breakpoint_id)
        return (
            f"**Breakpoint {breakpoint_id} deleted**"
            if removed
            else f"No breakpoint with id {breakpoint_id} (see debug_inspect what='breakpoints')"
        )

    if action == "exception_breakpoints":
        await session.set_exception_breakpoints(filters or [])
        active = ", ".join(filters) if filters else "none"
        available = ", ".join(session.capabilities.exception_filters) or "none"
        return f"**Exception breakpoints set:** {active} (available: {available})"

    if action == "watch_add":
        expr = watch or expression
        if not expr:
            return "Missing `watch` (or `expression`) for watch_add"
        error = session.add_watch(expr)
        if error:
            return f"Watch rejected: {error}"
        note = ""
        if session.state == SessionState.stopped:
            result = await session.evaluate(expr, _current_frame_id(session), context="watch")
            note = f" = {result.value}" if not result.error else f" (currently: {result.error})"
        return f"**Watch added:** `{expr}`{note}: evaluated at every stop ({len(session.watches)}/{16})"

    if action == "watch_remove":
        expr = watch or expression
        if not expr:
            return "Missing `watch` (or `expression`) for watch_remove"
        return f"**Watch removed:** `{expr}`" if session.remove_watch(expr) else f"No watch `{expr}`"

    if action == "watch_list":
        if not session.watches:
            return "No watches. Add with debug(action='watch_add', watch='expr')."
        return "**Watches:**\n" + "\n".join(f"  - `{w}`" for w in session.watches)

    if action == "watchpoint":
        return (
            "Data breakpoints (watchpoints) are not yet supported for unified debug sessions. "
            "Use a conditional breakpoint, or a watch (action='watch_add') to track the value at stops."
        )

    if action == "set_variable":
        if not variable or value is None:
            return "Missing `variable` (dotted path) and/or `value` for set_variable"
        frame_id = _current_frame_id(session, frame_index)
        resolved = await _resolve_variable_container(session, variable, frame_id)
        if isinstance(resolved, str):
            return f"set_variable failed: {resolved}"
        parent_ref, leaf = resolved
        updated = await session.set_variable(parent_ref, leaf, value)
        return f"**Variable set:** `{variable}` = {updated.value}" + (f" ({updated.type})" if updated.type else "")

    if action == "thread_select":
        if thread_id is None:
            return "Missing `thread_id` for thread_select action"
        session.selected_thread_id = thread_id
        session.selected_frame_id = None
        return f"**Selected thread {thread_id}** (used for stepping and evaluation)"

    if action == "frame_select":
        if frame_index is None:
            return "Missing `frame_index` for frame_select action"
        frame_id = _current_frame_id(session, frame_index)
        if frame_id is None:
            return f"No frame {frame_index} in the current stop (see debug_inspect what='stack')"
        session.selected_frame_id = frame_id
        snapshot = session.last_snapshot
        frames = snapshot.threads[0].frames if snapshot and snapshot.threads else []
        location_note = frames[frame_index].location if frame_index < len(frames) else ""
        return f"**Selected frame {frame_index}** {location_note}"

    if action == "command":
        if not raw_command:
            return "Missing `raw_command` for command action"
        output = await session.raw_command(raw_command)
        return f"**{raw_command}**\n\n{output or '(no output)'}"

    if action == "plan":
        if not plan:
            return "Missing `plan` dict for plan action"
        try:
            plan_obj = DebugPlan.model_validate(plan)
        except Exception as exc:  # noqa: BLE001
            return f"Invalid plan: {exc}"
        problems = await apply_plan_setup(session, plan_obj)
        report = await run_plan(manager, session_id, plan_obj)
        prefix = ("**Setup issues:**\n" + "\n".join(f"- {p}" for p in problems) + "\n\n") if problems else ""
        return prefix + format_plan_report(report)

    valid = (
        "run, continue, pause, step, next, finish, kill, breakpoint, breakpoint_delete, "
        "exception_breakpoints, watch_add, watch_remove, watch_list, watchpoint, set_variable, "
        "thread_select, frame_select, command, plan"
    )
    return f"Unknown action: `{action}`. Valid: {valid}"


async def _dap_inspect(  # noqa: PLR0911, PLR0912  # pLR0915  # one flat inspect table by design
    ctx: Context,
    session: DebugSession,
    session_id: str,
    what: str,
    expression: str | None,
    address: str | None,
    count: int | None,
    frame_index: int | None,
    expand: str | None,
    against: str | None,
    thread_id: int | None,
    workspace_id: str | None,
) -> str:
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    if what == "capabilities":
        nodes = app.get_debug_manager().children_of(session_id)
        tree_note = "\nChildren: " + ", ".join(n.node_id for n in nodes) if nodes else ""
        return format_capabilities(session.adapter_name, session.capabilities) + tree_note

    if what == "snapshot":
        stop = session.last_stop or StopInfo(reason="inspect")
        snapshot = await StopProcessor().process(session, stop)
        return format_stop_summary(snapshot)

    snapshot = _new_inspect_snapshot(session, what)

    if what in ("stack", "backtrace"):
        snapshot.tool = "stack"
        tid = thread_id if thread_id is not None else _stopped_thread_id(session)
        if tid is None:
            return "No stopped thread, stack is only available while stopped."
        frames = await session.stack_trace(tid, levels=count or 32)
        snapshot.threads = [ThreadInfo(thread_id=tid, stopped=True, frames=frames)]
        ws.store_run(snapshot)
        lines = [f"**Stack** (thread {tid}, {len(frames)} frame(s)):"]
        lines += [f"  {f.index}: {f.location}" for f in frames[:15]]
        if len(frames) > 15:
            lines.append(f"  … {len(frames) - 15} more")
        lines.append(f"\n**run_id:** `{snapshot.run_id}`")
        return "\n".join(lines)

    if what == "variables":
        frame_id = _current_frame_id(session, frame_index)
        if frame_id is None:
            return "No stopped frame, variables are only available while stopped."
        if expand:
            resolved = await _resolve_variable_container(session, expand, frame_id)
            if isinstance(resolved, str):
                return f"variables expand failed: {resolved}"
            parent_ref, leaf = resolved
            children = await session.variables(parent_ref)
            target = next((v for v in children if v.name == leaf), None)
            if target is None or target.ref <= 0:
                return f"'{expand}' has no expandable children"
            snapshot.variables = await expand_variables(session, target.ref, scope=expand)
            title = f"Variables under {expand}"
        else:
            scopes = await session.scopes(frame_id)
            for scope in scopes:
                if scope.expensive:
                    continue
                snapshot.variables.extend(await expand_variables(session, scope.ref, scope=scope.name.lower()))
            title = "Variables"
        ws.store_run(snapshot)
        return format_variables_summary(snapshot.variables, title) + f"\n\n**run_id:** `{snapshot.run_id}`"

    if what == "registers":
        frame_id = _current_frame_id(session, frame_index)
        if frame_id is None:
            return "No stopped frame, registers are only available while stopped."
        scopes = await session.scopes(frame_id)
        register_scope = next((s for s in scopes if "register" in s.name.lower()), None)
        if register_scope is None:
            return f"Adapter {session.adapter_name} exposes no register scope."
        snapshot.variables = await expand_variables(session, register_scope.ref, scope="registers")
        ws.store_run(snapshot)
        return format_variables_summary(snapshot.variables, "Registers") + f"\n\n**run_id:** `{snapshot.run_id}`"

    if what == "watches":
        frame_id = _current_frame_id(session, frame_index)
        for expr in session.watches:
            result = await session.evaluate(expr, frame_id, context="watch")
            snapshot.watches.append(
                WatchResult(expression=expr, value=result.value, type=result.type, error=result.error)
            )
        ws.store_run(snapshot)
        if not snapshot.watches:
            return "No watches. Add with debug(action='watch_add', watch='expr')."
        lines = ["**Watches:**"]
        for w in snapshot.watches:
            lines.append(f"  {w.expression} → {w.error or w.value}")
        lines.append(f"\n**run_id:** `{snapshot.run_id}`")
        return "\n".join(lines)

    if what == "threads":
        threads = await session.threads()
        for thread in threads[:16]:
            try:
                thread.frames = await session.stack_trace(thread.thread_id, levels=1)
            except Exception:  # noqa: BLE001  # running threads may refuse stackTrace
                thread.frames = []
        snapshot.threads = threads
        ws.store_run(snapshot)
        lines = [f"**Threads:** {len(threads)}"]
        for t in threads[:16]:
            top = f" @ {t.frames[0].location}" if t.frames else ""
            lines.append(f"  {t.thread_id}: {t.name}{top}")
        lines.append(f"\n**run_id:** `{snapshot.run_id}`")
        return "\n".join(lines)

    if what == "breakpoints":
        for states in session.breakpoints.values():
            snapshot.breakpoints.extend(states)
        ws.store_run(snapshot)
        if not snapshot.breakpoints:
            return "No breakpoints set."
        lines = ["**Breakpoints:**"]
        for bp in snapshot.breakpoints:
            where = bp.function or f"{bp.source}:{bp.line}"
            mark = "verified" if bp.verified else "pending"
            extras = []
            if bp.condition:
                extras.append(f"if {bp.condition}")
            if bp.hit_condition:
                extras.append(f"hits {bp.hit_condition}")
            if bp.log_message:
                extras.append(f"log: {bp.log_message}")
            extra_note = f" ({'; '.join(extras)})" if extras else ""
            lines.append(f"  [{bp.id}] {where}, {mark}{extra_note}")
        lines.append(f"\n**run_id:** `{snapshot.run_id}`")
        return "\n".join(lines)

    if what == "expression":
        if not expression:
            return "Missing `expression` for expression evaluation"
        result = await session.evaluate(expression, _current_frame_id(session, frame_index), context="repl")
        if result.error:
            return f"`{expression}` → error: {result.error}"
        snapshot.variables = [
            Variable(path=expression, name=expression, type=result.type, value=result.value, ref=result.ref)
        ]
        if result.ref > 0:
            snapshot.variables.extend(await expand_variables(session, result.ref, scope=expression))
        ws.store_run(snapshot)
        type_note = f" ({result.type})" if result.type else ""
        return f"`{expression}` = {result.value}{type_note}\n\n**run_id:** `{snapshot.run_id}`"

    if what == "memory":
        addr = address or expression
        if not addr:
            return "Missing `address` (or `expression`) for memory inspection"
        data = await session.read_memory(addr, count or 64)
        lines = []
        for offset in range(0, len(data), 16):
            chunk = data[offset : offset + 16]
            hex_part = " ".join(f"{b:02x}" for b in chunk)
            ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
            lines.append(f"{offset:08x}  {hex_part:<47}  {ascii_part}")
        snapshot.raw_output = "\n".join(lines)
        ws.store_run(snapshot)
        return (
            f"**Memory at {addr}** ({len(data)} bytes):\n"
            f"```\n{snapshot.raw_output}\n```\n**run_id:** `{snapshot.run_id}`"
        )

    if what == "disassemble":
        addr = address or expression
        if not addr:
            return "Missing `address` for disassembly (a memory reference, e.g. from a frame)"
        instructions = await session.disassemble(addr, count or 16)
        rendered = [f"{i.address}  {i.text}" for i in instructions]
        snapshot.raw_output = "\n".join(rendered)
        ws.store_run(snapshot)
        return f"**Disassembly at {addr}:**\n```\n{snapshot.raw_output}\n```\n**run_id:** `{snapshot.run_id}`"

    if what == "output":
        snapshot.raw_output = "\n".join(session.output)
        ws.store_run(snapshot)
        tail = session.output_tail(40) or "(no output captured)"
        return f"**Program output (tail):**\n```\n{tail}\n```\n**run_id:** `{snapshot.run_id}`"

    if what == "diff":
        current = session.last_snapshot
        if current is None:
            return "No stop snapshot yet, diff needs at least one stop."
        prev_id = against or current.parent_run_id
        if not prev_id or prev_id == "previous":
            prev_id = current.parent_run_id
        if not prev_id:
            return "No previous snapshot to diff against (first stop)."
        try:
            previous = ws.get_run(prev_id)
        except KeyError:
            return f"Run '{prev_id}' not found."
        if not isinstance(previous, DebugSnapshot):
            return f"Run '{prev_id}' is not a debug snapshot."
        snapshot.changes = diff_snapshots(previous, current)
        snapshot.parent_run_id = previous.run_id
        ws.store_run(snapshot)
        if not snapshot.changes:
            return f"No variable changes between stop #{previous.stop_seq} and stop #{current.stop_seq}."
        lines = [f"**Δ between stop #{previous.stop_seq} and stop #{current.stop_seq}:** {len(snapshot.changes)}"]
        for change in snapshot.changes[:20]:
            if change.kind == "changed":
                lines.append(f"  ~ {change.path}: {change.old} → {change.new}")
            elif change.kind == "added":
                lines.append(f"  + {change.path} = {change.new}")
            else:
                lines.append(f"  - {change.path} (was {change.old})")
        if len(snapshot.changes) > 20:
            lines.append(f"  … {len(snapshot.changes) - 20} more")
        lines.append(f"\n**run_id:** `{snapshot.run_id}`")
        return "\n".join(lines)

    valid = (
        "stack, variables, watches, threads, breakpoints, registers, expression, "
        "memory, disassemble, output, diff, capabilities, snapshot"
    )
    return f"Unknown inspect target: `{what}`. Valid: {valid}"


# ---------------------------------------------------------------------------
# The four tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def debug_start(
    ctx: Context,
    program: str = "",
    binary: str = "",
    args: list[str] | None = None,
    pid: int | None = None,
    attach_host: str = "",
    attach_port: int | None = None,
    language: str = "",
    adapter: str = "",
    cwd: str = "",
    working_dir: str | None = None,
    env: dict[str, str] | None = None,
    stop_on_entry: bool = False,
    breakpoints: list[dict] | None = None,
    watches: list[str] | None = None,
    plan: dict | None = None,
    extra: dict | None = None,
    timeout: float = 30.0,
    workspace_id: str | None = None,
) -> str:
    """Start a debug session: launch a program or attach to a running one.

    One interface across languages, the adapter is picked from `adapter`,
    `language`, or sniffed from the program (.py → debugpy, native binary →
    lldb, .js → js-debug, ...). Every stop auto-captures a snapshot (stack,
    locals, watches, diff vs previous stop) stored as a queryable run.

    Args:
        program: Path to the program/script to launch (alias: binary)
        binary: Legacy alias for program
        args: Program arguments
        pid: Attach to this process id instead of launching
        attach_host: Host for socket attach (with attach_port)
        attach_port: Attach to a debug listener port (debugpy --listen, JDWP,
                     node --inspect) instead of launching
        language: Pick adapter by language ("python", "c", "rust", ...)
        adapter: Pick adapter by name ("debugpy", "lldb-dap", ...)
        cwd: Working directory for the debuggee (alias: working_dir)
        working_dir: Legacy alias for cwd
        env: Extra environment variables for the debuggee
        stop_on_entry: Stop at the first line
        breakpoints: Breakpoints to set before launch: [{source, line,
                     condition?, hit_condition?, log_message?} | {function}]
        watches: Watch expressions evaluated at every stop (max 16)
        plan: Debug plan to run immediately: {breakpoints, watches, max_stops,
              per_stop: continue|step|step_into|finish, until, time_budget_s}
        extra: Adapter-specific launch fields (e.g. {"python": "...",
               "module": "pkg.mod", "main_class": "..."})
        timeout: Seconds to wait for the first stop
        workspace_id: Workspace to store snapshots in
    """
    target = program or binary
    attaching = pid is not None or attach_port is not None
    if not target and not attaching:
        return "Provide program= to launch, or pid=/attach_port= to attach."

    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    manager: DebugSessionManager = app.get_debug_manager()
    session_id = str(uuid.uuid4())

    def sink(snap: DebugSnapshot) -> str:
        return ws.store_run(snap)

    # Non-DAP providers (e.g. the ABAP plugin) build a DebugSession directly;
    # otherwise resolve a DAP adapter and wrap it in a DapSession.
    provider = resolve_session_provider(target, language, adapter)
    if provider is not None:
        session: DebugSession = provider.factory(session_id, manager, sink)
    else:
        adapter_spec = None
        if adapter or language:
            try:
                adapter_spec = resolve_adapter(target, language, adapter)
            except KeyError as exc:
                return str(exc)
        elif target:
            try:
                adapter_spec = resolve_adapter(program=target)
            except KeyError as exc:
                return str(exc)
        if adapter_spec is None:
            return "Cannot infer an adapter for attach. Pass adapter= or language=."
        session = DapSession(session_id, adapter_spec, manager, snapshot_sink=sink)

    return await _session_start(
        ctx,
        session,
        target,
        args,
        pid,
        attach_host,
        attach_port,
        cwd or (working_dir or ""),
        env,
        stop_on_entry,
        breakpoints,
        watches,
        plan,
        extra,
        timeout,
        workspace_id,
    )


@mcp.tool()
async def debug(
    ctx: Context,
    session_id: str,
    action: str,
    location: str | None = None,
    condition: str | None = None,
    hit_condition: str | None = None,
    log_message: str | None = None,
    expression: str | None = None,
    breakpoint_id: int | None = None,
    variable: str | None = None,
    value: str | None = None,
    thread_id: int | None = None,
    frame_index: int | None = None,
    instruction: bool = False,
    raw_command: str | None = None,
    filters: list[str] | None = None,
    watch: str | None = None,
    plan: dict | None = None,
    child: str | None = None,
    timeout: float = 15.0,
) -> str:
    """Execute a debug action. Execution verbs (run/continue/step/next/
    finish) wait for the next stop and return its auto-captured summary,
    stack, locals, watch values, and what changed since the last stop.

    Args:
        session_id: The session from debug_start
        action: One of: run, continue, pause, step (into), next (over),
                finish (out), kill, breakpoint, breakpoint_delete,
                exception_breakpoints, watch_add, watch_remove, watch_list,
                watchpoint, set_variable, thread_select, frame_select,
                command, plan
        location: For breakpoint, "file:line" or a function name
        condition: For breakpoint, conditional expression
        hit_condition: For breakpoint, hit count condition (e.g. ">= 5")
        log_message: For breakpoint, makes it a logpoint (no stop)
        expression: For watch_add/watch_remove (alias: watch)
        breakpoint_id: For breakpoint_delete
        variable: For set_variable, dotted path (e.g. "config.retries")
        value: For set_variable, the new value
        thread_id: For thread_select / targeted stepping
        frame_index: For frame_select (0 = top)
        instruction: Step by instruction instead of line
        raw_command: For command, passed to the debugger natively
                     (DAP repl / lldb command)
        filters: For exception_breakpoints, filter ids (see capabilities)
        watch: For watch_add/watch_remove, the expression
        plan: For plan, {breakpoints, watches, max_stops, per_stop, until,
              time_budget_s}; runs server-side, returns a per-stop table
        child: Target a specific child session (see capabilities for ids)
        timeout: Seconds execution verbs wait for the next stop
    """
    app = get_app_ctx(ctx)
    manager: DebugSessionManager = app.get_debug_manager()
    try:
        node = manager.resolve(session_id, child)
    except KeyError as exc:
        return f"No active debug session with ID: {session_id} ({exc})"

    try:
        return await _dap_action(
            node.session,
            manager,
            node,
            session_id,
            action,
            location,
            condition,
            hit_condition,
            log_message,
            expression,
            breakpoint_id,
            variable,
            value,
            thread_id,
            frame_index,
            instruction,
            raw_command,
            filters,
            watch,
            plan,
            timeout,
        )
    except UnsupportedCapability as exc:
        return str(exc)
    except AdapterCrashed as exc:
        await manager.stop_tree(session_id)
        return f"Debug adapter crashed, session closed. {exc}"
    except Exception as exc:  # noqa: BLE001
        return f"Debug action `{action}` failed: {exc}"


@mcp.tool()
async def debug_inspect(
    ctx: Context,
    session_id: str,
    what: str,
    expression: str | None = None,
    address: str | None = None,
    count: int | None = None,
    frame_index: int | None = None,
    expand: str | None = None,
    against: str | None = None,
    thread_id: int | None = None,
    child: str | None = None,
    workspace_id: str | None = None,
) -> str:
    """Inspect debug state and store a structured, queryable snapshot.

    Snapshots are stored as runs: drill in with devtools_query/devtools_analyze
    (variables are flattened with a dotted `path` column).

    Args:
        session_id: The session from debug_start
        what: "stack", "variables", "watches", "threads", "breakpoints",
              "registers", "expression", "memory", "disassemble", "output",
              "diff", "capabilities", "snapshot" (full re-capture)
        expression: For expression, what to evaluate
        address: For memory/disassemble, memory reference
        count: Item count (frames/bytes/instructions)
        frame_index: Evaluate/inspect in this frame (0 = top)
        expand: For variables, drill into a dotted path (e.g. "obj.field")
        against: For diff, run_id of the snapshot to diff the latest stop
                 against (default: the previous stop)
        thread_id: For stack, which thread (default: stopped thread)
        child: Target a specific child session
        workspace_id: Workspace to store the snapshot
    """
    app = get_app_ctx(ctx)
    manager: DebugSessionManager = app.get_debug_manager()
    try:
        node = manager.resolve(session_id, child)
    except KeyError as exc:
        return f"No active debug session with ID: {session_id} ({exc})"

    try:
        return await _dap_inspect(
            ctx,
            node.session,
            session_id,
            what,
            expression,
            address,
            count,
            frame_index,
            expand,
            against,
            thread_id,
            workspace_id,
        )
    except UnsupportedCapability as exc:
        return str(exc)
    except AdapterCrashed as exc:
        await manager.stop_tree(session_id)
        return f"Debug adapter crashed, session closed. {exc}"
    except Exception as exc:  # noqa: BLE001
        return f"Inspect `{what}` failed: {exc}"


@mcp.tool()
async def debug_stop(ctx: Context, session_id: str, terminate: bool = True) -> str:
    """Terminate a debug session (and any child sessions).

    Args:
        session_id: The session to terminate
        terminate: Also terminate the debuggee (False = detach, leave it
                   running, attach sessions only)
    """
    app = get_app_ctx(ctx)
    manager: DebugSessionManager = app.get_debug_manager()
    count = await manager.stop_tree(session_id, terminate=terminate)
    if count == 0:
        return f"No active session: `{session_id}`"
    verb = "terminated" if terminate else "detached"
    node_note = f" ({count} nodes)" if count > 1 else ""
    summary_note = _store_session_summary(app, session_id)
    return f"**Session {verb}:** `{session_id}`{node_note}{summary_note}"


def _store_session_summary(app, session_id: str) -> str:
    """Aggregate a finished session's stop snapshots into one summary run:
    one root-first stack per stop, so devtools_flamegraph renders 'where
    this session stopped'. Best-effort. Never fails debug_stop."""
    try:
        stops: list[tuple[int, object, DebugSnapshot]] = []
        for ws in app.workspaces.values():
            for run in ws.runs.values():
                if isinstance(run, DebugSnapshot) and run.batch_id == session_id and run.tool == "stop":
                    stops.append((run.stop_seq, ws, run))
        if len(stops) < 2:
            return ""  # one stop has its own stack; nothing to aggregate
        stops.sort(key=lambda item: item[0])
        target_ws = stops[-1][1]
        last = stops[-1][2]
        summary = DebugSnapshot(
            run_id=str(uuid.uuid4()),
            tool="session",
            binary=last.binary,
            session_id=session_id,
            adapter=last.adapter,
            batch_id=session_id,
            stop_seq=len(stops),
            stop_reason=f"session summary ({len(stops)} stops)",
        )
        for seq, _, snap in stops:
            if not (snap.threads and snap.threads[0].frames):
                continue
            frames = snap.threads[0].frames
            summary.session_stacks.append(
                StackSample(
                    frames=[f.function or f.file or f"frame#{f.index}" for f in reversed(frames)],
                    weight=1,
                )
            )
            # One pseudo-thread per stop → debug_frames_df gives a queryable
            # cross-stop frame table (thread_id column = stop_seq).
            summary.threads.append(ThreadInfo(thread_id=seq, name=f"stop {seq}: {snap.stop_reason}", frames=frames))
        if not summary.session_stacks:
            return ""
        target_ws.store_run(summary)
        return (
            f"\n**Session summary:** `{summary.run_id}` ({len(stops)} stops): "
            f"devtools_flamegraph(run_id) shows where this session stopped."
        )
    except Exception:  # noqa: BLE001  # summary is a bonus, never a failure
        return ""
