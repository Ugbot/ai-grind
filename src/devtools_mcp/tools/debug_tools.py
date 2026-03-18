"""Debug tools: debug_start, debug, debug_inspect, debug_stop.

4 normalized tools that replace the 28 individual LLDB tools.
"""

from __future__ import annotations

import re
import uuid

from mcp.server.fastmcp import Context

from devtools_mcp.lldb.formatters import format_snapshot_summary
from devtools_mcp.lldb.models import LldbSnapshot
from devtools_mcp.lldb.parsers import (
    parse_backtrace,
    parse_breakpoint_list,
    parse_registers,
    parse_thread_list,
    parse_variables,
)
from devtools_mcp.lldb.session import LldbSession
from devtools_mcp.server import get_app_ctx, mcp


def _get_session(ctx: Context, session_id: str) -> LldbSession:
    """Get an LLDB session by ID."""
    app = get_app_ctx(ctx)
    session = app.lldb_sessions.get(session_id)
    if session is None or not isinstance(session, LldbSession):
        msg = f"No active LLDB session with ID: {session_id}"
        raise ValueError(msg)
    return session


@mcp.tool()
async def debug_start(
    ctx: Context,
    binary: str,
    args: list[str] | None = None,
    lldb_path: str = "lldb",
    working_dir: str | None = None,
    timeout: float = 30.0,
) -> str:
    """Start an LLDB debug session for a binary.

    Creates an interactive LLDB process. Use debug() to send commands
    and debug_inspect() to get structured state snapshots.

    Supports C, C++, Rust, Zig, and any language that compiles to native code
    with debug info (DWARF).

    Args:
        binary: Path to the executable to debug
        args: Arguments for the binary (set via LLDB settings)
        lldb_path: Path to lldb executable (default: "lldb")
        working_dir: Working directory for the session
        timeout: Command timeout in seconds (default: 30)
    """
    app = get_app_ctx(ctx)
    session_id = str(uuid.uuid4())

    session = LldbSession(
        session_id=session_id,
        lldb_path=lldb_path,
        working_dir=working_dir,
        command_timeout=timeout,
    )

    try:
        output = await session.start()
    except Exception as e:
        await session.cleanup()
        return f"Failed to start LLDB: {e}"

    # Load the binary
    await session.execute_command(f'file "{binary}"')
    session.target = binary

    # Set arguments if provided
    if args:
        args_str = " ".join(f'"{a}"' for a in args)
        await session.execute_command(f"settings set -- target.run-args {args_str}")

    app.lldb_sessions[session_id] = session

    return (
        f"**LLDB session started**\n"
        f"**Session ID:** `{session_id}`\n"
        f"**Binary:** {binary}\n"
        f"**LLDB:** {output.splitlines()[0] if output else 'ready'}\n\n"
        f'Use `debug(session_id="{session_id}", action="run")` to start execution.\n'
        f'Use `debug(session_id="{session_id}", action="breakpoint", location="main")` to set breakpoints.'
    )


@mcp.tool()
async def debug(
    ctx: Context,
    session_id: str,
    action: str,
    location: str | None = None,
    condition: str | None = None,
    expression: str | None = None,
    breakpoint_id: int | None = None,
    variable: str | None = None,
    thread_id: int | None = None,
    frame_index: int | None = None,
    instruction: bool = False,
    raw_command: str | None = None,
    args: list[str] | None = None,
) -> str:
    """Execute a debug action in an LLDB session.

    This single tool replaces ~20 individual LLDB tools by dispatching
    on the `action` parameter.

    Args:
        session_id: The session from debug_start
        action: One of: run, continue, step, next, finish, kill,
                breakpoint, breakpoint_delete, watchpoint,
                thread_select, frame_select, command
        location: For breakpoint — function name, file:line, or address
        condition: For breakpoint — conditional expression
        expression: For watchpoint — variable/address to watch
        breakpoint_id: For breakpoint_delete — which breakpoint to remove
        variable: For watchpoint — variable name
        thread_id: For thread_select — which thread
        frame_index: For frame_select — which frame (0-based)
        instruction: For step/next — step by instruction instead of line
        raw_command: For command action — raw LLDB command string
        args: For run — override program arguments
    """
    try:
        session = _get_session(ctx, session_id)
    except ValueError as e:
        return str(e)

    try:
        if action == "run":
            if args:
                args_str = " ".join(f'"{a}"' for a in args)
                await session.execute_command(f"settings set -- target.run-args {args_str}")
            output = await session.execute_command("run")
            return f"**Running program**\n\n{_clean_output(output)}"

        if action == "continue":
            output = await session.execute_command("continue")
            return f"**Continued**\n\n{_clean_output(output)}"

        if action == "step":
            cmd = "si" if instruction else "s"
            output = await session.execute_command(cmd)
            return f"**Stepped {'instruction' if instruction else 'line'}**\n\n{_clean_output(output)}"

        if action == "next":
            cmd = "ni" if instruction else "n"
            output = await session.execute_command(cmd)
            return f"**Stepped over**\n\n{_clean_output(output)}"

        if action == "finish":
            output = await session.execute_command("finish")
            return f"**Finished function**\n\n{_clean_output(output)}"

        if action == "kill":
            output = await session.execute_command("process kill")
            return f"**Process killed**\n\n{_clean_output(output)}"

        if action == "breakpoint":
            if not location:
                return "Missing `location` for breakpoint action"
            # Detect file:line vs function name
            if ":" in location and not location.startswith("0x"):
                file_part, line_part = location.split(":", 1)
                output = await session.execute_command(f'breakpoint set --file "{file_part}" --line {line_part}')
            else:
                output = await session.execute_command(f'breakpoint set --name "{location}"')
            if condition:
                bp_match = re.search(r"Breakpoint (\d+):", output)
                if bp_match:
                    await session.execute_command(f'breakpoint modify -c "{condition}" {bp_match.group(1)}')
            return f"**Breakpoint set:** {location}\n\n{_clean_output(output)}"

        if action == "breakpoint_delete":
            if breakpoint_id is None:
                return "Missing `breakpoint_id` for breakpoint_delete action"
            output = await session.execute_command(f"breakpoint delete {breakpoint_id}")
            return f"**Breakpoint {breakpoint_id} deleted**\n\n{_clean_output(output)}"

        if action == "watchpoint":
            expr = expression or variable
            if not expr:
                return "Missing `expression` or `variable` for watchpoint action"
            output = await session.execute_command(f"watchpoint set variable {expr}")
            return f"**Watchpoint set:** {expr}\n\n{_clean_output(output)}"

        if action == "thread_select":
            if thread_id is None:
                return "Missing `thread_id` for thread_select action"
            output = await session.execute_command(f"thread select {thread_id}")
            return f"**Selected thread {thread_id}**\n\n{_clean_output(output)}"

        if action == "frame_select":
            if frame_index is None:
                return "Missing `frame_index` for frame_select action"
            output = await session.execute_command(f"frame select {frame_index}")
            return f"**Selected frame {frame_index}**\n\n{_clean_output(output)}"

        if action == "command":
            if not raw_command:
                return "Missing `raw_command` for command action"
            output = await session.execute_command(raw_command)
            return f"**{raw_command}**\n\n{_clean_output(output)}"

        valid = (
            "run, continue, step, next, finish, kill, breakpoint, "
            "breakpoint_delete, watchpoint, thread_select, frame_select, command"
        )
        return f"Unknown action: `{action}`. Valid: {valid}"

    except Exception as e:
        return f"Debug action `{action}` failed: {e}"


@mcp.tool()
async def debug_inspect(
    ctx: Context,
    session_id: str,
    what: str,
    expression: str | None = None,
    address: str | None = None,
    location: str | None = None,
    register: str | None = None,
    count: int | None = None,
    frame_index: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Inspect debug state and store a structured snapshot.

    The snapshot is stored in the workspace and queryable via
    devtools_analyze(), devtools_query(), and devtools_search().

    Args:
        session_id: The session from debug_start
        what: What to inspect — "backtrace", "variables", "threads",
              "breakpoints", "registers", "memory", "expression", "disassemble"
        expression: For expression/memory — what to evaluate/read
        address: For memory — address to examine
        location: For disassemble — function or address
        register: For registers — specific register name (None=all)
        count: For memory/disassemble — number of items
        frame_index: Select frame before inspecting variables
        workspace_id: Workspace to store the snapshot
    """
    try:
        session = _get_session(ctx, session_id)
    except ValueError as e:
        return str(e)

    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    snapshot = LldbSnapshot(
        run_id=str(uuid.uuid4()),
        suite="lldb",
        tool=what,
        binary=session.target or "unknown",
        session_id=session_id,
        snapshot_type=what,
    )

    try:
        if what == "backtrace":
            cmd = "bt all"
            if count:
                cmd = f"bt -c {count}"
            raw = await session.execute_command(cmd)
            snapshot.threads = parse_backtrace(raw)
            snapshot.raw_output = raw

        elif what == "threads":
            raw = await session.execute_command("thread list")
            snapshot.threads = parse_thread_list(raw)
            snapshot.raw_output = raw

        elif what == "variables":
            if frame_index is not None:
                await session.execute_command(f"frame select {frame_index}")
            raw = await session.execute_command("frame variable")
            snapshot.variables = parse_variables(raw)
            snapshot.raw_output = raw

        elif what == "breakpoints":
            raw = await session.execute_command("breakpoint list")
            snapshot.breakpoints = parse_breakpoint_list(raw)
            snapshot.raw_output = raw

        elif what == "registers":
            cmd = "register read"
            if register:
                cmd += f" {register}"
            raw = await session.execute_command(cmd)
            snapshot.registers = parse_registers(raw)
            snapshot.raw_output = raw

        elif what == "memory":
            addr = address or expression
            if not addr:
                return "Missing `address` or `expression` for memory inspection"
            n = count or 16
            raw = await session.execute_command(f"memory read -c {n} {addr}")
            snapshot.raw_output = raw

        elif what == "expression":
            if not expression:
                return "Missing `expression` for expression evaluation"
            raw = await session.execute_command(f"expression -- {expression}")
            snapshot.raw_output = raw

        elif what == "disassemble":
            cmd = "disassemble"
            if location:
                cmd += f" --name {location}"
            if count:
                cmd += f" -c {count}"
            raw = await session.execute_command(cmd)
            snapshot.raw_output = raw

        else:
            valid = "backtrace, variables, threads, breakpoints, registers, memory, expression, disassemble"
            return f"Unknown inspect target: `{what}`. Valid: {valid}"

    except Exception as e:
        return f"Inspect `{what}` failed: {e}"

    # Store snapshot in workspace for querying
    ws.store_run(snapshot)

    return format_snapshot_summary(snapshot)


@mcp.tool()
async def debug_stop(ctx: Context, session_id: str) -> str:
    """Terminate an LLDB debug session.

    Sends quit to LLDB and cleans up the PTY.

    Args:
        session_id: The session to terminate
    """
    app = get_app_ctx(ctx)
    session = app.lldb_sessions.pop(session_id, None)

    if session is None:
        return f"No active session: `{session_id}`"

    if isinstance(session, LldbSession):
        await session.cleanup()

    return f"**Session terminated:** `{session_id}`"


def _clean_output(output: str) -> str:
    """Clean LLDB output for display — strip prompts and excess whitespace."""
    lines = output.strip().splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped and stripped != "(lldb)":
            cleaned.append(stripped)
    return "\n".join(cleaned) if cleaned else "(no output)"
