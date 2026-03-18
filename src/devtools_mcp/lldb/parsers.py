"""Parse LLDB text output into structured models.

LLDB output is text-based (PTY). These parsers extract structured data
from common LLDB commands.
"""

from __future__ import annotations

import re

from devtools_mcp.lldb.models import (
    LldbBreakpoint,
    LldbStackFrame,
    LldbThread,
    LldbVariable,
)


def parse_backtrace(text: str) -> list[LldbThread]:
    """Parse `bt` or `bt all` output into structured threads.

    Example LLDB backtrace output:
    * thread #1, queue = 'com.apple.main-thread', stop reason = breakpoint 1.1
        frame #0: 0x0000000100003f60 a.out`main at main.c:10:5
        frame #1: 0x00007ff8004b1345 dyld`start + 1765
    """
    threads: list[LldbThread] = []
    current_thread: LldbThread | None = None

    for line in text.splitlines():
        line = line.rstrip()

        # Thread header: "* thread #1, name = 'main', stop reason = breakpoint 1.1"
        _thread_pat = (
            r"[*\s]*thread #(\d+)(?:,\s*tid\s*=\s*0x[0-9a-fA-F]+)?"
            r"(?:,\s*name\s*=\s*'([^']*)')?"
            r"(?:,\s*queue\s*=\s*'([^']*)')?"
            r"(?:,\s*stop reason\s*=\s*(.+))?"
        )
        thread_match = re.match(_thread_pat, line)
        if thread_match:
            current_thread = LldbThread(
                thread_id=int(thread_match.group(1)),
                index=int(thread_match.group(1)),
                name=thread_match.group(2),
                queue=thread_match.group(3),
                stop_reason=thread_match.group(4),
            )
            threads.append(current_thread)
            continue

        # Frame line: "  frame #0: 0x0000000100003f60 a.out`main at main.c:10:5"
        frame_match = re.match(
            r"\s+frame #(\d+):\s+(0x[0-9a-fA-F]+)\s+(\S+)`(\S+)(?:\s+(?:\+\s+\d+\s+)?at\s+([^:]+):(\d+)(?::(\d+))?)?",
            line,
        )
        if frame_match and current_thread is not None:
            current_thread.frames.append(
                LldbStackFrame(
                    index=int(frame_match.group(1)),
                    address=frame_match.group(2),
                    module=frame_match.group(3),
                    function=frame_match.group(4),
                    file=frame_match.group(5),
                    line=int(frame_match.group(6)) if frame_match.group(6) else None,
                    column=int(frame_match.group(7)) if frame_match.group(7) else None,
                )
            )
            continue

        # Simpler frame format: "  frame #0: 0x00007ff8004b1345 dyld`start + 1765"
        simple_frame = re.match(
            r"\s+frame #(\d+):\s+(0x[0-9a-fA-F]+)\s+(\S+)`(\S+)",
            line,
        )
        if simple_frame and current_thread is not None:
            current_thread.frames.append(
                LldbStackFrame(
                    index=int(simple_frame.group(1)),
                    address=simple_frame.group(2),
                    module=simple_frame.group(3),
                    function=simple_frame.group(4),
                )
            )

    return threads


def parse_thread_list(text: str) -> list[LldbThread]:
    """Parse `thread list` output.

    Example:
    Process 12345 stopped
    * thread #1: tid = 0x1234, 0x0000000100003f60 a.out`main, stop reason = breakpoint 1.1
      thread #2: tid = 0x1235, 0x00007ff800123456 libsystem_kernel.dylib`__psynch_cvwait
    """
    threads: list[LldbThread] = []

    for line in text.splitlines():
        _tl_pat = (
            r"[*\s]*thread #(\d+):\s*"
            r"(?:tid\s*=\s*0x[0-9a-fA-F]+,?\s*)?"
            r"(?:name\s*=\s*'([^']*)',?\s*)?"
            r"(?:queue\s*=\s*'([^']*)',?\s*)?"
            r"(0x[0-9a-fA-F]+)?\s*(\S+`\S+)?"
            r"(?:,?\s*stop reason\s*=\s*(.+))?"
        )
        match = re.match(_tl_pat, line)
        if match:
            func_str = match.group(5)
            module = None
            function = None
            if func_str and "`" in func_str:
                parts = func_str.split("`", 1)
                module = parts[0]
                function = parts[1] if len(parts) > 1 else None

            threads.append(
                LldbThread(
                    thread_id=int(match.group(1)),
                    index=int(match.group(1)),
                    name=match.group(2),
                    queue=match.group(3),
                    stop_reason=match.group(6),
                    frames=[
                        LldbStackFrame(
                            address=match.group(4) or "",
                            module=module,
                            function=function,
                        )
                    ]
                    if match.group(4)
                    else [],
                )
            )

    return threads


def parse_variables(text: str) -> list[LldbVariable]:
    """Parse `frame variable` output.

    Example:
    (int) argc = 1
    (const char **) argv = 0x00007ffeefbff5f0
    (char [256]) buffer = "hello world"
    """
    variables: list[LldbVariable] = []

    for line in text.splitlines():
        # Pattern: (type) name = value
        match = re.match(r"\s*\(([^)]+)\)\s+(\S+)\s*=\s*(.*)", line)
        if match:
            variables.append(
                LldbVariable(
                    type=match.group(1).strip(),
                    name=match.group(2).strip(),
                    value=match.group(3).strip(),
                )
            )

    return variables


def parse_breakpoint_list(text: str) -> list[LldbBreakpoint]:
    """Parse `breakpoint list` output.

    Example:
    Current breakpoints:
    1: name = 'main', locations = 1, resolved = 1, hit count = 1
      1.1: where = a.out`main + 20 at main.c:10:5, address = 0x0000000100003f60, resolved, hit count = 1
    """
    breakpoints: list[LldbBreakpoint] = []

    for line in text.splitlines():
        # Main breakpoint line: "1: name = 'main', ..."
        bp_match = re.match(r"\s*(\d+):\s*(?:name\s*=\s*'([^']*)')?", line)
        if bp_match and not line.strip().startswith(f"{bp_match.group(1)}."):
            bp_id = int(bp_match.group(1))
            hit_match = re.search(r"hit count\s*=\s*(\d+)", line)
            bp = LldbBreakpoint(
                id=bp_id,
                name=bp_match.group(2),
                hit_count=int(hit_match.group(1)) if hit_match else 0,
                resolved="resolved" in line,
                enabled="disabled" not in line,
            )

            # Check sub-location for file:line
            # Look ahead would be complex; we parse location from the line itself
            file_match = re.search(r"at\s+(\S+):(\d+)", line)
            if file_match:
                bp.file = file_match.group(1)
                bp.line = int(file_match.group(2))

            addr_match = re.search(r"address\s*=\s*(0x[0-9a-fA-F]+)", line)
            if addr_match:
                bp.address = addr_match.group(1)

            cond_match = re.search(r"condition\s*=\s*'([^']*)'", line)
            if cond_match:
                bp.condition = cond_match.group(1)

            breakpoints.append(bp)

    return breakpoints


def parse_registers(text: str) -> dict[str, str]:
    """Parse `register read` output.

    Example:
    General Purpose Registers:
        rax = 0x0000000100003f60
        rbx = 0x0000000000000000
    """
    registers: dict[str, str] = {}

    for line in text.splitlines():
        match = re.match(r"\s+(\w+)\s*=\s*(0x[0-9a-fA-F]+(?:\s+.*)?)", line)
        if match:
            registers[match.group(1)] = match.group(2).strip()

    return registers
