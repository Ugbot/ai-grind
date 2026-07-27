#!/usr/bin/env python3
"""A scripted Debug Adapter Protocol adapter for tests. Stdlib only.

Speaks Content-Length framed DAP over stdin/stdout. Behavior:

- initialize      -> advertises conditional breakpoints, setVariable,
                     configurationDone, one exception filter ("uncaught").
- launch          -> emits the `initialized` event, then responds.
- setBreakpoints  -> verifies every breakpoint (ids 1..n).
- configurationDone -> responds, then emits a `stopped` event
                     (reason=breakpoint, threadId=1).
- threads         -> one thread (id 1, "MainThread").
- stackTrace      -> two frames (inner @ /tmp/fake.py:3, main @ :10).
- scopes          -> one Locals scope with variablesReference=100.
- variables       -> ref 100: x=1 (leaf) and obj (ref 101);
                     ref 101: field=42 (leaf).
- evaluate        -> echoes back "eval:<expression>".
- setVariable     -> echoes the requested value back.
- continue        -> responds, then (after a small delay so the client's
                     `continue` bookkeeping wins the race) emits `terminated`.
- disconnect      -> responds, then exits 0.

Crash mode: set FAKE_ADAPTER_CRASH_AFTER=<command> in the environment (or
pass --crash-after=<command>) and the adapter exits(1) immediately after
responding to that command — for adapter-crash detection tests.
"""

from __future__ import annotations

import json
import os
import sys
import time

CRASH_AFTER = os.environ.get("FAKE_ADAPTER_CRASH_AFTER", "")
for _arg in sys.argv[1:]:
    if _arg.startswith("--crash-after="):
        CRASH_AFTER = _arg.split("=", 1)[1]

_seq = 0


def _next_seq() -> int:
    global _seq
    _seq += 1
    return _seq


def send(payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    sys.stdout.buffer.write(b"Content-Length: %d\r\n\r\n" % len(body))
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()


def respond(request: dict, body: dict | None = None, success: bool = True, message: str = "") -> None:
    payload = {
        "seq": _next_seq(),
        "type": "response",
        "request_seq": request["seq"],
        "command": request.get("command", ""),
        "success": success,
    }
    if body is not None:
        payload["body"] = body
    if message:
        payload["message"] = message
    send(payload)


def event(name: str, body: dict | None = None) -> None:
    payload = {"seq": _next_seq(), "type": "event", "event": name}
    if body is not None:
        payload["body"] = body
    send(payload)


def read_message(stdin) -> dict | None:
    """Read one Content-Length framed message. None on EOF."""
    length = -1
    while True:
        line = stdin.readline()
        if not line:
            return None  # EOF
        stripped = line.strip()
        if not stripped:
            break  # end of headers
        name, _, value = stripped.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    if length < 0:
        return None
    body = stdin.read(length)
    if len(body) < length:
        return None
    return json.loads(body)


_VARIABLES = {
    100: [
        {"name": "x", "value": "1", "type": "int", "variablesReference": 0},
        {"name": "obj", "value": "Obj(...)", "type": "Obj", "variablesReference": 101},
    ],
    101: [
        {"name": "field", "value": "42", "type": "int", "variablesReference": 0},
    ],
}


def handle(request: dict) -> bool:
    """Handle one request. Returns False when the adapter should exit."""
    command = request.get("command", "")
    args = request.get("arguments") or {}

    if command == "initialize":
        respond(
            request,
            {
                "supportsConditionalBreakpoints": True,
                "supportsSetVariable": True,
                "supportsConfigurationDoneRequest": True,
                "exceptionBreakpointFilters": [{"filter": "uncaught", "label": "Uncaught Exceptions"}],
            },
        )
    elif command == "launch":
        event("initialized")
        respond(request, {})
    elif command == "setBreakpoints":
        breakpoints = [
            {"id": index + 1, "verified": True, "line": bp.get("line")}
            for index, bp in enumerate(args.get("breakpoints", []))
        ]
        respond(request, {"breakpoints": breakpoints})
    elif command == "setExceptionBreakpoints":
        respond(request, {})
    elif command == "configurationDone":
        respond(request, {})
        event(
            "stopped",
            {
                "reason": "breakpoint",
                "threadId": 1,
                "allThreadsStopped": True,
                "hitBreakpointIds": [1],
            },
        )
    elif command == "threads":
        respond(request, {"threads": [{"id": 1, "name": "MainThread"}]})
    elif command == "stackTrace":
        respond(
            request,
            {
                "stackFrames": [
                    {
                        "id": 1,
                        "name": "inner",
                        "line": 3,
                        "column": 1,
                        "source": {"path": "/tmp/fake.py", "name": "fake.py"},
                    },
                    {
                        "id": 2,
                        "name": "main",
                        "line": 10,
                        "column": 1,
                        "source": {"path": "/tmp/fake.py", "name": "fake.py"},
                    },
                ],
                "totalFrames": 2,
            },
        )
    elif command == "scopes":
        respond(
            request,
            {"scopes": [{"name": "Locals", "variablesReference": 100, "expensive": False}]},
        )
    elif command == "variables":
        ref = args.get("variablesReference", 0)
        respond(request, {"variables": _VARIABLES.get(ref, [])})
    elif command == "evaluate":
        respond(
            request,
            {
                "result": "eval:" + str(args.get("expression", "")),
                "type": "str",
                "variablesReference": 0,
            },
        )
    elif command == "setVariable":
        respond(
            request,
            {"value": str(args.get("value", "")), "type": "str", "variablesReference": 0},
        )
    elif command == "continue":
        respond(request, {"allThreadsContinued": True})
        # Small delay so the client processes the continue response (and its
        # running-state bookkeeping) before terminated arrives — mirrors the
        # gap a real debuggee takes to finish.
        time.sleep(0.05)
        event("exited", {"exitCode": 0})
        event("terminated")
    elif command == "disconnect":
        respond(request, {})
        return False
    else:
        respond(request, None, success=False, message=f"unknown command: {command}")
    return True


def main() -> int:
    stdin = sys.stdin.buffer
    while True:
        message = read_message(stdin)
        if message is None:
            return 0
        if message.get("type") != "request":
            continue
        keep_going = handle(message)
        if CRASH_AFTER and message.get("command") == CRASH_AFTER:
            sys.stderr.write(f"fake_adapter: crashing after {CRASH_AFTER}\n")
            sys.stderr.flush()
            return 1
        if not keep_going:
            return 0


if __name__ == "__main__":
    sys.exit(main())
