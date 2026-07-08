"""Claude Code PostToolUse hook: report edited files to the local devtools-mcp
collab service so agents on this machine can see each other's activity.

Fire-and-forget by design — any failure (service down, bad JSON, timeout)
exits 0 silently so editing is never blocked. Stdlib only; keep startup cheap.

Env:
    DEVTOOLS_MCP_COLLAB=0        disable entirely (fast exit)
    DEVTOOLS_MCP_COLLAB_URL      service base URL (default http://127.0.0.1:8765)
    DEVTOOLS_MCP_AGENT           human-readable agent label shown in the dashboard
    DEVTOOLS_MCP_TASK            tracker task key (PROJ-123) to link activity to
    DEVTOOLS_MCP_COLLAB_MODE     warn (default) | off — whether conflict warnings
                                 are surfaced back into the agent's context
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request

TIMEOUT_S = 0.5
FILES_MAX = 50


def _touched_files(tool_name: str, tool_input: dict) -> list[str]:
    """File paths a tool call touched. Bounded, deduplicated, order-preserving."""
    paths: list[str] = []
    for key in ("file_path", "notebook_path"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            paths.append(value)
    edits = tool_input.get("edits")
    if isinstance(edits, list):
        for edit in edits[:FILES_MAX]:
            value = edit.get("file_path") if isinstance(edit, dict) else None
            if isinstance(value, str) and value:
                paths.append(value)
    seen: set[str] = set()
    unique: list[str] = []
    for p in paths:  # bounded: paths <= 2 + FILES_MAX
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique[:FILES_MAX]


def _conflict_note(conflicts: list[dict]) -> str:
    """One human sentence per conflicting session, bounded."""
    notes = []
    for c in conflicts[:5]:
        who = c.get("agent") or c.get("session_id", "another agent")
        task = f" (task {c['task_key']})" if c.get("task_key") else ""
        if c.get("kind") == "claim":
            notes.append(f"{who}{task} has CLAIMED {c.get('file')} until {c.get('expires_at')}")
        else:
            notes.append(f"{who}{task} recently touched {c.get('file')} at {c.get('ts')}")
    return "; ".join(notes)


def main() -> int:
    if os.environ.get("DEVTOOLS_MCP_COLLAB", "1") == "0":
        return 0
    try:
        event = json.load(sys.stdin)
        tool_name = str(event.get("tool_name") or "")
        files = _touched_files(tool_name, event.get("tool_input") or {})
        if not files:
            return 0
        op = "write" if tool_name.lower() == "write" else "edit"
        payload = {
            "session_id": str(event.get("session_id") or "unknown-session"),
            "agent": os.environ.get("DEVTOOLS_MCP_AGENT", ""),
            "task_key": os.environ.get("DEVTOOLS_MCP_TASK", ""),
            "tool": tool_name,
            "op": op,
            "cwd": str(event.get("cwd") or os.getcwd()),
            "files": files,
        }
        base = os.environ.get("DEVTOOLS_MCP_COLLAB_URL", "http://127.0.0.1:8765").rstrip("/")
        req = urllib.request.Request(
            base + "/api/collab/touch",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            body = json.loads(resp.read().decode("utf-8", "replace"))
        conflicts = body.get("conflicts") or []
        mode = os.environ.get("DEVTOOLS_MCP_COLLAB_MODE", "warn")
        if conflicts and mode != "off":
            print(
                json.dumps(
                    {
                        "hookSpecificOutput": {
                            "hookEventName": "PostToolUse",
                            "additionalContext": (
                                "devtools-collab: " + _conflict_note(conflicts) + ". Coordinate via the tracker "
                                "(tracker_files action='status') to avoid conflicting edits."
                            ),
                        }
                    }
                )
            )
    except Exception:
        pass  # never break editing
    return 0


if __name__ == "__main__":
    sys.exit(main())
