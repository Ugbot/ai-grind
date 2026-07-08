"""Claude Code PreToolUse hook: before editing a file, ask the local
devtools-mcp collab service whether another agent session has an active claim
on it. Optional strict layer — the PostToolUse touch report already surfaces
warnings; install this one only when you want claimed files to prompt.

Behavior by DEVTOOLS_MCP_COLLAB_MODE:
    warn (default) — emit an additionalContext warning, never block
    ask            — active claims by another session surface a permission
                     prompt to the human (permissionDecision 'ask');
                     recent-touch-only conflicts still just warn
    off            — do nothing

Any failure (service down, timeout, bad JSON) exits 0 silently.
Same env as report_touch.py otherwise (URL, AGENT, COLLAB=0 kill switch).
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request

TIMEOUT_S = 2.0  # see report_touch.py — cold tracker open can exceed 0.5s


def _target_file(tool_input: dict) -> str:
    for key in ("file_path", "notebook_path"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _describe(conflicts: list[dict]) -> str:
    notes = []
    for c in conflicts[:5]:
        who = c.get("agent") or c.get("session_id", "another agent")
        task = f" (task {c['task_key']})" if c.get("task_key") else ""
        if c.get("kind") == "claim":
            notes.append(f"{who}{task} holds a claim until {c.get('expires_at')}")
        else:
            notes.append(f"{who}{task} touched it at {c.get('ts')}")
    return "; ".join(notes)


def main() -> int:
    mode = os.environ.get("DEVTOOLS_MCP_COLLAB_MODE", "warn")
    if os.environ.get("DEVTOOLS_MCP_COLLAB", "1") == "0" or mode == "off":
        return 0
    try:
        event = json.load(sys.stdin)
        path = _target_file(event.get("tool_input") or {})
        if not path:
            return 0
        session = str(event.get("session_id") or "unknown-session")
        cwd = str(event.get("cwd") or os.getcwd())
        base = os.environ.get("DEVTOOLS_MCP_COLLAB_URL", "http://127.0.0.1:8765").rstrip("/")
        query = urllib.parse.urlencode({"session": session, "path": path, "cwd": cwd})
        with urllib.request.urlopen(base + "/api/collab/conflicts?" + query, timeout=TIMEOUT_S) as resp:
            body = json.loads(resp.read().decode("utf-8", "replace"))
        conflicts = body.get("conflicts") or []
        if not conflicts:
            return 0
        claims = [c for c in conflicts if c.get("kind") == "claim"]
        message = f"devtools-collab: {body.get('file', path)} — " + _describe(conflicts)
        if claims and mode == "ask":
            print(
                json.dumps(
                    {
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "ask",
                            "permissionDecisionReason": message,
                        }
                    }
                )
            )
        else:
            print(
                json.dumps(
                    {
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "additionalContext": message + ". Coordinate via the tracker before editing.",
                        }
                    }
                )
            )
    except Exception:
        pass  # never break editing
    return 0


if __name__ == "__main__":
    sys.exit(main())
