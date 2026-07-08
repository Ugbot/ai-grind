---
name: agent-collab
description: >
  Coordinate multiple AI agents working on the same machine and codebases via
  the devtools-mcp local collaboration layer — file-touch reporting hooks,
  advisory file claims (leases), conflict checks, and the /collab dashboard
  page, all linked to tracker tasks. Use when running more than one agent at
  once (Claude Code, Cursor, Codex), when an edit warns that another agent is
  on the file, when asked "who is working on what", or to claim files before a
  big refactor. A multi-user team collab server is coming soon; this is its
  single-machine precursor.
---

# Local agent collaboration

Multiple agents on one machine share one tracker DB (`~/.devtools-mcp/tracker.db`).
Every file touch and claim lands there, so each agent — and the human at the
dashboard — can see who is working where. Nothing here blocks by default:
claims are **advisory** leases, surfaced as warnings.

> Coming soon: a multi-user **team collab server** extends this across
> machines and teammates. This local layer is its precursor — same data shapes,
> same tracker linkage.

## Prerequisite

The shared devtools service must be running (hooks are silent no-ops otherwise):

```powershell
.\scripts\devtools-service.ps1 start   # MCP :8000, dashboard :8765
```

## Identity — do this first

Set a stable, human-readable label per agent session so the dashboard doesn't
show raw session ids, and link work to the task you're on:

```powershell
$env:DEVTOOLS_MCP_AGENT = "claude-refactor"   # who you are
$env:DEVTOOLS_MCP_TASK  = "GRIND-42"          # what you're working on (optional)
```

Hooks attach both to every touch automatically.

## Automatic touch reporting (hooks)

Installed with the plugin (`hooks/hooks.json`), or manually in `settings.json`.
Two scripts, both stdlib-only and never blocking:

- `hooks/report_touch.py` (PostToolUse on Edit/Write/MultiEdit/NotebookEdit) —
  reports the touched file; if someone else is on it, a warning lands in the
  agent's context.
- `hooks/check_conflict.py` (PreToolUse, optional strict layer) — checks for
  claims *before* the edit.

Config via env:

| Var | Meaning |
|---|---|
| `DEVTOOLS_MCP_COLLAB=0` | kill switch — hooks exit immediately |
| `DEVTOOLS_MCP_COLLAB_MODE` | `warn` (default) / `ask` (claimed files prompt the human) / `off` |
| `DEVTOOLS_MCP_COLLAB_URL` | service base, default `http://127.0.0.1:8765` |

Minimum-overhead install: PostToolUse only — its response already carries conflicts.

## Explicit coordination (tracker_files tool)

```
tracker_files(action="status")                      # who's active, claims, recent touches
tracker_files(action="claim", repo=".", file="src/core.py",
              task_key="GRIND-42", ttl_minutes=30)  # advisory lease before a big refactor
tracker_files(action="conflicts", repo=".", file="src/core.py")  # who else is there
tracker_files(action="release")                     # drop all your claims when done
tracker_files(action="touch", repo=".", files=[...])# manual touch (non-hook clients)
```

Claims expire on their own (default 15 min) and are heartbeated by your own
touches, so a crashed agent never wedges a file. Another session claiming a
file you hold gets a clear error naming you and your task.

## Queries and the dashboard

- `tracker_query(view="activity")` / `view="claims"` — bounded tables.
- **`http://127.0.0.1:8765/collab`** — sessions, active claims, recent touches;
  contested files (two+ sessions) are highlighted.
- Task detail pages (`/tracker/task/KEY`) show the file activity linked to that
  task via `DEVTOOLS_MCP_TASK` or `task_key=`.

## Workflow for a multi-agent session

1. Start the service; give each agent a `DEVTOOLS_MCP_AGENT` label and its
   `DEVTOOLS_MCP_TASK` key.
2. Partition work by task in the tracker (see tracker-breakdown skill).
3. Before a wide-ranging change, `claim` the hot files; release when done.
4. When a warning names another agent, check `tracker_files(action="status")`
   and either pick a different file or coordinate through task comments —
   don't race the edit.
