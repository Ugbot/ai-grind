---
name: tracker-usage
description: >
  Track work in the devtools-mcp tracker — a persistent SQLite-backed mini-JIRA
  driven entirely through MCP tools. Use when starting or organizing any
  multi-step coding effort: create a project, file tasks with PROJ-123 keys,
  move them through the status workflow, and query bounded views (table, tree,
  rollup). The entry-point skill; see tracker-breakdown, tracker-acceptance,
  and tracker-github-sync for the deeper workflows.
---

# Using the devtools-mcp tracker

A persistent mini-JIRA inside the devtools-mcp server. State lives in one
global SQLite database (`~/.devtools-mcp/tracker.db`, override with the
`DEVTOOLS_MCP_TRACKER_DB` env var) and survives server restarts. Like the
profiling tools, **responses are always bounded** — full data is queried in
pages via `tracker_query`, never dumped.

## The ten tools

| Tool | Verbs (`action=`) | Role |
|---|---|---|
| `tracker_project` | create, list, get, set_policy | Project namespaces for task keys |
| `tracker_task` | create, get, update, move, breakdown | Tasks: CRUD + hierarchy |
| `tracker_status` | — (`key`, `status`, `override`) | Status transitions + close gate |
| `tracker_criteria` | add, update, record, remove, list | Acceptance criteria ↔ tests |
| `tracker_tag` | add, remove, rule_add, rule_list, rule_remove | Tags + auto-tag rules |
| `tracker_commits` | link, scan | Commit-hash linking |
| `tracker_deps` | add, remove, list, resolve | Dependencies + "what needs to happen" |
| `tracker_issue` | create, sync, close | GitHub (et al.) issue bridge |
| `tracker_query` | — (`view=`) | Bounded reporting |
| `tracker_sync` | status, sync | Local-first CRDT replication between machines |

## Bootstrap

```
tracker_project(action="create", key="GRIND", name="ai-grind work")
tracker_task(action="create", project="GRIND", title="Ship the tracker", kind="epic")
→ Created `GRIND-1` ...
```

- Project keys: 2-10 chars, `[A-Z][A-Z0-9]*`. Task keys allocate as `GRIND-1`,
  `GRIND-2`, ... and are never reused, even after deletes.
- Kinds: `epic / story / task / subtask / spike / test` (free-form strings are
  allowed; these get default behavior).
- Statuses: `open → in_progress → blocked → done / cancelled` (any transition
  is legal; closing to `done` runs the acceptance gate — see tracker-acceptance).
- Priority: 1 (highest) to 5.

## Querying without flooding

`tracker_query(view=…)` is the only reporting surface. Views:

- `tasks` — flat table; filter by `project`, `status`, `kind`, `tag`, `parent`
  (key), `title_pattern` (regex). Page with `offset`/`limit` (max 200).
- `tree` — indented hierarchy for a `project` (narrow with `parent`).
  `[ ]` open, `[>]` in progress, `[!]` blocked, `[x]` done, `[-]` cancelled.
- `rollup` — per project+kind status counts and criteria pass totals.
- `criteria`, `commits`, `tags` — the supporting tables.

`columns=["schema"]` lists a view's columns. `sort_by` + `sort_descending`
order any column.

## What needs to happen next

`tracker_deps(action="resolve", project="GRIND")` is the planner's view:

- **Ready now** — open tasks with no unsatisfied dependencies and no open
  subtasks; start anywhere in this list.
- **Blocked** — each with the exact tasks it waits on.
- **Order** — topological layers; everything in one layer can proceed in
  parallel.

Pass `key="GRIND-12"` instead to scope the plan to one goal: its open subtree
plus everything it transitively depends on. Declare edges as you plan:
`tracker_deps(action="add", key="GRIND-13", depends_on="GRIND-12")` — cycles
are rejected, and closing a task reports which dependents just became
unblocked.

## Seeing it and sharing it

- **Cards in the browser**: `devtools_dashboard(action="start")`, then open
  `/tracker` — project cards with progress bars, a per-project board (status
  columns + the "what needs to happen" plan), and task detail pages.
- **Multiple machines**: the tracker is a CRDT replica. `tracker_sync(
  action="sync", url="http://other-box:8765")` exchanges ops with another
  machine's dashboard and converges (LWW, idempotent, offline-friendly —
  concurrent PROJ-N allocations are re-keyed deterministically, nothing is
  lost). `tracker_sync(action="status")` shows this replica's site id and
  known peers.

## Daily loop

1. `tracker_deps(action="resolve", project="GRIND")` — what's actionable now.
2. `tracker_status(key="GRIND-7", status="in_progress")` — claim work.
3. Work; link commits (`tracker_commits(action="scan", repo=".")` picks up
   `GRIND-7` mentions in commit messages automatically).
4. `tracker_criteria(action="record", criterion_id=…, result="pass")` after
   the linked test runs green.
5. `tracker_status(key="GRIND-7", status="done")`.
