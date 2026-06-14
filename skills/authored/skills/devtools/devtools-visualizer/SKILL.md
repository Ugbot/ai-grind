---
name: devtools-visualizer
description: >
  Use the devtools-mcp browser visualization terminal — a local web UI for every
  profiling/debugging run. Use when you want to SEE the data a human can explore:
  interactive (click-to-zoom) flame graphs, the queryable data table per run, raw
  logs, and the tracker as card boards (projects, status columns, task detail),
  across Linux/macOS/Windows. Covers starting/stopping the dashboard, what each
  view shows, and the CRDT sync API it serves for other replicas. The visual
  companion to devtools-mcp-usage and tracker-usage.
---

# The visualization terminal (devtools-mcp `devtools_dashboard`)

The LLM works from bounded summaries + Polars queries; a **human** often wants to
*see* the profile. `devtools_dashboard` serves a local web UI that turns the
browser into a window onto the same runs.

## Start / stop

```
devtools_dashboard(action="start")               # prints a http://127.0.0.1:8765 URL
devtools_dashboard(action="start", open_browser=true)   # also opens it
devtools_dashboard(action="status")
devtools_dashboard(action="stop")
```

It binds to **localhost only** — nothing leaves the machine. Use `port=0` to grab
any free port if 8765 is taken. The server reads the **live** workspace, so new
runs appear on refresh; no restart needed.

## What you get

- **`/` dashboard** — every run (run_id, suite:tool, target, time, exit) with
  links to each view. Runs that have stacks show a **flame** link.
- **`/run/<id>`** — the bounded text summary, the full **queryable data table**
  (the same DataFrame `devtools_analyze` filters), and links to flame/raw.
- **`/flame/<id>`** — an **interactive flame graph**: width = inclusive time,
  depth grows downward, hover for name + %, and **click any frame to zoom** into
  its subtree (it re-roots; use "reset" to go back). Server-rendered SVG — no JS
  framework, works offline.
- **`/raw/<id>`** — raw tool output / logs (bounded).
- **`/tracker`** — the project-management views, as proper cards: project
  overview with status chips + progress bars, **`/tracker/<PROJ>`** a board
  (status columns of task cards + the "what needs to happen" plan with ready
  and blocked highlighted), **`/tracker/task/<KEY>`** full task detail
  (criteria checklist, commits, dependencies, children).
- **`/api/crdt/*`** — the JSON sync API other tracker replicas pull from /
  push to (`tracker_sync` on another machine points here).

## Typical flow

```
devtools_run(suite="etw", tool="cpu", binary="C:/app.exe")     # or jvm/dtrace/perf/cdb
devtools_dashboard(action="start", open_browser=true)
# → open the flame link for that run, click into the hot subtree
```

## Cross-platform parity

Flame graphs render identically from any backend that produces stacks: **perf**
(`perf script`) on Linux, **dtrace** (`ustack`) on macOS, **etw** (folded) and
**cdb** (thread stacks) on Windows, and **jvm** (JFR/async-profiler) anywhere.
Folded stacks are the universal currency.

## Notes

- This is the seam for future **agentic/visual tooling** — the UI reads the live
  AppContext, so new pages/controls can be added without touching the backends.
- For interpreting what you see, read [[flamegraph-reading]]. For the overall
  tool workflow, [[devtools-mcp-usage]].
