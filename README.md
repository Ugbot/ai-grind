# ai-grind

**An AI toolkit for developers building high-performance code with an LLM in the loop.** It gives your coding assistant real performance tooling it can drive itself, a built-in project tracker so plans never get lost between sessions, and the discipline to never pollute the model's context with raw tool output. Built to work nicely with **Claude Code**; works just as well with **Cursor** or any other [MCP](https://modelcontextprotocol.io/) client.

## Why this exists

Serious performance tools — VTune, Valgrind, PerfView/ETW, perf, JFR, the debuggers — demand two kinds of expertise: the command-line incantations to run them, and the experience to read what comes back. An LLM actually has both. What it doesn't have is room: one raw profiler dump is tens of thousands of symbol-heavy lines, and pasting that into a conversation destroys the context you're working in. ai-grind handles that noise. Every run is parsed into a queryable [Polars](https://pola.rs/) DataFrame; the model gets a bounded summary and a `run_id`, then **queries for exactly what it needs** instead of getting everything thrown back at once.

The same problem applies to the plan itself. Plans made in chat evaporate between sessions — pause and resume, and the knowledge is gone. So the server carries a **built-in project tracker** (a mini-JIRA in one SQLite file): the LLM writes its plan directly into it — epics, stories, punch-card subtasks, acceptance criteria tied to real tests, dependencies that resolve into "what should happen next" — and that state persists on disk across restarts, sessions, and tools.

> This is a working version, not a final one — it's the toolkit I'm using to build the next one.

## The pieces

1. **devtools-mcp** — the MCP server: 16 backends spanning profilers (VTune, ETW/PerfView, perf, DTrace, Valgrind, JFR/async-profiler, py-spy, V8), debuggers (LLDB, CDB), and build/package systems (Maven, Gradle, npm, pnpm, yarn, Cargo), all behind one normalized vocabulary, with flame graphs and a local browser dashboard for the human in the loop.
2. **The tracker** — persistent project management driven entirely through MCP tools: tasks with `PROJ-123` keys, hierarchy, status workflow with an acceptance-test close gate, commit linking, auto-tagging, GitHub issue sync, and a dependency resolver.
3. **The skills library** (`skills/`) — ~50 skills that teach the assistant *how* to use all of this and more: driving each profiler and reading its output, interpreting flame graphs, the tracker workflows, making PowerShell behave on Windows (5.1 vs 7), uv for Python, and project-specific drivers.

> **Design rule (everywhere):** the LLM is never flooded with raw, symbol-heavy tool output. Every run is stored as a queryable Polars DataFrame; tools return only bounded summaries (top-N, percentages, a `run_id`) and large artifacts (flame-graph SVGs, raw traces) are written to disk and returned as a path. Drill in on demand with `devtools_analyze` / `devtools_query`.

## The MCP tools

Instead of dozens of individual tool wrappers, devtools-mcp provides **a few categories of tools** that work uniformly across all backends:

| Category | Tools | Purpose |
|----------|-------|---------|
| **Batch** | `devtools_check`, `devtools_run`, `devtools_list`, `devtools_raw` | Detect tools, run analyses, list results, view raw output |
| **Analysis** | `devtools_analyze`, `devtools_query`, `devtools_compare` | Filter, group, query, and diff run results |
| **Search** | `devtools_search`, `devtools_correlate` | Cross-run text search and join runs on shared columns |
| **Flame** | `devtools_flamegraph` | Render any sampling run as an SVG flame graph + bounded text tree |
| **Visualize** | `devtools_dashboard` | Launch a local browser "visualization terminal" for all runs |
| **Debug** | `debug_start`, `debug`, `debug_inspect`, `debug_stop` | Interactive LLDB sessions with structured snapshots |
| **Track** | `tracker_project`, `tracker_task`, `tracker_status`, `tracker_criteria`, `tracker_tag`, `tracker_commits`, `tracker_deps`, `tracker_issue`, `tracker_query` | Persistent plans, tasks, acceptance gates, and "what's next" |

### Supported backends

| Suite | Tools | Platform |
|-------|-------|----------|
| **Valgrind** | memcheck, helgrind, drd, callgrind, cachegrind, massif | Linux |
| **LLDB** | Interactive debugging with backtrace, variables, breakpoints, registers, memory, disassembly | macOS, Linux |
| **DTrace** | cpu, syscall, trace | macOS, Solaris |
| **perf** | cpu, stat, annotate | Linux |
| **ETW** (PerfView) | cpu (CPU hotspots Exc%/Inc% + flame graph) | Windows |
| **VTune** (Intel) | cpu (hotspots), threads (threading), alloc (memory-consumption), memory (memory-access), uarch (top-down), snapshot | Windows, Linux (oneAPI) |
| **JVM** | cpu (JFR), alloc (async-profiler), threads (jstack), heap (jmap/jcmd) | any (JDK) |
| **CDB** | stacks (`~*k`), analyze (`!analyze -v`), inspect — batch-mode Windows debugger | Windows |
| **Python** | cpu (py-spy sampling), threads (py-spy dump), cprofile (deterministic) | any (cProfile stdlib; py-spy = `pip install py-spy`) |
| **Node/JS** | cpu (`--cpu-prof`), alloc (`--heap-prof`) — V8 profiles → flame graph | any (Node.js) |
| **Maven** | build, test, deps, sync | any (mvn or project `mvnw`) |
| **Gradle** | build, test, deps, sync, tasks | any (gradle or project `gradlew`) |
| **npm** | build, test, deps, sync, audit, outdated, tasks | any (Node.js) |
| **pnpm** | build, test, deps, sync, audit, outdated, tasks | any (pnpm) |
| **yarn** | build, test, deps, sync, audit, tasks | any (yarn classic) |
| **Cargo** | build, check, test, deps, sync, audit | any (Rust / rustup) |

**One vocabulary, many backends.** Every build/package-manager backend speaks the
same verbs — `deps` (dependency tree + subdependencies), `sync` (resolve / install
/ refresh / fetch), `build`, `test`, plus `audit` / `outdated` / `tasks` where the
tool supports them — so `devtools_run(suite="npm", tool="deps")` and
`devtools_run(suite="cargo", tool="deps")` behave identically; only the backend
implementation differs.

Profiling verbs are unified too — **`cpu`** means a CPU profile whether the backend
is perf, dtrace, etw, jvm, py, or node; **`alloc`** is allocation profiling
(jvm/node); **`threads`** is a thread dump (jvm/py). Any sampling run can be turned
into a flame graph with `devtools_flamegraph(run_id=...)` — folded stacks are the
universal currency, so flame graphs work the same for native code (Linux `perf
script`, macOS dtrace `ustack`, Windows ETW), the JVM, **Python** (py-spy), and
**JavaScript** (V8).

### Visualization terminal

`devtools_dashboard(action="start")` launches a local (127.0.0.1-only) web UI — the browser becomes a window onto everything the LLM sees:

- **`/`** — every run across workspaces.
- **per run** — the bounded summary, the full **queryable data table**, and raw logs.
- **interactive flame graph** — click any frame to zoom into its subtree (re-roots), hover for name + %. Pure server-rendered SVG, no JS framework or CDN.

It reads the live workspace, so runs appear as you create them. This is also the seam where agentic/visual tooling can be added later.

### Progress tracker (mini-JIRA)

A persistent, SQLite-backed task tracker built into the server, so the LLM can
put its plan **directly into durable storage** instead of leaving it in chat.
Pause a session, resume tomorrow, switch from Claude Code to Cursor — the
epics, subtasks, acceptance criteria, and "what's next" are all still there.
Ten `tracker_*` tools over one global database (`~/.devtools-mcp/tracker.db`,
override with `DEVTOOLS_MCP_TRACKER_DB`):

| Tool | Role |
|---|---|
| `tracker_project` | Projects: key namespaces (`GRIND-123`), close policy (advisory/strict) |
| `tracker_task` | Tasks: create / get / update / move / `breakdown` (punch-card subtasks), 6-level hierarchy |
| `tracker_status` | Status workflow + the acceptance close gate |
| `tracker_criteria` | Acceptance criteria linked to tests (`file::test_name`), pass/fail recording |
| `tracker_tag` | Tags + auto-tag rules (kind / regex / parent-kind, applied at creation) |
| `tracker_commits` | Commit links: manual or `git log` scan for task keys in messages (scan runs the subprocess off the event loop and batches links in one transaction — never blocks the server) |
| `tracker_deps` | Task dependencies + execution-plan resolver: what's ready now, what's blocked by what, parallelizable order |
| `tracker_issue` | GitHub issue bridge (create from task with criteria checklist, sync drift, close); provider-abstracted, `GITHUB_TOKEN`/`GH_TOKEN` |
| `tracker_query` | Bounded reporting: `tasks` / `tree` / `rollup` / `criteria` / `commits` / `tags` views |
| `tracker_sync` | Local-first collaboration: CRDT sync with another replica (see below) |

Same no-token-flood contract as the profiling tools: every response is bounded
markdown; the data lives in SQLite (WAL, indexed for the tag/kind/rollup and
reverse-dependency probes) and is paged through Polars-backed views.
Workflow skills live in `skills/authored/skills/tracker/`. The tracker also
renders as **cards in the dashboard** — project overview, a per-project board
(status columns + the execution plan), and task detail pages at `/tracker`.

### Local-first collaboration (CRDT)

The tracker is a **replicated, local-first store**, not just a local file.
Every replica has a site id and a hybrid logical clock; SQLite triggers
capture every mutation into an op-log with site-independent payloads (rows
carry global `uid`s, references use uids — never local rowids). Merging is
last-writer-wins per row, idempotent, and converges regardless of sync order
or topology — ops propagate transitively, so two laptops that only ever talk
to a third machine still end up identical. Concurrent allocation of the same
human key (both sides mint `PROJ-7` offline) is resolved by deterministic
re-keying, so no replica ever loses a task.

Syncing is one tool call: `tracker_sync(action="sync", url="http://other-box:8765")`
— the peer is any machine running `devtools_dashboard`, which serves the sync
API at `/api/crdt/`. Work fully offline, sync when you meet the network again.
(v1 is row-level LWW with full-exchange transport; field-level merge and
incremental vector-clock exchange are the documented upgrade path.)

### Agent collaboration (local)

Run several agents on one machine — Claude Code, Cursor, Codex, side by side —
without them silently stepping on each other's edits. **Claude Code hooks**
(`hooks/`, wired up automatically by the plugin) report every Edit/Write to
the local service; the tracker records who touched what, and if another
session is on the same file the agent gets a warning injected straight into
its context. Agents can also take **advisory claims** (self-expiring leases,
heartbeated by their own edits) on files before a big refactor via the
`tracker_files` tool — by default a contested file warns, and with
`DEVTOOLS_MCP_COLLAB_MODE=ask` it surfaces a permission prompt to the human
instead. Everything links to tracker tasks (`DEVTOOLS_MCP_TASK=GRIND-42`), the
**`/collab`** dashboard page shows sessions, claims and contested files live,
and `tracker_query(view="activity"|"claims")` gives the bounded tables. See
the `agent-collab` skill for the workflow.

> **Coming soon:** a multi-user **team collab server** that extends this
> across machines and teammates. The local layer is its single-machine
> precursor and will sync into it.

### Skills library

`skills/` is the other half of the toolkit: the knowledge that makes the tools
usable. Around 50 Claude Code skills covering how to drive each profiler and
*read* its output (etw-profiling, vtune-profiling, flamegraph-reading, jvm /
python / node profiling), the tracker workflows (tracker-usage, -breakdown,
-acceptance, -github-sync), build tooling, a full set of
PowerShell-on-Windows survival guides (5.1 vs 7 idioms, errors, native
commands, non-interactive automation), and uv for Python projects. Skills are
managed as a library — harvested + hand-authored sources merged into a
loadable mirror by `skills/sync.py` — see `skills/README.md`.

## Install

Requires Python 3.11+.

```bash
# Clone and install
git clone https://github.com/Ugbot/ai-grind.git
cd ai-grind
uv sync
```

### As a Claude Code plugin (recommended)

This repo is a self-contained Claude Code **plugin marketplace**
(`.claude-plugin/marketplace.json`) shipping one plugin, `devtools-mcp`, that
bundles the MCP server **and** the full skills library (49 skills, 15 commands,
3 agents), plus the agent-collab hooks. Install it in Claude Code:

```
/plugin marketplace add Ugbot/ai-grind
/plugin install devtools-mcp@ai-grind
```

The plugin wires up the MCP server automatically via `${CLAUDE_PLUGIN_ROOT}` —
no hand-edited `.mcp.json` path. It still needs `uv` on `PATH` and the repo's
Python deps (`uv sync` runs on first launch). To iterate on bundled skills, edit
`skills/authored/` (or `skills/catalog/`) and regenerate the committed bundle:

```bash
python skills/sync.py --target plugin   # rebuild plugin/{skills,commands,agents}
```

## Usage

### As a shared local service (recommended)

Run **one instance** on the machine; every project — and every client — talks
to it over HTTP. The dashboard comes up with it, so the UI is always there:

```powershell
.\scripts\devtools-service.ps1 start     # MCP at http://127.0.0.1:8000/mcp, dashboard at :8765
.\scripts\devtools-service.ps1 status    # is it up?
.\scripts\devtools-service.ps1 install   # also start automatically at login
```

(Equivalent manual command: `uv run devtools-mcp --transport http --port 8000`
— network transports auto-start the dashboard; `--no-dashboard` to opt out.)

Then point clients at the URL — **once, at user scope**, so all projects get it:

```bash
# Claude Code (all projects):
claude mcp add --transport http devtools-mcp http://127.0.0.1:8000/mcp --scope user
```

Or per project — `.mcp.json` (Claude Code) and `.cursor/mcp.json` (Cursor),
both already in this repo:

```json
// .mcp.json (Claude Code)
{ "mcpServers": { "devtools-mcp": { "type": "http", "url": "http://127.0.0.1:8000/mcp" } } }

// .cursor/mcp.json (Cursor)
{ "mcpServers": { "devtools-mcp": { "type": "streamable-http", "url": "http://127.0.0.1:8000/mcp" } } }
```

Why this shape: one process owns the tracker DB and the run workspace, so every
project's profiling runs and tasks land in the same dashboard; restart the
service and Claude Code reconnects automatically (exponential backoff). If the
service is down, the MCP server just shows as unavailable — `/mcp` in Claude
Code retries it once you've started the service.

### Cursor setup

1. `uv sync` (once)
2. `.\scripts\devtools-service.ps1 start`
3. Open **Cursor → Settings → MCP** — project [`.cursor/mcp.json`](.cursor/mcp.json) registers `devtools-mcp` at `http://127.0.0.1:8000/mcp`
4. **Reload MCP** if the service was started after Cursor opened
5. Smoke test: ask the agent to run `devtools_check`
6. Optional login autostart: `.\scripts\devtools-service.ps1 install`

Dashboard (auto-starts with the service): `http://127.0.0.1:8765` — Runs, Search, Tracker board, installed-tools status.

### As a per-project stdio server (alternative)

The classic spawn-per-session model still works (this is also what the plugin
install above wires up automatically) — in `.mcp.json`:

```json
{
  "mcpServers": {
    "devtools-mcp": {
      "command": "uv",
      "args": ["run", "--directory", "C:\\path\\to\\ai-grind", "devtools-mcp"]
    }
  }
}
```

Then in Claude Code:

```
> Check what dev tools are installed on this system

> Run memcheck on ./my_binary and show me the top memory leaks

> Start a debug session on ./crash_repro, set a breakpoint at main, and inspect variables

> Profile ./app with VTune hotspots and show me a flame graph

> Plan this feature in the tracker: epic, stories, subtasks with dependencies — then tell me what to do first
```

### As an MCP server (Cursor and other clients)

The server speaks three transports — pick with `--transport` (or env vars):

```bash
uv run devtools-mcp                              # stdio (pipes) — default
uv run devtools-mcp --transport http --port 8000 # streamable HTTP at /mcp
uv run devtools-mcp --transport sse  --port 8000 # SSE at /sse
```

- **stdio** — MCP over stdin/stdout; how editor/CLI clients (Claude Code, Cursor)
  spawn it. Nothing is printed to stdout except the protocol.
- **http** (streamable-http) — a long-lived server at `http://<host>:<port>/mcp`;
  connect multiple clients, run it remotely, or share one instance.
- **sse** — legacy Server-Sent-Events transport at `/sse` for older clients.

Host/port/transport can also come from env: `DEVTOOLS_MCP_TRANSPORT`,
`DEVTOOLS_MCP_HOST`, `DEVTOOLS_MCP_PORT` (default `127.0.0.1:8000`). To connect an
MCP client to the HTTP server instead of spawning stdio:

```json
{
  "mcpServers": {
    "devtools-mcp": { "type": "http", "url": "http://127.0.0.1:8000/mcp" }
  }
}
```

### Standalone (Python)

```python
from devtools_mcp.server import mcp

# The FastMCP server instance is importable for testing
# or embedding in other applications
```

## How it works

```
Claude Code ←→ MCP Protocol ←→ devtools-mcp server
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                Valgrind          LLDB          DTrace/perf
                    │               │               │
                Parse output    PTY session     Parse output
                    │               │               │
                    └───────┬───────┘───────────────┘
                            │
                     Polars DataFrames
                            │
                ┌───────────┼───────────────┐
                │           │               │
            Filtering   Comparison     Unified Index
            & Sampling  (A vs B)       (cross-run search)
```

**Key design decisions:**

- **Normalized interface** — Every backend registers a `BackendSpec` with `detect()`, `run()`, `df_builders`, and `format_summary()`. Adding a new tool suite means implementing one module, not 10 tools.
- **Polars DataFrames** — All results are converted to DataFrames for filtering, grouping, correlation, and comparison. The `FilterSpec` engine supports regex patterns, thresholds, pagination, sampling (random, stratified, every-nth), and sort overrides.
- **Unified search index** — All runs in a workspace are indexed into a single DataFrame with normalized columns (`function`, `file`, `kind`, `value`, etc.), enabling cross-tool queries like "find all functions that both leak memory and are CPU hotspots."
- **Structured LLDB sessions** — Debug sessions use a PTY-based interactive process. Snapshots (backtrace, variables, breakpoints, etc.) are parsed into structured models and stored as workspace runs, making them queryable through the same analysis tools.

## Example: correlating memory leaks with CPU hotspots

```
1. devtools_run(suite="valgrind", tool="memcheck", binary="./server")
   → run_id: "abc123"

2. devtools_run(suite="valgrind", tool="callgrind", binary="./server")
   → run_id: "def456"

3. devtools_correlate(run_id_a="abc123", run_id_b="def456", join_on="function")
   → Table showing functions that both leak memory AND are CPU-hot
```

## Testing

```bash
# Run full test suite (437 tests)
uv run pytest tests/ -v

# Tests cover:
# - All parser suites (valgrind, lldb, dtrace, perf, etw, jvm, cdb)
# - The flame-graph engine (fold -> tree -> SVG/text invariants)
# - Polars analysis and comparison functions
# - Rich filtering and sampling engine
# - MCP server endpoints (via in-memory client/server session)
# - Workspace, registry, index, and formatter internals
# - Cross-run correlation
# - The progress tracker (schema/migrations, hierarchy + close gate,
#   tag rules, git-scan against real temp repos, GitHub via MockTransport,
#   end-to-end tracker_* tools)
```

Windows/JVM/CDB backends are tested with **synthetic** tool output (PerfView CSV,
JFR JSON, jstack/jmap text, CDB backtraces) so the suite needs none of the real
tools installed.

All test data is randomized via factory functions — no hardcoded fixtures.

## Project structure

```
src/devtools_mcp/
├── server.py              # FastMCP server, lifespan, shared helpers
├── models.py              # RunBase — shared base for all results
├── registry.py            # Backend auto-registration and tool detection
├── workspace.py           # Run storage, caching, temp file management
├── index.py               # Unified cross-run search index
├── filters.py             # Declarative FilterSpec engine
├── formatters/            # Markdown table and summary formatters
├── tools/                 # MCP tool definitions
│   ├── batch_tools.py     # check, run, list, raw
│   ├── analysis_tools.py  # analyze, query, compare
│   ├── search_tools.py    # search, correlate
│   ├── flame_tools.py     # devtools_flamegraph
│   ├── viz_tools.py       # devtools_dashboard
│   ├── tracker_tools.py   # tracker_* — the progress tracker (mini-JIRA)
│   └── debug_tools.py     # start, debug, inspect, stop
├── tracker/               # Tracker domain layer (SQLite, WAL, migrations)
│   ├── schema.py db.py    # versioned DDL + connection/transactions
│   ├── tasks.py           # projects, PROJ-123 keys, hierarchy, close gate
│   ├── criteria.py        # acceptance criteria <-> tests, gate evaluation
│   ├── tags.py            # tags + auto-tag rules engine
│   ├── commits.py         # manual links + git-log key scanning
│   ├── issues.py          # task -> external issue lifecycle
│   ├── frames.py          # Polars views (tasks/tree/rollup/...)
│   └── providers/         # IssueProvider ABC, GitHub REST, GitLab stub
├── viz/                   # Browser visualization terminal (stdlib HTTP, dark UI)
├── flamegraph/            # Shared engine: fold -> tree -> SVG + text
│   ├── fold.py            # Brendan-Gregg folded-stack I/O
│   ├── tree.py            # call tree + per-function exc/inc stats
│   ├── render_text.py     # bounded text flame-tree + top table
│   └── render_svg.py      # pure-Python interactive SVG (no deps)
├── valgrind/              # Valgrind backend (6 tools)
│   ├── parsers/           # XML and text parsers
│   └── analysis/          # DataFrame builders and comparisons
├── lldb/                  # LLDB backend (PTY sessions + parsers)
├── dtrace/                # DTrace backend (3 tools)
├── perf/                  # perf backend (3 tools)
├── etw/                   # Windows ETW backend (PerfView) — CPU hotspots + stacks
├── vtune/                 # Intel VTune backend — hotspots/threading/memory/uarch + flame graph
├── jvm/                   # JVM backend — JFR, threads, heap, async-profiler
├── cdb/                   # Windows debugger backend (batch CDB)
├── py/                    # Python backend — py-spy, thread dumps, cProfile
├── node/                  # Node/JS backend — V8 --cpu-prof / --heap-prof
├── build/                 # shared build core — models, JUnit, JS dep/audit parsers, frames
├── maven/ gradle/         # JVM build backends
├── npm/ pnpm/ yarn/       # JS package-manager backends
├── cargo/                 # Rust/Cargo backend
└── hotspots.py            # shared stacks -> hotspots DataFrame (jvm/py/node)
```

Build & package tools are normalized too: `mvn dependency:tree` / `npm ls --all`
/ `cargo tree` (thousands of lines) become a **queryable dependency frame** with
depth + conflict detection; reactor/task results, JUnit tests, and audit
advisories become bounded summaries. The `binary` argument for these backends is
the **project directory** (Maven/Gradle prefer a `mvnw`/`gradlew` wrapper, else
the global tool).

A backend is one module exposing `detect()`, `run()`, `df_builders`,
`format_summary()`, and an optional `stacks` builder (which unlocks flame graphs).
See any of `etw/`, `jvm/`, `cdb/` for the pattern.

## License

MIT
