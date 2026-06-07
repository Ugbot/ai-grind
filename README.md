# devtools-mcp

A unified performance engineering **and build** toolkit exposed as an [MCP](https://modelcontextprotocol.io/) server. Gives AI coding assistants (Claude Code, Cursor, etc.) direct access to Valgrind, LLDB, DTrace, perf, **Windows ETW (PerfView), the JVM (JFR/jstack/jmap/async-profiler), the Windows debugger (CDB), Python (py-spy/cProfile), JavaScript (V8), and the Maven, Gradle, npm, pnpm, yarn and Cargo build/package systems** through a normalized, Polars-backed interface — plus **flame graphs** rendered as both an interactive SVG and a bounded text tree, and a browser visualization terminal.

> **Design rule:** the LLM is never flooded with raw, symbol-heavy tool output. Every run is stored as a queryable [Polars](https://pola.rs/) DataFrame; tools return only bounded summaries (top-N, percentages, a `run_id`) and large artifacts (flame-graph SVGs, raw traces) are written to disk and returned as a path. Drill in on demand with `devtools_analyze` / `devtools_query`.

## What it does

Instead of dozens of individual tool wrappers, devtools-mcp provides **a few categories of tools** that work uniformly across all backends:

| Category | Tools | Purpose |
|----------|-------|---------|
| **Batch** | `devtools_check`, `devtools_run`, `devtools_list`, `devtools_raw` | Detect tools, run analyses, list results, view raw output |
| **Analysis** | `devtools_analyze`, `devtools_query`, `devtools_compare` | Filter, group, query, and diff run results |
| **Search** | `devtools_search`, `devtools_correlate` | Cross-run text search and join runs on shared columns |
| **Flame** | `devtools_flamegraph` | Render any sampling run as an SVG flame graph + bounded text tree |
| **Visualize** | `devtools_dashboard` | Launch a local browser "visualization terminal" for all runs |
| **Debug** | `debug_start`, `debug`, `debug_inspect`, `debug_stop` | Interactive LLDB sessions with structured snapshots |

### Supported backends

| Suite | Tools | Platform |
|-------|-------|----------|
| **Valgrind** | memcheck, helgrind, drd, callgrind, cachegrind, massif | Linux |
| **LLDB** | Interactive debugging with backtrace, variables, breakpoints, registers, memory, disassembly | macOS, Linux |
| **DTrace** | trace, syscall, profile | macOS, Solaris |
| **perf** | stat, record, annotate | Linux |
| **ETW** (PerfView) | cpu (CPU hotspots Exc%/Inc% + flame graph) | Windows |
| **JVM** | jfr, threads (jstack), heap (jmap/jcmd), asprof (async-profiler) | any (JDK) |
| **CDB** | stacks (`~*k`), analyze (`!analyze -v`), inspect — batch-mode Windows debugger | Windows |
| **Python** | pyspy (sampling flame), dump (thread stacks), cprofile (deterministic stats) | any (cProfile stdlib; py-spy = `pip install py-spy`) |
| **Node/JS** | cpu (`--cpu-prof`), heap (`--heap-prof`) — V8 profiles → flame graph | any (Node.js) |
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

Any sampling run (dtrace `profile`, perf `record`, etw `cpu`, jvm `jfr`/`asprof`, cdb stacks, **py `pyspy`**, **node `cpu`/`heap`**) can be turned into a flame graph with `devtools_flamegraph(run_id=...)` — folded stacks are the universal currency, so flame graphs work the same for native code (Linux `perf script`, macOS dtrace `ustack`, Windows ETW), the JVM, **Python** (py-spy), and **JavaScript** (V8).

### Visualization terminal

`devtools_dashboard(action="start")` launches a local (127.0.0.1-only) web UI — the browser becomes a window onto everything the LLM sees:

- **`/`** — every run across workspaces.
- **per run** — the bounded summary, the full **queryable data table**, and raw logs.
- **interactive flame graph** — click any frame to zoom into its subtree (re-roots), hover for name + %. Pure server-rendered SVG, no JS framework or CDN.

It reads the live workspace, so runs appear as you create them. This is also the seam where agentic/visual tooling can be added later.

## Install

Requires Python 3.11+.

```bash
# Clone and install
git clone https://github.com/Ugbot/ai-grind.git
cd ai-grind
uv sync
```

## Usage

### As an MCP server (Claude Code)

Add to your project's `.mcp.json`:

```json
{
  "mcpServers": {
    "devtools-mcp": {
      "command": "uv",
      "args": ["run", "--directory", "C:\\Users\\Capta\\ai-grind", "devtools-mcp"]
    }
  }
}
```

Then in Claude Code:

```
> Check what dev tools are installed on this system

> Run memcheck on ./my_binary and show me the top memory leaks

> Start a debug session on ./crash_repro, set a breakpoint at main, and inspect variables
```

### As an MCP server (generic)

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
# Run full test suite (295 tests)
uv run pytest tests/ -v

# Tests cover:
# - All parser suites (valgrind, lldb, dtrace, perf, etw, jvm, cdb)
# - The flame-graph engine (fold -> tree -> SVG/text invariants)
# - Polars analysis and comparison functions
# - Rich filtering and sampling engine
# - MCP server endpoints (via in-memory client/server session)
# - Workspace, registry, index, and formatter internals
# - Cross-run correlation
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
│   └── debug_tools.py     # start, debug, inspect, stop
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
