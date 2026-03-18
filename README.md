# devtools-mcp

A unified performance engineering toolkit exposed as an [MCP](https://modelcontextprotocol.io/) server. Gives AI coding assistants (Claude Code, Cursor, etc.) direct access to Valgrind, LLDB, DTrace, and perf through a normalized 13-tool interface backed by [Polars](https://pola.rs/) DataFrames.

## What it does

Instead of 40+ individual tool wrappers, devtools-mcp provides **4 categories of tools** that work uniformly across all backends:

| Category | Tools | Purpose |
|----------|-------|---------|
| **Batch** | `devtools_check`, `devtools_run`, `devtools_list`, `devtools_raw` | Detect tools, run analyses, list results, view raw output |
| **Analysis** | `devtools_analyze`, `devtools_query`, `devtools_compare` | Filter, group, query, and diff run results |
| **Search** | `devtools_search`, `devtools_correlate` | Cross-run text search and join runs on shared columns |
| **Debug** | `debug_start`, `debug`, `debug_inspect`, `debug_stop` | Interactive LLDB sessions with structured snapshots |

### Supported backends

| Suite | Tools | Platform |
|-------|-------|----------|
| **Valgrind** | memcheck, helgrind, drd, callgrind, cachegrind, massif | Linux |
| **LLDB** | Interactive debugging with backtrace, variables, breakpoints, registers, memory, disassembly | macOS, Linux |
| **DTrace** | trace, syscall, profile | macOS, Solaris |
| **perf** | stat, record, annotate | Linux |

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
      "args": ["run", "--directory", "/path/to/ai-grind", "devtools-mcp"]
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

```bash
uv run devtools-mcp
```

Speaks MCP over stdio. Compatible with any MCP client.

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
# Run full test suite (177 tests)
uv run pytest tests/ -v

# Tests cover:
# - All 4 parser suites (valgrind, lldb, dtrace, perf)
# - Polars analysis and comparison functions
# - Rich filtering and sampling engine
# - MCP server endpoints (via in-memory client/server session)
# - Workspace, registry, index, and formatter internals
# - Cross-run correlation
```

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
├── tools/                 # MCP tool definitions (4 modules)
│   ├── batch_tools.py     # check, run, list, raw
│   ├── analysis_tools.py  # analyze, query, compare
│   ├── search_tools.py    # search, correlate
│   └── debug_tools.py     # start, debug, inspect, stop
├── valgrind/              # Valgrind backend (6 tools)
│   ├── parsers/           # XML and text parsers
│   └── analysis/          # DataFrame builders and comparisons
├── lldb/                  # LLDB backend (PTY sessions + parsers)
├── dtrace/                # DTrace backend (3 tools)
└── perf/                  # perf backend (3 tools)
```

## License

MIT
