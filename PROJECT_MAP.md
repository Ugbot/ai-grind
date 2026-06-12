# PROJECT_MAP — ai-grind

A unified developer-tooling workspace. Two concerns live here:

1. **devtools-mcp** — an MCP server giving AI assistants a normalized,
   Polars-backed interface over Valgrind, LLDB, DTrace, perf, Windows ETW
   (PerfView), Intel VTune (hotspots/threading/memory/uarch/snapshot), the JVM
   (JFR/jstack/jmap/async-profiler), the Windows debugger
   (CDB), Python (py-spy/cProfile), Node/JavaScript (V8 cpu/heap), and the
   Maven/Gradle/npm/pnpm/yarn/Cargo build & package systems (uniform verbs:
   build/test/deps/sync/audit/outdated/tasks)
   — plus cross-platform flame graphs (SVG + text) and a browser
   **visualization terminal** (`devtools_dashboard`) for humans, and a
   persistent SQLite-backed **progress tracker** (mini-JIRA: `tracker_*` tools —
   projects, hierarchical tasks with PROJ-123 keys, acceptance criteria linked
   to tests, commit links, auto-tag rules, GitHub issue sync). Core rule: never
   flood the LLM with raw output; return bounded summaries + a queryable frame
   (see the `devtools-mcp-no-token-flood` memory).
2. **skills library** — one canonical home for every Claude Code skill, command,
   and agent harvested from across all local projects, plus hand-authored skills
   (PowerShell, profiling/debugging).

## Layout

| Path | What |
|---|---|
| `src/devtools_mcp/` | MCP server: backends (`valgrind/ lldb/ dtrace/ perf/ etw/ vtune/ jvm/ cdb/ py/ node/ maven/ gradle/ npm/ pnpm/ yarn/ cargo/`), shared `build/` + `flamegraph/` engines, `hotspots.py`, `viz/` browser terminal, `tools/`, `formatters/`, `models.py`, `registry.py`, `workspace.py`, `index.py`, `filters.py`, `server.py` |
| `src/devtools_mcp/tracker/` | Progress tracker domain layer: `schema.py` (versioned migrations), `db.py` (WAL SQLite at `~/.devtools-mcp/tracker.db`, env `DEVTOOLS_MCP_TRACKER_DB`), `tasks.py`/`criteria.py`/`tags.py`/`commits.py`/`deps.py` (dependency edges + execution-plan resolver)/`issues.py`, `frames.py` (Polars views), `providers/` (GitHub REST via `GITHUB_TOKEN`, GitLab stub). Tools in `tools/tracker_tools.py` (9 `tracker_*` tools); skills in `skills/authored/skills/tracker/` |
| `tests/` | Test suite (409 cases) + `tests/fixtures/` (compiled targets gitignored) |
| `.mcp.json` | MCP server definition (`devtools-mcp`, stdio). HTTP/SSE: `devtools-mcp --transport http\|sse --port N` (or `DEVTOOLS_MCP_TRANSPORT`) |
| `skills/` | **Unified skills library** — see `skills/README.md` |
| `pyproject.toml`, `uv.lock` | Python project metadata / lock |

## Skills library (`skills/`)

Three trees: `catalog/` (harvested, hierarchical, regenerated), `authored/`
(hand-written original skills, committed) and `loadable/` (generated flat mirror
Claude loads). Driven by two scripts and one map:

- `sources.toml` — explicit harvest work-list (upstream paths → type/category)
- `harvest.py` — copies upstream → `catalog/`, writes `MANIFEST.json` (provenance)
- `sync.py --target local|project|global` — merges `catalog/` + `authored/` →
  loadable mirror

Contents: 47 skills = 24 harvested (debug / profiling / code-intel /
project-drivers / narrative) + 23 authored (`powershell/` 5.1 & 7 side by side,
`devtools/`, `tracker/` — the progress-tracker workflow skills);
23 commands (build / llm-station); 3 agents (docs / testing / integration).
Harvested items are copied from upstream projects, never moved. Full breakdown and
dedup notes in `skills/README.md`.

## Conventions

- Python 3.11+; scripts follow Tiger Style (bounded loops, ≥2 asserts/function,
  explicit, fail-loud on invariant violations).
- Lint gate (all must pass): `uv run ruff check src tests`, `uv run black --check
  src tests`, `uv run mypy src/devtools_mcp`. Config in `pyproject.toml`; mypy has
  a ratchet list of legacy modules with `ignore_errors` — fix one, remove it from
  the list, never add to it.
- To add/refresh a harvested asset: edit `skills/sources.toml`, run
  `python skills/harvest.py`, then `python skills/sync.py --target <t>`.
