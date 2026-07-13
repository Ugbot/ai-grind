# PROJECT_MAP — ai-grind

A unified developer-tooling workspace. Two concerns live here:

1. **devtools-mcp** — an MCP server giving AI assistants a normalized,
   Polars-backed interface over Valgrind, LLDB, DTrace, perf, Windows ETW
   (PerfView), Intel VTune (hotspots/threading/memory/uarch/snapshot), the JVM
   (JFR/jstack/jmap/async-profiler), the Windows debugger
   (CDB), Python (py-spy/cProfile), Node/JavaScript (V8 cpu/heap), RenderDoc
   (GPU frame capture + headless replay: drawcall tree, GPU counter timings,
   resources, thumbnails, GPU-time flame graphs), and the
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
| `src/devtools_mcp/` | MCP server: backends (`valgrind/ lldb/ dtrace/ perf/ etw/ vtune/ jvm/ cdb/ py/ node/ renderdoc/ maven/ gradle/ npm/ pnpm/ yarn/ cargo/`), shared `build/` + `flamegraph/` engines, `hotspots.py`, `viz/` browser terminal, `tools/`, `formatters/`, `models.py`, `registry.py` (capability model + `_BACKEND_MODULES` manifest loader + `InstallSpec`), `install.py` (+ `tools/install_tools.py`: `devtools_install` — per-OS install commands, dry-run default, execute gated by `DEVTOOLS_MCP_ALLOW_INSTALL=1`), `workspace.py`, `index.py`, `filters.py`, `server.py` |
| `src/devtools_mcp/renderdoc/` | GPU frame suite: verbs `capture` (targetcontrol auto-trigger or launch-wait/F12) / `analyze` / `counters` / `resources` / `thumb`. Replay runs `scripts/bridge.py` inside `qrenderdoc --python` (embedded Py 3.6 — env-var params, JSON out, sys.exit suppresses the UI; ruff per-file-ignores). Needs GPU + interactive session for replay verbs |
| `src/devtools_mcp/station/` | **Station sync** — llm-station-remote (`C:/code/llm-station-remote`, FastAPI+Postgres platform) as remote backend, local-first: `config.py` (per-repo `.devtools-mcp/station.toml` rules, env>repo>global precedence, lls_ keys env-only + leak check), `client.py` (sync httpx, the ONLY platform-HTTP module; zero `llm_station` imports — test-enforced), `links.py` (row identity map + canonical hashes = echo suppression), `diff.py` (crdt_ops as local change feed), `engine.py` (run_sync: watermarks, auto-pause at 10 failures, sync log), `domains/` (tasks both-ways local-wins + pending-intent creates; coord sessions push + handoff mirror; claims→advisory checkouts w/ TTL slack; skills manifest push; perf run upload w/ `local-run:` tag recovery). Tables in tracker.db MIGRATION_V6. Tools: `station_link` (incl. `auth`/`logout`) / `station_sync` / `station_session`. **Browser auth**: dashboard `/station/auth` page → platform OAuth (`?local_callback=` loopback redirect, platform-side GRIND-49) → key stored in `credentials.py` (`~/.devtools-mcp/station-auth.json`; env `LLM_STATION_API_KEY` always wins); auth instructions live in tool docstrings/errors + the `station-sync` authored skill |
| `src/devtools_mcp/tracker/` | Progress tracker domain layer: `schema.py` (versioned migrations), `db.py` (WAL SQLite at `~/.devtools-mcp/tracker.db`, env `DEVTOOLS_MCP_TRACKER_DB`), `tasks.py`/`criteria.py`/`tags.py`/`commits.py`/`deps.py` (dependency edges + execution-plan resolver)/`issues.py`, `frames.py` (Polars views), `crdt.py`+`sync.py` (local-first replication: HLC, op-capture triggers, LWW merge, HTTP peer sync via the dashboard's `/api/crdt/`), `activity.py` (local agent collaboration: v5 `file_activity`+`file_claims` tables — debounced touch log, advisory TTL claims, conflicts; site-local, not CRDT-synced), `providers/` (GitHub REST via `GITHUB_TOKEN`, GitLab stub). Tools in `tools/tracker_tools.py` (11 `tracker_*` tools); tracker card + `/collab` views in `viz/`; skills in `skills/authored/skills/tracker/` + `collab/` |
| `tests/` | Test suite (592 cases) + `tests/fixtures/` (compiled targets gitignored) |
| `.mcp.json` / `.cursor/mcp.json` | Client configs pointing at the **shared local service** (`http://127.0.0.1:8000/mcp`, streamable HTTP). One instance serves all projects; also registered at Claude Code user scope. Stdio spawn remains available: `uv run devtools-mcp` |
| `scripts/devtools-service.ps1` | Run that shared instance: start/stop/status/install(-at-login). Network transports auto-start the dashboard (`:8765`, `--no-dashboard` to opt out) |
| `.claude-plugin/` | Claude Code **plugin + marketplace** manifests. `plugin.json` (repo root = plugin root, so `${CLAUDE_PLUGIN_ROOT}` = the Python project) points component paths at `plugin/`. `marketplace.json` lists the one `devtools-mcp` plugin with `source: "."`. Install: `/plugin marketplace add Ugbot/ai-grind` then `/plugin install devtools-mcp@ai-grind` |
| `plugin/` | **Committed** flat plugin bundle (`skills/ commands/ agents/`) generated by `sync.py --target plugin`. This is what the plugin loads — Claude does not recurse into the hierarchical `skills/catalog` \| `authored` trees |
| `src/devtools_mcp/skilldocs/` | **Live skills**: SKILL.md as a pycrdt text doc — `store.py` (update-log persistence in `<data_root>/skilldocs.db`, snapshot compaction, **variant-aware** materialization to `~/.claude/skills/<name>/SKILL.md`, env `DEVTOOLS_MCP_LIVE_SKILLS_DIR`; patch uses UTF-8 **byte** offsets — pycrdt Text is byte-indexed), `sync.py` (state-vector diff exchange via the dashboard's `/api/skilldoc/`). **Dynamic skills**: `variants.py` (`<!-- power:low\|high -->` block rendering), `control.py` (`skill_control` LWW table: mode/overrides/disabled; env `DEVTOOLS_MCP_SKILL_MODE`). **Router**: `router.py` (auto-generated `skill-router` live skill — indexes catalog+authored+live skills between INDEX markers under live-editable rules; rebuild patches only the index). Tool: `skill_live` (create/get/list/append/patch/sync/publish/delete/**route/mode/enable/disable**) in `tools/skill_tools.py`; free control panel at dashboard `GET /skills` + `/api/skilldoc/{route/rebuild,mode,enable,disable}` |
| `src/devtools_mcp/planning/` + `goap/` | **Pluggable planner seam** (optional/severable — the system works fully without it). `goap/` = vendored regressive-A* GOAP core (agentix/GOAP, MIT). `planning/planner.py` (`resolve()` picks backend from `DEVTOOLS_MCP_PLANNER`=none\|local\|platform\|url; native wheel hook), `local_backend.py` (in-process GOAP over skills' ```goap descriptors, cost scaled by power mode), `remote_backend.py` (platform/URL delegation). Tool: `plan` (goal/world/mode/layered → ordered skills) in `tools/plan_tools.py`; dashboard `POST /api/plan`. Canonical planner + Kahn layering are premium (platform, `llm-station-remote`) |
| `hooks/` | Claude Code hooks (plugin `hooks` key): `report_touch.py` (PostToolUse — reports edited files to the collab API, relays conflicts into context) + `check_conflict.py` (PreToolUse — optional; `DEVTOOLS_MCP_COLLAB_MODE=warn\|ask\|off`). Stdlib-only, never blocking; local precursor to the **team collab server (coming soon)** |
| `skills/` | **Unified skills library** — see `skills/README.md` |
| `pyproject.toml`, `uv.lock` | Python project metadata / lock |

## Skills library (`skills/`)

Three trees: `catalog/` (harvested, hierarchical, regenerated), `authored/`
(hand-written original skills, committed) and `loadable/` (generated flat mirror
Claude loads). Driven by two scripts and one map:

- `sources.toml` — explicit harvest work-list (upstream paths → type/category)
- `harvest.py` — copies upstream → `catalog/`, writes `MANIFEST.json` (provenance)
- `sync.py --target local|plugin|agents|project|global` — merges `catalog/` +
  `authored/` → flat mirror (`plugin/` committed; `.agents/` gitignored,
  skills-only; `.codex/`+`.cursor/` are hand-written configs, not targets)
- Or via MCP: `skills_sync(action=status|harvest|sync, target=…)` in
  `tools/skills_sync_tools.py` wraps both scripts (`target="all"` = the derived
  mirrors; `DEVTOOLS_MCP_SKILLS_ROOT` overrides the library location) — see the
  `skills-sync` skill

Contents: 52 skills = 24 harvested (debug / profiling / code-intel /
project-drivers / narrative) + 28 authored (`powershell/` 5.1 & 7 side by side,
`profiling/` incl. `renderdoc-frame-analysis`, `devtools/` incl. `skills-sync`,
`tracker/`, `collab/`); 23 commands (build / llm-station); 3 agents (docs /
testing / integration).
Harvested items are copied from upstream projects, never moved. Full breakdown and
dedup notes in `skills/README.md`.

## Conventions

- Python 3.12+; scripts follow Tiger Style (bounded loops, ≥2 asserts/function,
  explicit, fail-loud on invariant violations).
- Lint gate (all must pass): `uv run ruff check src tests`, `uv run black --check
  src tests`, `uv run mypy src/devtools_mcp`. Config in `pyproject.toml`; mypy has
  a ratchet list of legacy modules with `ignore_errors` — fix one, remove it from
  the list, never add to it.
- To add/refresh a harvested asset: edit `skills/sources.toml`, run
  `python skills/harvest.py`, then `python skills/sync.py --target <t>`.
