# Project map: ai-grind

A unified developer-tooling workspace. Two concerns live here:

1. `devtools-mcp`, an MCP server giving AI assistants a normalized,
   Polars-backed interface over Valgrind, LLDB, DTrace, perf, Windows ETW
   (PerfView), Intel VTune (hotspots, threading, memory, uarch, snapshot), the
   JVM (JFR, jstack, jmap, async-profiler), the Windows debugger (CDB), Python
   (py-spy, cProfile), Node and JavaScript (V8 cpu/heap), RenderDoc (GPU frame
   capture and headless replay: drawcall tree, GPU counter timings, resources,
   thumbnails, GPU-time flame graphs), and the Maven, Gradle, npm, pnpm, yarn,
   and Cargo build and package systems under uniform verbs
   (build/test/deps/sync/audit/outdated/tasks). It also gives you
   cross-platform flame graphs (SVG and text), a browser visualization terminal
   for humans (`devtools_dashboard`), and a persistent SQLite-backed progress
   tracker. The tracker is a mini-JIRA behind `tracker_*` tools: projects,
   hierarchical tasks with PROJ-123 keys, acceptance criteria linked to tests,
   commit links, auto-tag rules, GitHub issue sync. Core rule: never flood the
   LLM with raw output, return bounded summaries plus a queryable frame (see the
   `devtools-mcp-no-token-flood` memory).
2. The skills library, one home for every Claude Code skill, command, and agent
   harvested from local projects, plus the skills written here (PowerShell,
   profiling, debugging).

## Layout

| Path | What |
|---|---|
| `src/devtools_mcp/` | MCP server: backends (`valgrind/ lldb/ dtrace/ perf/ etw/ vtune/ jvm/ cdb/ py/ node/ renderdoc/ maven/ gradle/ npm/ pnpm/ yarn/ cargo/`), `debug/` unified cross-language debugger (hand-rolled async DAP client `protocol.py`; protocol-agnostic `DebugSession` ABC plus session TREE and manager `session.py`; `dap_session.py` with runInTerminal and startDebugging reverse requests; adapters `debugpy`/`lldb-dap`/`js-debug`/`kotlin`/`java`(jdt.ls); `adt/` SAP ABAP debugger over ADT REST, no DAP; stop pipeline `snapshot.py` auto-captures stack, locals, watches, and a diff per stop as queryable runs; `plans.py` multi-stop sweeps; tools stay `debug_start/debug/debug_inspect/debug_stop`), shared `build/` and `flamegraph/` engines, `hotspots.py`, `viz/` browser terminal, `tools/`, `formatters/`, `models.py`, `registry.py` (capability model, `_BACKEND_MODULES` manifest loader, `InstallSpec`), `install.py` and `tools/install_tools.py` (`devtools_install` prints per-OS install commands, dry-run by default, execution gated by `DEVTOOLS_MCP_ALLOW_INSTALL=1`), `workspace.py`, `index.py`, `filters.py`, `server.py` |
| `src/devtools_mcp/renderdoc/` | GPU frame suite. Verbs: `capture` (targetcontrol auto-trigger, or launch-wait/F12), `analyze`, `counters`, `resources`, `thumb`. Replay runs `scripts/bridge.py` inside `qrenderdoc --python`, whose embedded Python 3.6 takes env-var params and returns JSON, with `sys.exit` suppressing the UI (ruff per-file-ignores). Replay verbs need a GPU and an interactive session |
| `src/devtools_mcp/station/` | Station sync, with llm-station-remote (`C:/code/llm-station-remote`, a FastAPI and Postgres platform) as the remote backend, local-first. `config.py` reads per-repo `.devtools-mcp/station.toml` rules with env over repo over global precedence, keeps lls_ keys env-only, and leak-checks them. `client.py` is the only platform-HTTP module (sync httpx, zero `llm_station` imports, test-enforced). `links.py` holds the row identity map and canonical hashes that suppress echoes. `diff.py` turns crdt_ops into the local change feed. `engine.py` runs the sync: watermarks, auto-pause at 10 failures, sync log. `domains/` carries tasks both ways with local-wins plus pending-intent creates, pushes coord sessions and mirrors handoffs, maps claims to advisory checkouts with TTL slack, pushes the skills manifest, and uploads perf runs with `local-run:` tag recovery. Tables live in tracker.db MIGRATION_V6. Tools: `station_link` (including `auth` and `logout`), `station_sync`, `station_session`. Browser auth: the dashboard `/station/auth` page hands off to platform OAuth (`?local_callback=` loopback redirect, platform-side GRIND-49), then `credentials.py` stores the key in `~/.devtools-mcp/station-auth.json`, and env `LLM_STATION_API_KEY` always wins. Auth instructions live in tool docstrings and errors; the `station-sync` authored skill sits disabled under `skills/authored/_disabled/` and reaches no mirror |
| `src/devtools_mcp/tracker/` | Progress tracker domain layer. `schema.py` holds versioned migrations. `db.py` is WAL SQLite at `~/.devtools-mcp/tracker.db`, overridable with `DEVTOOLS_MCP_TRACKER_DB`. Then `tasks.py`, `criteria.py`, `tags.py`, `commits.py`, `deps.py` (dependency edges and the execution-plan resolver), `issues.py`, and `frames.py` (Polars views). `crdt.py` and `sync.py` do local-first replication: HLC, op-capture triggers, LWW merge, HTTP peer sync through the dashboard's `/api/crdt/`. `activity.py` handles local agent collaboration through the v5 `file_activity` and `file_claims` tables: debounced touch log, advisory TTL claims, conflicts, all site-local rather than CRDT-synced. `providers/` covers GitHub REST via `GITHUB_TOKEN` and a GitLab stub. Tools live in `tools/tracker_tools.py` (11 `tracker_*` tools), the tracker card and `/collab` views in `viz/`, the skills in `skills/authored/skills/tracker/` and `collab/` |
| `tests/` | Test suite (735 cases) plus `tests/fixtures/`, whose compiled targets are gitignored |
| `.mcp.json` / `.cursor/mcp.json` | Client configs pointing at the shared local service (`http://127.0.0.1:8000/mcp`, streamable HTTP). One instance serves every project, and it is also registered at Claude Code user scope. Stdio spawn still works: `uv run devtools-mcp` |
| `scripts/devtools-service.ps1` | Run that shared instance: start, stop, status, install(-at-login). Network transports auto-start the dashboard on `:8765`; pass `--no-dashboard` to opt out |
| `scripts/unslop_check.py` | Flags AI writing tells in the markdown and Python this repo owns, skipping vendored and generated trees. Encodes the `unslop` skill as a check |
| `.claude-plugin/` | Claude Code plugin and marketplace manifests. `plugin.json` treats the repo root as the plugin root, so `${CLAUDE_PLUGIN_ROOT}` is the Python project, and points component paths at `plugin/`. `marketplace.json` lists the one `devtools-mcp` plugin with `source: "."`. Install with `/plugin marketplace add Ugbot/ai-grind`, then `/plugin install devtools-mcp@ai-grind` |
| `plugin/` | The committed flat plugin bundle (`skills/ commands/ agents/`) that `sync.py --target plugin` generates. The plugin loads this, because Claude does not recurse into the hierarchical `skills/catalog` and `authored` trees |
| `src/devtools_mcp/skilldocs/` | Live skills, meaning SKILL.md as a pycrdt text doc. `store.py` keeps an update log in `<data_root>/skilldocs.db`, compacts snapshots, and materializes variant-aware output to `~/.claude/skills/<name>/SKILL.md` (env `DEVTOOLS_MCP_LIVE_SKILLS_DIR`); patches use UTF-8 byte offsets, because pycrdt Text is byte-indexed. `sync.py` exchanges state-vector diffs through the dashboard's `/api/skilldoc/`. Dynamic skills come from `variants.py`, which renders `<!-- power:low\|high -->` blocks, and `control.py`, the `skill_control` LWW table for mode, overrides, and disabled entries (env `DEVTOOLS_MCP_SKILL_MODE`). `router.py` builds the auto-generated `skill-router` live skill: it indexes catalog, authored, and live skills between INDEX markers under live-editable rules, and a rebuild patches only the index. Tool: `skill_live` (create, get, list, append, patch, sync, publish, delete, route, mode, enable, disable) in `tools/skill_tools.py`, plus a free control panel at dashboard `GET /skills` and `/api/skilldoc/{route/rebuild,mode,enable,disable}` |
| `src/devtools_mcp/planning/` + `goap/` | A pluggable planner seam, optional and severable: everything works without it. `goap/` is the vendored regressive-A* GOAP core (agentix/GOAP, MIT). `planning/planner.py` resolves a backend from `DEVTOOLS_MCP_PLANNER`=none\|local\|platform\|url and hooks a native wheel. `local_backend.py` runs in-process GOAP over skills' ```goap descriptors, with cost scaled by power mode. `remote_backend.py` delegates to the platform or a URL. Tool: `plan` (goal, world, mode, layered, returns ordered skills) in `tools/plan_tools.py`, plus dashboard `POST /api/plan`. The canonical planner and Kahn layering are premium, living in the platform (`llm-station-remote`) |
| `hooks/` | Claude Code hooks under the plugin `hooks` key. `report_touch.py` (PostToolUse) reports edited files to the collab API and relays conflicts into context; `check_conflict.py` (PreToolUse) is optional and reads `DEVTOOLS_MCP_COLLAB_MODE=warn\|ask\|off`. Stdlib-only, never blocking. This is the local precursor to the team collab server, coming soon |
| `skills/` | The unified skills library. See `skills/README.md` |
| `pyproject.toml`, `uv.lock` | Python project metadata and lock |

## Skills library (`skills/`)

Three trees: `catalog/` (harvested, hierarchical, regenerated), `authored/`
(skills written here, committed) and `loadable/` (the generated flat mirror
Claude loads). Two scripts and one map drive it:

- `sources.toml` is the explicit harvest work-list, mapping upstream paths to a
  type and category
- `harvest.py` copies upstream into `catalog/` and writes `MANIFEST.json` for
  provenance
- `sync.py --target local|plugin|agents|project|global` merges `catalog/` and
  `authored/` into a flat mirror. `plugin/` is committed; `.agents/` is
  gitignored and skills-only; `.codex/` and `.cursor/` are hand-written configs,
  not sync targets
- Through MCP instead: `skills_sync(action=status|harvest|sync, target=...)` in
  `tools/skills_sync_tools.py` wraps both scripts, where `target="all"` means the
  derived mirrors and `DEVTOOLS_MCP_SKILLS_ROOT` overrides the library location.
  See the `skills-sync` skill

Contents: 112 loadable skills, being 83 harvested plus 29 authored. The
harvested set is 4 local skills (debug, profiling, project-drivers) plus a
79-skill external superset (MIT and Apache) under
`planning/ build/ review/ ship/ web/ meta/ writing/ understanding/ principles/`
from addyosmani/agent-skills, obra/superpowers, mattpocock/skills,
anthropics/skills, and cursor/plugins. Clones live in
`C:/code/vendor-skills/`, attribution in `skills/THIRD_PARTY_SKILLS.md`.
Authored: `powershell/` (5.1 and 7), `profiling/`, `devtools/` including
`skills-sync`, `tracker/`, `collab/`, and `meta/skill-router.rules.md`. Plus 5
commands and 6 agents.

Sidelined skills are harvested into `catalog/` for reference but never synced to
the mirrors or indexed by the router, which `sync.sidelined()` enforces:
`experimental/*` (the Story Engine `se-*` skills), the four retired
`llm-station-*` skills under `_disabled/`, and `_archive`. Harvested items are
copied from upstream, never moved. Full breakdown in `skills/README.md`.

## Conventions

- Python 3.12+; scripts follow Tiger Style: bounded loops, two or more asserts
  per function, explicit, fail loud on an invariant violation.
- Lint gate, all of which must pass: `uv run ruff check src tests`, `uv run black
  --check src tests`, `uv run mypy src/devtools_mcp`. Config sits in
  `pyproject.toml`. mypy carries a ratchet list of legacy modules with
  `ignore_errors`: fix one, remove it from the list, never add to it.
- Prose gate: `python scripts/unslop_check.py` before committing docs, skill
  text, or comments.
- To add or refresh a harvested asset: edit `skills/sources.toml`, run
  `python skills/harvest.py`, then `python skills/sync.py --target <t>`.
