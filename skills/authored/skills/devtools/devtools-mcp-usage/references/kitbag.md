# Kitbag reference

Schema/source snapshot: devtools-mcp 0.2.0, 2026-10-08. Refresh from the connected server when versions change. All 39 tool names and 18 suites below were also verified over a real MCP stdio session. The same tool signatures, suite declarations and verbs were verified read-only against upstream `c6fc8ab`. Both report version 0.2.0, but upstream contains DTrace fixes in `15d2d0e` that the audited local runner lacks. This is a routing reference, not a claim that every tool has been executed.


| Family | Registered tools |
|---|---|
| Run and tool inventory | `devtools_check`, `devtools_run`, `devtools_list`, `devtools_raw`, `devtools_delete_run`, `devtools_export`, `devtools_tag_run` |
| Analysis and campaign comparison | `devtools_analyze`, `devtools_query`, `devtools_aggregate`, `devtools_compare`, `devtools_search`, `devtools_correlate`, `devtools_flamegraph` |
| Interactive debugging | `debug_start`, `debug`, `debug_inspect`, `debug_stop` |
| Availability and visualization | `devtools_install`, `plugins`, `devtools_dashboard` |
| Planning and pipeline execution | `plan`, `recipe` |
| Skill maintenance | `skill_live`, `skills_sync` |
| Platform linking and coordination | `station_link`, `station_sync`, `station_session` |
| Tracker | `tracker_project`, `tracker_task`, `tracker_status`, `tracker_criteria`, `tracker_tag`, `tracker_commits`, `tracker_deps`, `tracker_issue`, `tracker_sync`, `tracker_files`, `tracker_query` |


## Complete backend suites and verbs

All suites derive `detect`, `run`, `frames`, `summary`. Optional tags below are the actual registered capabilities, not guarantees of successful runtime collection.

| Suite | Registered verbs | Optional capabilities |
|---|---|---|
| `cargo` | `build`, `check`, `test`, `deps`, `sync`, `audit`, `outdated` |  |
| `cdb` | `stacks`, `analyze`, `inspect` | flamegraph, install |
| `debug` | `debugpy`, `java`, `js-debug`, `kotlin`, `lldb-dap` | flamegraph, install |
| `dtrace` | `trace`, `syscall`, `cpu` | flamegraph |
| `etw` | `cpu` | flamegraph, install |
| `gradle` | `build`, `test`, `check`, `deps`, `sync`, `tasks`, `audit`, `outdated`, `insight`, `projects` |  |
| `jvm` | `cpu`, `alloc`, `threads`, `heap` | flamegraph |
| `lldb` | `lldb` |  |
| `maven` | `build`, `test`, `check`, `deps`, `sync`, `audit`, `outdated`, `insight`, `projects` |  |
| `node` | `cpu`, `alloc` | flamegraph |
| `npm` | `build`, `test`, `deps`, `sync`, `audit`, `outdated`, `tasks` |  |
| `perf` | `cpu`, `stat`, `annotate` | flamegraph |
| `pnpm` | `build`, `test`, `deps`, `sync`, `audit`, `outdated`, `tasks` |  |
| `py` | `cpu`, `threads`, `cprofile` | flamegraph, install |
| `renderdoc` | `capture`, `analyze`, `counters`, `resources`, `thumb` | flamegraph, install |
| `valgrind` | `memcheck`, `helgrind`, `drd`, `callgrind`, `cachegrind`, `massif` | install |
| `vtune` | `alloc`, `cpu`, `memory`, `snapshot`, `threads`, `uarch` | flamegraph |
| `yarn` | `build`, `test`, `deps`, `sync`, `audit`, `outdated`, `tasks` |  |

## Additional tool families

| Surface | Practical use |
|---|---|
| `devtools_aggregate` | Compare broad versus spiky costs across many sampling runs; normalized share per run, tags intersection, self/total metric, whole-stack filtering. |
| `devtools_search` / `devtools_correlate` | Search across stored runs; join compatible frame columns from two runs, e.g. allocations and CPU attribution. |
| Export / run tags | `devtools_export(format="bundle" / "parquet" / "json")` preserves artifacts; tagging labels campaigns and tracker associations. |
| `recipe` | Register/get/list/seed shell-step pipelines; run/dry-run/force; inspect runs/steps. Passing runs are cached while the recipe spec remains unchanged. |
| `plan` | GOAP skill ordering from JSON-string goal/world; configured planner decides availability/layering. It does not execute the skills. |
| `skill_live` | CRDT live skill creation/patch/append/materialization/sync/router/mode/enabling. |
| `skills_sync` | Separate static library status/discover/adopt/harvest/mirror sync. Editing a global mirror alone can be overwritten by harvest/sync. |
| Tracker | Projects, task hierarchy/status, acceptance tests, tags, commits, dependency resolver, issue bridging, replica sync and advisory file claims; bounded views via `tracker_query`. |
| Station | Auth/link configuration, dry-run or live domain sync and online sessions/handoffs. Network/mutation verbs have their stated effects; status/query verbs are discovery. |

## Native lookup / allocation / I/O / compaction selection map

| Question | First collection or inspection | Useful follow-up and limits |
|---|---|---|
| Where does lookup CPU time go on macOS? | Fixed upstream: `dtrace:cpu` with target; older runners/custom probes: target-scoped `dtrace:trace`; local `debug_start(adapter="lldb-dap", program=..., breakpoints=[...])` for state/correctness. | `devtools_flamegraph(..., stack_include=...)`, then schema-first query. DTrace detection means executable exists, not that sudo/probes can run. Built-in CPU target attribution caveat below matters. |
| Where does lookup CPU time go on Linux? | `perf:cpu`; `perf:stat` for cycles, cache and branch evidence; `perf:annotate` for existing perf.data instruction view. | Flame/compare use collected call stacks. Cachegrind/callgrind can diagnose work counts but instrumentation is not a production latency measurement. |
| Where does lookup CPU time go on Windows? | `etw:cpu` with matching symbols/PDBs, or `vtune:cpu`; VTune uarch/memory for Intel pipeline/cache causes. | ETW is CPU-only in this kitbag: there is no registered `etw:alloc` or `etw:io`. |
| Unexpected allocations / retained heap? | Linux `valgrind:massif` for heap growth and `valgrind:memcheck` for errors/leaks; supported VTune `alloc` maps to memory-consumption. On Mac use custom DTrace allocation aggregation or LLDB malloc breakpoints. | CPU sampling in malloc is not allocation bytes/count proof. DTrace allocation scripts need explicit probes/aggregation units; `agg_type` currently labels parsed numeric rows as count even for a custom sum. |
| File I/O / fsync / compaction stalls? | Target-scoped `dtrace:trace` custom syscall entry/return aggregation on Mac; Linux `perf:stat`/custom supported perf event args plus appropriate system trace tooling. | Built-in `dtrace:syscall` reports counts, not latency/bytes or file offsets. No dedicated registered native disk-I/O suite. Add an explicit entry/return timing D program and use bounded aggregation if duration is the question. |
| Is compaction stealing CPU from reads? | Mixed-load capture including read/compaction thread call paths; isolate read or compaction stacks and compare like-for-like baseline/candidate. | Use `stack_include` / `stack_exclude` on flamegraph, compare and aggregate, **before percentage normalization**. Function-name exclusion alone cannot distinguish shared memcpy/malloc leaves across phases. |
| Memory access fault, race, or corruption? | `debug_start` + `debug_inspect` snapshots/watches/conditional breakpoints; Linux memcheck, helgrind or drd; CDB crash analysis on Windows. | Bounded debug plans can collect many stops; stop timing and profiler overhead do not establish p99. |
| p99 regression? | Existing workload benchmark with identical build, data, settings, load and durability/compaction state, plus profiling of the same phase. | Profilers explain attribution; a sample percentage is not an absolute latency or p99 improvement. Keep benchmark evidence separately. |

## Concrete stale claims and schema hazards

1. **Previous usage skill says `devtools_flamegraph(run_id)` writes an SVG. Current MCP tool does not.** `flame_tools.py` accepts only `fmt="text"` (bounded ASCII tree/hotspot table) or `fmt="data"` (up to 2,000 heaviest folded stack JSON rows). Full interactive rendering belongs to the dashboard. `fmt="svg"` is rejected.
2. **Previous skill omits substantial kitbag surfaces.** Its frontmatter limits usage to native/JVM and its table omits Python (`py:cpu/threads/cprofile`), Node (`node:cpu/alloc`), six build backends, VTune threads/alloc/snapshot, Valgrind massif/thread checks, DAP adapters, aggregate, export, recipes, tracker, station, plugin health and skill maintenance. Its omission is not proof those tools are unavailable.
3. **Current server instruction string is itself stale:** it advertises Node `cpu/heap`, but actual registered Node verbs are `cpu/alloc`. `node:heap` fails availability. Likewise words jfr/asprof/pyspy/dump/hotspots/threading are underlying CLI names, not registered suite verbs; use current table.
4. **Comparisons/correlation take `run_id_a` and `run_id_b`.** Previous skill `devtools_compare(a,b)` / `devtools_correlate(a,b,...)` is positional shorthand, not actual named MCP JSON. Use named fields in new examples. `devtools_query(columns=["schema"])` is valid.
5. **Frame schemas differ by suite.** ETW columns are `function,module,exc_pct,inc_pct,exc,inc,value`; perf CPU uses `symbol,function,shared_object,command,overhead_pct`; DTrace CPU uses `function,module,count,frame_count,top_frame`; DTrace aggregation uses `value,agg_type,key_N,function`. Do not prescribe a universal `exclusive_pct`/`self_pct` to `devtools_query`; request schema first. `self_pct/total_pct` appear in flame/campaign-derived function frames.
6. **`devtools_analyze(group_by=...)` ranks by group row count**, and sums up to five numeric columns; it does not rank by requested cost after grouping. Use ungrouped sort by the real metric or a dedicated flame/aggregate view when cost ranking is intended.
7. **Legacy LLDB suite is session-only despite `run` capability.** Registered `lldb:lldb` differs from detector name `lldb:lldb-dap`; its backend run explicitly redirects to `debug_start`. `debug` suite similarly redirects batch callers. Use the four debugger tools, not `devtools_run(suite="lldb",...)`. Kotlin detection name differs from adapter name too.
8. **DTrace older-runner caveat:** the installed 2026-10-08 audited runner lacked target predicates for built-in CPU/syscall when launching a binary. Upstream commit `15d2d0e` fixes those predicates and nonempty target environment propagation for built-in verbs. Confirm the connected source, since package version remains 0.2.0. Custom trace D programs still require their own target predicate and launch/attach arguments; custom trace plus `-c` does not use the fixed direct-launch environment path. With no target, built-in whole-machine capture remains possible. The runner uses noninteractive sudo and reports permission failures.

9. **Native raw cap remains valid:** `devtools_raw` reads/truncates at 200,000 characters; it is not a schema-first substitute. General statements that every result is a frame need qualification: pipeline/tracker/planner/plugin tools return domain summaries; run/profiler outputs are the Polars workflow.
10. **Console entrypoint matters.** Protocol audit found `python -m devtools_mcp.server` may create a separate `__main__` FastMCP instance with zero attached tools through circular imports. Use the installed `devtools-mcp` console script (or import the canonical module for offline registration), not `-m`, until fixed.


## Custom / older-runner DTrace command shape

```json
{
  "suite": "dtrace", "tool": "trace", "binary": "/path/to/native-bench",
  "args": ["profile-997 /pid == $target/ { @[ustack()] = count(); }"],
  "extra_args": ["-q", "-c", "/path/to/native-bench query /path/to/fixture"],
  "timeout": 120, "label": "lookup CPU",
  "notes": "Exact query phase, maintenance paused; instrumented time unscored",
  "task_key": "PROJ-123"
}
```

`dtrace:trace` selects an aggregation frame builder. A stack-only custom D
program can therefore have an empty query/analyze frame while
`devtools_flamegraph` still reads valid stored `run.stacks`.

`trace` treats args as the D program; `extra_args` launches the command here.
Quote executable/argument paths for the tool's command parser when they contain
spaces. Avoid launching SIP-protected wrappers such as `/usr/bin/env` on macOS.
The runner uses noninteractive sudo; permissions/SIP can reject a probe family
even when CPU sampling works. For this custom `trace -c` shape, sudo may strip environment settings; verify
the target configuration. On fixed runners prefer built-in `cpu`/`syscall`
with nonempty `env` for environment-gated targets: they launch as the calling
user and attach by PID, possibly missing early startup work. Do not weaken system security to collect
a profile.

## MCP and offline discovery

Read the client's current tools/list through its SDK; save schemas outside model
context and print only names or selected schemas. Bound initialization and tool
calls. Prefer the configured service. For a temporary stdio audit, launch the
installed `devtools-mcp --transport stdio --no-dashboard` console script. Use a
temporary `DEVTOOLS_MCP_DATA` for isolated run/recipe state; a separate
`DEVTOOLS_MCP_DBOS_DB` may isolate recipe executor state. If authorized tracker
work must use the existing tracker, explicitly set `DEVTOOLS_MCP_TRACKER_DB` to
that existing store; otherwise leave the audit isolated. Stop the owned session
when finished. A fresh process sees on-disk source changes that a long-running
service may not yet have loaded.

For a read-only source audit, import the canonical `devtools_mcp.server.mcp` and
call its `list_tools()` without entering the server lifespan. Inspect
`registry.list_backends()` / `get_backend()` and installed entry points in
`devtools_mcp.backends`, `.mcp_tools`, `.viz_pages`, `.debug_adapters`. Check each
loader's failure map, including `failed_adapter_plugins()`. Executable detection
via `ToolRegistry.detect_all()` is a separate bounded check, not a profiler run.

## Related skills

Use the dedicated `tracker-usage` / `agent-collab`, `build-tools`,
`flamegraph-reading`, `devtools-visualizer`, `live-skills` / `skills-sync`,
`etw-profiling`, `vtune-profiling`, `python-profiling`, `js-node-profiling`,
`jvm-profiling`, `jvm-threads-heap`, `cdb-windows-debug`, and
`renderdoc-frame-analysis` skills when available and relevant. Their presence
does not establish the corresponding tool is installed.

## Target and metadata details

- JVM: `binary` is the JVM PID (or `--pid N` in `extra_args`); `--duration N`
  controls the sampling window.
- CDB: a live executable, or `--dump path.dmp` for crash analysis.
- ETW: `--decode-only --etl path.etl` reprocesses an existing trace.
- RenderDoc: capture takes an executable; replay verbs take the `.rdc` artifact.
- Tracker project/task descriptions should state what, why and done-when; run
  labels/notes/tags/task links populate dashboard cards.
- If an installation is explicitly requested, server-side execution also needs
  `DEVTOOLS_MCP_ALLOW_INSTALL=1`; default `execute=false` only gives guidance.
