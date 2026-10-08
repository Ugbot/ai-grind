---
name: devtools-mcp-usage
description: >
  Discover and use the devtools-mcp kitbag for native, JVM, Python and Node
  profiling, interactive debugging, build-tool diagnostics and stored-run
  analysis. Select the correct backend, scope collection to the target, and
  query bounded evidence. Also routes tracker, recipes, skill maintenance,
  plugins and Station coordination to their own tool families.
---

# Using devtools-mcp

Start from the current MCP tool schemas and `devtools_check()`. Registered
capability, executable detection, and a successful target capture are different
facts. Installation alone does not establish privileges, adapter startup,
project wrappers or working symbols.

Read [references/kitbag.md](references/kitbag.md) for the complete tool-family
inventory, backend verbs, target types and platform-specific caveats. Rediscover
when the server changes; do not assume a skill mentioning a separate connector
makes that connector part of this server.

## Choose and collect evidence

1. State the question, workload, target and measured phase. For latency, keep an
   uninstrumented benchmark alongside profiling; samples explain cost but do
   not measure p99. Match build, data, durability, worker counts, cache state and
   background work between baseline and candidate.
2. Inspect the selected tool's schema. `devtools_run` takes `suite`, `tool`,
   `binary`, `args`, `extra_args`, `timeout`, `working_dir`, `env`, plus run
   metadata. Native `binary` is an executable; build suites use a project
   directory; JVM/attach tools may use a PID; replay tools use an artifact.
   Backend handling of arguments/environment differs: consult its runner or
   dedicated skill when correctness depends on those settings.
3. Set `label`, `notes`, `task_key` and useful `tags`. Record the exact command,
   build and dataset identity. `parent_run_id` and `batch_id` can link a campaign.
   Bound duration/output and preserve the run ID. Missing tools:
   `devtools_install(suite="…", execute=false)` returns installation guidance;
   discovery is not a reason to install everything or run mutation tools.
4. Ask `devtools_query(run_id="…", columns=["schema"])` before selecting metric
   columns. Frame schemas vary. For custom DTrace stack-only traces, the
   aggregation frame can be empty while stored stacks are valid; go directly
   to `devtools_flamegraph` in that case. Use `devtools_analyze` / `devtools_query` with
   filters, sorting and limits; select explicit columns with `devtools_query`. `devtools_analyze(group_by=…)`
   ranks groups by row count, which may differ from cost.
5. For sampling, use `devtools_flamegraph(run_id="…", fmt="text", top_n=15)`.
   `fmt="data"` gives bounded folded stacks; the current tool has no SVG output.
   The dashboard provides interactive rendering. `stack_include` /
   `stack_exclude` select whole call paths before normalization—use these to
   separate query work from compaction sharing the same leaf functions.
6. Compare like-for-like runs with
   `devtools_compare(run_id_a="…", run_id_b="…")`; use `devtools_aggregate` for
   campaign patterns and `devtools_correlate(run_id_a="…", run_id_b="…",
   join_on="function")` for compatible frame joins. Record absolute benchmark
   time separately from sample percentages and instrumented execution time.
7. Export useful evidence with `devtools_export` (`bundle`, `parquet`, `json`),
   attach results to tracker acceptance criteria, and preserve exact failures.
   `devtools_raw` is a last resort and truncates at 200,000 characters; use the
   exported artifact when complete raw evidence is needed.

## Targeted native work

- **macOS CPU:** on runners containing `15d2d0e` (the `_target_pred` and
  direct-attach fixes), use `dtrace:cpu` with `binary` and target arguments.
  Built-in CPU/syscall probes then scope to the target; nonempty target `env`
  uses direct launch followed by PID attach, which may miss early startup.
  The local source audited on 2026-10-08 predates those fixes: use the explicitly
  scoped custom trace in the reference and verify effective settings there.
  Check the connected runner, not package version alone.
- **Linux CPU/cache:** `perf:cpu`, `perf:stat`; callgrind/cachegrind for work-count
  attribution. Use memcheck for access errors/leaks, massif for heap growth,
  helgrind/drd for race investigation. Instrumented runtimes are not speed gates.
- **Windows CPU:** `etw:cpu` or `vtune:cpu`; VTune `uarch`, `memory`, `alloc`,
  `threads` answer different questions. There is no `etw:alloc` or `etw:io` verb.
- **First failure/state:** `debug_start` → `debug` / `debug_inspect` →
  `debug_stop`, with explicit breakpoints/watches and bounded debug plans.
  Legacy `lldb` and `debug` suites redirect to these session tools. Capture
  errors and live objects before cleanup destroys them. A debugger stop flame
  graph counts stops, not CPU time. If an adapter cannot initialize, preserve
  the failure and use the native debugger rather than repeating the same call.
- **Allocation/I/O:** CPU stacks in malloc or read are clues, not byte/count or
  stall-duration proof. Use explicit allocation counters or syscall
  entry/return timing. Custom DTrace aggregation units must be recorded; its
  parsed `count` label does not establish the units of a custom sum.

## Discovery, service health and the rest of the kitbag

Use `plugins(action="list")` or `plugins(action="status")` for registered extensions and failures.
There are backend, MCP-tool, console-page and debug-adapter extension groups;
the current plugin status tool does not expose the last group's failure map.
Use `devtools_dashboard(action="status")` to inspect an existing console.
Defaults are MCP `http://127.0.0.1:8010/mcp` and dashboard
`http://127.0.0.1:8765`; configured endpoints take precedence.
Tracker, recipe, Station and skill operations return domain summaries, not
profiler frames. Route them using the reference; inspect mutation semantics
before executing a recipe, sync, install, export, publish or deletion.

If client tools are not surfaced, use the installed MCP SDK against the
configured HTTP endpoint or console-script stdio transport. Set a client
timeout. Use the installed `devtools-mcp` entrypoint: `python -m devtools_mcp.server` can create a second module instance with zero tools.
Do not bypass the tracker with direct SQLite writes. A listening socket is not
proof of a responsive server. Preserve timeout/stack evidence before recovery;
consider other clients before restarting a shared service. A temporary stdio
session with isolated run/recipe data can audit tool registration without
restarting it. See the reference for state isolation and offline discovery.
