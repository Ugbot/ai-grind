---
name: devtools-mcp-usage
description: >
  How to drive the devtools-mcp server, the normalized interface over Valgrind,
  LLDB, DTrace, perf, Windows ETW (PerfView), the JVM (JFR/jstack/jmap/async-
  profiler), and the Windows debugger (CDB), plus flame graphs. Use whenever you
  need to profile or debug a native or JVM program through devtools-mcp: pick a
  suite/tool, run it, then drill into the resulting Polars frame instead of
  reading raw output. Explains the no-token-flood query workflow.
---

# Using devtools-mcp

devtools-mcp wraps symbol-heavy perf/debug CLIs behind a few normalized MCP
tools. **The golden rule: it never dumps raw output at you.** Every run is stored
as a queryable Polars DataFrame; you get a small summary + a `run_id`, then query
precisely.

## The loop

1. `devtools_check()` shows which suites and tools are installed on this machine
   (per-suite capability tags, plus install hints for missing suites).
   Missing a tool? **`devtools_install(suite)`** returns the exact per-OS install
   commands (winget/apt/pip/download) to run in your shell; `execute=True` runs
   them from the server when `DEVTOOLS_MCP_ALLOW_INSTALL=1`.
2. `devtools_run(suite, tool, binary, args=…, extra_args=…)` runs a tool.
   Returns a bounded summary + `run_id`. Always set **`label`** (short title) and
   `notes` (what you profiled and why) appear on dashboard run cards at
   `http://127.0.0.1:8765`. Link runs to tracker work with **`task_key`** (`PROJ-123`).
3. `devtools_analyze(run_id, …)` and `devtools_query(run_id, columns=…)`:
   filter/group/sort/paginate the stored DataFrame. `columns=["schema"]` lists
   available columns first.
4. `devtools_flamegraph(run_id)` writes an SVG for any sampling run and shows a
   bounded text flame-tree.
5. **`devtools_compare(a, b)`** / **`devtools_correlate(a, b, join_on="function")`**
   Diff two runs, or join them (leaks ∩ CPU hotspots, for instance).

## Suites and what to reach for

| Goal | suite:tool |
|---|---|
| Native CPU profile on **Windows** | `etw:cpu` (PerfView) or `vtune:cpu` |
| Deep Intel analysis (top-down uarch, memory-bound, false sharing) | `vtune:uarch` / `vtune:memory` (see vtune-profiling skill) |
| Native CPU/syscall profile on macOS | `dtrace:cpu` / `dtrace:syscall` |
| Native CPU profile on Linux | `perf:cpu` / `perf:stat` |
| Memory errors / leaks (Linux) | `valgrind:memcheck` |
| Cache/branch/callgraph (Linux) | `valgrind:cachegrind` / `callgrind` |
| **JVM** CPU/alloc profile | `jvm:cpu` (JFR) / `jvm:alloc` (async-profiler) |
| JVM thread dump / deadlocks | `jvm:threads` |
| JVM heap histogram | `jvm:heap` |
| **Windows** crash dump / live debug | `cdb:analyze` / `cdb:stacks` |
| Step-through debug (macOS/Linux) | `debug_start` + `debug` (LLDB) |
| **GPU frame** capture + drawcall/timing analysis | `renderdoc:capture` then `renderdoc:analyze` / `counters` (see renderdoc-frame-analysis skill) |

## Targets

- Native tools take `binary` = path to the exe (+ `args`).
- **JVM** tools take `binary` = the **PID** of a running JVM (or `--pid N` in
  `extra_args`); `--duration N` sets the sampling window.
- **CDB** debugs a live exe (`binary`) or a crash dump (`--dump path.dmp` in
  `extra_args`).
- **ETW** can re-decode an existing trace with `--decode-only --etl path.etl`.
- **renderdoc** `capture` takes `binary` = the graphics .exe; the replay verbs
  (`analyze`/`counters`/`resources`/`thumb`) take `binary` = the produced `.rdc`.

## Why query instead of read

A perf/ETW/JFR run can contain tens of thousands of symbol-resolved nodes. Asking
for all of it wastes context and buries the signal. The summary gives you the
top-N; `devtools_analyze` lets you ask exactly what you need, for example group by
module, filter to your own namespace, sort by exclusive %, or sample. Reach for
`devtools_raw(run_id)` only as a last resort (it truncates at 200 KB).

## Dashboard metadata (cards)

The browser UI at `http://127.0.0.1:8765` shows **clickable cards** for runs and
tracker tasks. Populate them when you create work:

| Surface | Fields to set | Shown on card |
|---------|---------------|---------------|
| Runs | `label`, `notes`, `tags`, `task_key` on `devtools_run` | title, preview text, tags, task link |
| Tracker tasks | `description` on `tracker_task` create/update | title + description preview (3 lines) |
| Projects | `description` on `tracker_project` create | project card preview |

Use descriptions for what, why, and done-when in plain language, not the title repeated.

See also: [[flamegraph-reading]], [[etw-profiling]], [[jvm-profiling]],
[[jvm-threads-heap]], [[cdb-windows-debug]], [[renderdoc-frame-analysis]].
