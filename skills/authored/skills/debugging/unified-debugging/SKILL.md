---
name: unified-debugging
description: >
  Interactive cross-language debugging through devtools-mcp's unified debug
  tools (debug_start/debug/debug_inspect/debug_stop): one verb vocabulary over
  Python (debugpy), C/C++/Rust (lldb-dap), JS/Node (js-debug), Kotlin/JVM
  (kotlin-debug-adapter), Java (jdt.ls+java-debug), and ABAP (SAP ADT REST).
  Use whenever a program needs breakpoints, stepping, variable inspection,
  watches, or attach-to-process in ANY of those languages, including mixed
  codebases, instead of print-debugging. Every stop auto-captures a queryable
  snapshot with a diff vs the previous stop; debug plans sweep many stops in
  one call.
---

# Unified debugging

One interface, many implementations. You never pick a protocol, you pick a
program (or a PID/port), and the server routes to the right debug adapter.
The AI workflow is IDENTICAL across languages: same four tools, same verbs,
same snapshot/diff/query surface.

## The loop

1. `debug_start(program=...)` launches. The adapter is sniffed from the file
   (`.py` → debugpy, native binary → lldb-dap, `.js/.ts` → js-debug, `.kt` →
   kotlin) or forced with `language=`/`adapter=`. Attach instead with `pid=`
   or `attach_host=`/`attach_port=` (debugpy --listen, JDWP, node --inspect).
   Pass `breakpoints=[{source, line, condition?}]` and `watches=[...]` up
   front to save round trips. Returns a session id + capabilities line.
2. `debug(session_id, action=...)` carries the verbs:
   - Execution: `continue`, `pause`, `step` (into), `next` (over), `finish`
     (out), `kill`. Execution verbs WAIT for the next stop and return its
     auto-captured summary: stop reason, top frames, locals, watch values,
     and a "Δ since last stop" diff line. One call = one stop, fully
     inspected — don't follow a step with inspection calls unless you need
     to drill deeper.
   - Breakpoints: `breakpoint` (location="file:line" or a function name;
     `condition=`, `hit_condition=">= 5"`, `log_message=` makes a logpoint),
     `breakpoint_delete`, `exception_breakpoints` (filters= from
     capabilities).
   - Watches: `watch_add`/`watch_remove`/`watch_list`, server-evaluated at
     EVERY stop, snapshotted, diffed.
   - State: `set_variable` (variable="config.retries", value="5"),
     `thread_select`, `frame_select`.
   - Escape hatch: `command` (raw_command=...) reaches the debugger's native REPL
     (lldb commands, Python expressions, ...).
3. `debug_inspect(session_id, what=...)` takes deeper looks, each stored as a
   queryable run: `stack`, `variables` (expand="obj.field" drills the tree),
   `watches`, `threads`, `breakpoints`, `registers`, `expression`, `memory`,
   `disassemble`, `output` (program stdout/stderr), `diff` (vs any earlier
   stop's run_id), `capabilities`, `snapshot` (full re-capture).
4. `debug_stop(session_id)` tears down. Multi-stop sessions emit a
   session-summary run: `devtools_flamegraph(run_id)` renders WHERE the
   session stopped across all stops.

## Debug plans: many stops, one call

For sweeps ("capture state at this breakpoint for the first 10 iterations"),
don't step interactively. Pass a plan to `debug_start(plan=...)` or
`debug(action="plan", plan=...)`:

```json
{"breakpoints": [{"source": "/abs/path.py", "line": 42, "condition": "n > 0"}],
 "watches": ["total", "queue.size()"],
 "max_stops": 20, "per_stop": "continue",
 "until": "total > 1000", "time_budget_s": 120}
```

Returns a bounded per-stop table (location, watch values, Δ count, run_id per
stop). The session stays live wherever the plan halts, so continue
interactively. `until` hands control back the moment the expression goes
truthy at a stop.

## Snapshots are runs

Every stop writes a `DebugSnapshot` run (suite="debug", grouped by
batch_id=session id). Variables are FLATTENED with a dotted `path` column,
`devtools_query(run_id, filter="path.str.contains('config')")` finds any
nested value; the `diff` frame answers "when did X change". The stop summary
already shows the important bits; query only when you need more.

## Per-language notes

- **Python (debugpy)**: debugpy must be importable by the TARGET interpreter
  (venv detection is automatic; override with extra={"python": "..."}).
  Debug a module with extra={"module": "pkg.mod"}. Attach by pid injects.
- **C/C++/Rust (lldb-dap)**: needs debug info (-g -O0). macOS attach/launch
  needs developer mode (`DevToolsSecurity -status`); override the binary
  with $DEVTOOLS_LLDB_DAP. Raw lldb commands still work via
  debug(action="command", raw_command="`<lldb cmd>").
- **JS/Node (js-debug)**: stops arrive in CHILD sessions (the session tree
  handles this invisibly; summaries say "stopped in child"). Requires node +
  the js-debug bundle (devtools_install(suite="debug", tool="js-debug")).
- **Kotlin (kotlin-debug-adapter)**: launch needs extra={"main_class": ...}
  and a COMPILED Gradle/Maven project (run the build first); program args
  are unsupported, so prefer JDWP attach (attach_port=5005 with
  -agentlib:jdwp=...,address=*:5005).
- Java (jdt.ls + java-debug): first import of a project takes 30 to 60s
  (the jdt.ls process is cached per project after that). Kotlin-only Gradle
  projects resolve poorly, so use the kotlin adapter there.
- **ABAP (ADT REST, attach-only)**: debugging a SAP system needs
  base_url/client/credentials in extra; set external breakpoints, then
  trigger the code path in the system (there is no "launch"). One debugger
  listener per user per system, so a live Eclipse ADT session for the same
  user conflicts.

## Install

`devtools_check()` shows per-adapter availability under the `debug` suite.
`devtools_install(suite="debug", tool="<adapter>")` prints (or runs) the
per-OS install commands. Each adapter installs separately.
