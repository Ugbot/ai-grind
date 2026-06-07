---
name: jvm-profiling
description: >
  CPU/allocation profile a running JVM via the devtools-mcp jvm backend — JFR
  (Java Flight Recorder, built into the JDK) or async-profiler. Use when a Java
  process is slow or allocating heavily and you need hot methods + a flame graph.
  Covers attaching by PID, choosing JFR vs async-profiler, and reading the
  method-level Exc%/Inc% output. For thread dumps and heap histograms see
  jvm-threads-heap.
---

# JVM profiling (devtools-mcp `jvm:jfr` / `jvm:asprof`)

Profiles a **live JVM by PID**. JFR ships with every modern JDK and needs no
extra install; async-profiler is a separate download with lower overhead and
native+Java stacks. Both produce a flame graph via `devtools_flamegraph`.

## Find the PID

```
jcmd -l            # or jps -l — lists running JVMs and their pids
```

## JFR (built-in, default choice)

```
devtools_run(suite="jvm", tool="jfr", binary="<pid>", extra_args=["--duration","20"])
```

The backend runs `JFR.start settings=profile duration=Ns`, lets it complete, then
`jfr print --json` and aggregates execution samples into per-method Exc%/Inc%.
`settings=profile` gives richer sampling than `default`.

## async-profiler (lower overhead, native+Java)

Install from github.com/async-profiler/async-profiler, then set `$DEVTOOLS_ASPROF`
(or put `asprof` on PATH):

```
devtools_run(suite="jvm", tool="asprof", binary="<pid>", extra_args=["--duration","20"])
```

It captures collapsed (folded) stacks directly — ideal flame-graph input.

## Read it

```
devtools_flamegraph(run_id)                                    # SVG + text flame-tree
devtools_analyze(run_id, function_pattern="com\\.myapp", sort_by="exclusive")
devtools_analyze(run_id, group_by="function")                 # hottest methods
```

The summary shows hottest methods (Exc% / Inc%) and the event breakdown. See
[[flamegraph-reading]] for interpretation.

## Choosing JFR vs async-profiler

- **JFR** — zero install, low overhead, also records GC/locks/IO events, safe in
  production. Java frames only by default.
- **async-profiler** — lowest overhead, **native + Java** stacks (sees JIT, GC,
  and C/C++ frames), supports `alloc`/`lock`/`cache-misses` events. Needs the
  agent. Prefer it when the cost might be below the Java layer.

## Gotchas

- Sampling needs the process to actually be *doing* the slow thing during the
  window — reproduce the load while profiling.
- `-XX:+FlightRecorder` is unlocked by default on JDK 11+; on older JDKs add
  `-XX:+UnlockCommercialFeatures`.
- Inlined hot methods may appear merged into callers; cross-check with the
  Inc%/Exc% split. See also [[jvm-threads-heap]] and [[devtools-mcp-usage]].
