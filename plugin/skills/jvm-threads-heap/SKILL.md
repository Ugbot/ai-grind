---
name: jvm-threads-heap
description: >
  Diagnose JVM thread state and memory via the devtools-mcp jvm backend:
  thread dumps (jstack / Thread.print) and heap histograms (jmap / GC.class_
  histogram). Use when a Java process hangs, deadlocks, is stuck/blocked, or its
  heap is growing, to see thread states, lock contention, deadlocks, and the
  classes retaining the most bytes. For CPU/alloc sampling use jvm-profiling.
---

# JVM threads & heap (devtools-mcp `jvm:threads` / `jvm:heap`)

Point-in-time JVM diagnosis by PID, with no sampling window needed. Output is
parsed into queryable Polars frames, so you triage from a bounded summary and
drill in.

## Thread dump: hangs, deadlocks, contention

```
devtools_run(suite="jvm", tool="threads", binary="<pid>")
```

The summary shows the thread-state breakdown, flags a **deadlock** if the JVM
reports one, and lists blocked/waiting threads with their top frame. Then:

```
devtools_analyze(run_id, kind_pattern="BLOCKED|WAITING")     # who's stuck
devtools_analyze(run_id, group_by="state")                   # state histogram
devtools_analyze(run_id, function_pattern="lock|park|synchronized")
```

What to look for:
- **Deadlock** banner → two threads each holding what the other wants.
- Many threads **BLOCKED** on the same monitor → lock contention; find the holder.
- A pool of threads **WAITING** on a queue → usually fine (idle workers).
- Take two dumps a few seconds apart. Frames that don't move are truly stuck.

## Heap histogram: what's eating memory

```
devtools_run(suite="jvm", tool="heap", binary="<pid>")
```

Uses `GC.class_histogram` (a live-set count). The summary lists the top classes by
retained bytes + the total. Then:

```
devtools_analyze(run_id, sort_by="bytes")                    # biggest consumers
devtools_analyze(run_id, function_pattern="com\\.myapp")     # your classes only
```

What to look for:
- `[B` / `[C` (byte/char arrays) dominating → strings/buffers; trace who holds
  them.
- A growing custom class across two snapshots → a leak suspect.
- Counts in the millions for a small wrapper class → boxing / per-item overhead.

## Notes

- These attach to a **live** JVM (PID via `binary` or `--pid N`); the JDK's
  `jcmd`/`jstack`/`jmap` must be on PATH.
- A heap *histogram* is cheap; a full **heap dump** (`jmap -dump`) is heavy and
  not exposed here. Capture one manually if you need object-graph analysis.
- For *why* CPU is hot rather than *where threads are stuck*, use
  [[jvm-profiling]]. Overall workflow: [[devtools-mcp-usage]].
