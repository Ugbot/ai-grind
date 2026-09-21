---
name: python-profiling
description: >
  Profile and inspect Python programs via the devtools-mcp py backend: py-spy
  (sampling CPU flame graphs, attach to any running process), py-spy dump (thread
  stacks of a hung process), and cProfile (deterministic call stats). Use when
  Python code is slow, stuck, or you want a flame graph or per-function timing.
  cProfile is stdlib (works now); py-spy is `pip install py-spy`.
---

# Python profiling (devtools-mcp `py:*`)

Three complementary tools, normalized into queryable frames + flame graphs.

| tool | what | needs |
|---|---|---|
| `cpu` | low-overhead **sampling** CPU profile (py-spy) → flame graph; attach to a live PID or launch a script | py-spy |
| `threads` | one-shot **thread stacks** (py-spy dump) of a running process (the Python "jstack") | py-spy |
| `cprofile` | **deterministic** per-function stats (ncalls/tottime/cumtime) | stdlib |

## Sampling profile (py-spy): the flame graph

```
# attach to a running process for 20s:
devtools_run(suite="py", tool="cpu", binary="<pid>", extra_args=["--duration","20"])
# or launch a script and profile it:
devtools_run(suite="py", tool="cpu", binary="C:/path/app.py", args=["--flag"])
devtools_flamegraph(run_id="...")            # SVG + text flame-tree
```

py-spy samples the C stack of the Python interpreter without modifying or pausing
your program meaningfully, which is ideal for production and for code already running.
Install once: `pip install py-spy`.

## Thread dump (py-spy dump): hangs and deadlocks

```
devtools_run(suite="py", tool="threads", binary="<pid>")
devtools_analyze(run_id="...", kind_pattern="active")   # which threads are running
```

Shows every thread's current Python stack. Take two dumps a few seconds apart,
frames that don't move are stuck (a lock, a blocking I/O, an infinite loop).

## Deterministic profile (cProfile): exact call counts

```
devtools_run(suite="py", tool="cprofile", binary="C:/path/script.py", args=["..."])
devtools_analyze(run_id="...", sort_by="cumtime")        # hottest by cumulative time
devtools_analyze(run_id="...", sort_by="tottime")        # hottest excluding callees
devtools_analyze(run_id="...", function_pattern="mymod") # your code only
```

cProfile instruments every call, giving exact `ncalls`, `tottime` (self), `cumtime`
(incl. callees). Higher overhead than sampling, so use it on a representative
workload, not a hot production server.

## Sampling vs deterministic

- py-spy (sampling) has near-zero overhead and attaches to live or prod processes,
  gives flame graphs, may miss very rare calls. Reach for this first.
- cProfile (deterministic) gives exact counts and timings, but overhead distorts
  microbenchmarks and it can't attach (must launch the script).

See [[flamegraph-reading]] for interpretation and [[devtools-mcp-usage]] for the
overall workflow. For JVM use [[jvm-profiling]]; for JS use [[js-node-profiling]].
