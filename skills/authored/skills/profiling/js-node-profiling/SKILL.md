---
name: js-node-profiling
description: >
  Profile Node.js / JavaScript via the devtools-mcp node backend — V8 CPU
  profiles (--cpu-prof) and sampling heap profiles (--heap-prof), turned into
  flame graphs and queryable hotspot tables. Use when a Node script/server is
  slow (CPU) or memory-hungry (allocations) and you want to see which functions
  are hot or allocating. Needs Node.js on PATH; nothing else.
---

# Node.js / JavaScript profiling (devtools-mcp `node:*`)

Runs a Node script under V8's built-in profilers and parses the emitted profile
into the same flame-graph + Polars-table pipeline as every other backend.

| tool | what | output unit |
|---|---|---|
| `cpu` | `node --cpu-prof` — sampled CPU profile (`.cpuprofile`) | samples |
| `heap` | `node --heap-prof` — sampled allocation profile (`.heapprofile`) | bytes |

## CPU profile → flame graph

```
devtools_run(suite="node", tool="cpu", binary="C:/path/server.js", args=["--port","3000"])
devtools_flamegraph(run_id="...")                 # interactive SVG + text tree
devtools_analyze(run_id="...", sort_by="exclusive")  # hottest functions
```

Width = inclusive time; high **Exc%** = where the CPU actually is. Node's own
startup/internal frames appear too — filter to your code with
`function_pattern=...` or just read the app frames in the flame graph.

## Heap (allocation) profile → where bytes come from

```
devtools_run(suite="node", tool="heap", binary="C:/path/app.js")
devtools_flamegraph(run_id="...")                 # flame graph weighted by BYTES
devtools_analyze(run_id="...", sort_by="exclusive")  # top allocation sites
```

The heap profile is a **sampling allocation** profile: each frame's weight is
bytes allocated there. Wide frames = allocation hotspots; chase their callers to
cut churn / GC pressure.

## Notes

- Both tools **launch a script** (`binary` = path to a `.js`/`.mjs`). The profile
  is captured for the life of that process, so make the script exercise the slow
  path (or add a load loop) before it exits.
- Profiling a long-running server: run it with a load generator and stop it, or
  wrap the hot path in a short driver script.
- These are the same `.cpuprofile`/`.heapprofile` files Chrome DevTools produces,
  so anything you capture elsewhere can be dropped in later.
- For interpreting flame graphs see [[flamegraph-reading]]; for the tool workflow
  and the browser visualization terminal see [[devtools-mcp-usage]] and
  [[devtools-visualizer]]. Python: [[python-profiling]].
