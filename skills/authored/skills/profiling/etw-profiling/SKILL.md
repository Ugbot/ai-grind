---
name: etw-profiling
description: >
  CPU-profile native Windows programs via the devtools-mcp ETW backend (PerfView).
  Use when you need to find CPU hotspots in a Windows .exe — Exc%/Inc% tables,
  call attribution, and flame graphs. Covers the symbol-resolution prerequisites
  (PDBs, _NT_SYMBOL_PATH), the elevation/exit-code-2 gotcha, and how to read the
  output. The deep-dive complement to bench-rdtsc-profile (which says WHICH
  function; this says WHY).
---

# ETW CPU profiling on Windows (devtools-mcp `etw:cpu`)

The `etw` backend drives **PerfView** to capture an ETW CPU trace, resolves
symbols, and returns hotspot tables (Exc%/Inc%) as a queryable Polars frame — no
raw spew. Add a flame graph with `devtools_flamegraph`.

## Prerequisites (symbols are 90% of the battle)

1. **PerfView** at `C:\code\PerfView.exe`, on PATH, or set `$DEVTOOLS_PERFVIEW`.
2. **PDBs next to your exe.** A Release build with `/GL /LTCG` still needs `/Zi`
   at compile **and** `/DEBUG` at link, or every frame decodes to `module!?`.
   Verify a real (~MB) `.pdb` sits beside the `.exe`.
3. The backend sets `_NT_SYMBOL_PATH` to the exe dir + the Microsoft public symbol
   server, so system DLLs resolve (`ucrtbase!memset`, not `ucrtbase!?`).
4. Some kernel providers need **admin**; PerfView self-elevates. The parent
   process exits with **code 2** while the elevated child keeps writing the ETL —
   that's expected; the backend waits for the `.etl` to stabilise instead of
   trusting the exit code.

## Run it

```
devtools_run(suite="etw", tool="cpu", binary="C:/path/app.exe", args=["100000"])
```

Useful `extra_args`:
- `--process NAME` — focus process name (default = exe stem).
- `--decode-only --etl C:/x.etl` — re-decode an existing trace, no recapture.
- `--folded C:/stacks.txt` — feed folded stacks (async-profiler-style) so
  `devtools_flamegraph` can draw a true flame graph from ETW.

## Read it

The summary gives two bounded tables:
- **Hottest leaves (Exc%)** — where CPU cycles actually burn. Start here.
- **Top dispatchers (Inc%)** — functions whose time is in their callees.

Then drill in without flooding context:
```
devtools_analyze(run_id, function_pattern="myns::", sort_by="exc_pct")   # your code only
devtools_analyze(run_id, group_by="module")                              # cost by DLL
devtools_flamegraph(run_id)                                              # SVG + text tree
```

See [[flamegraph-reading]] for interpretation and [[devtools-mcp-usage]] for the
overall workflow. For sub-µs loops where sampling is too coarse, use inline RDTSC
(`bench-rdtsc-profile`) first to find the function, then ETW to see why.

## Gotchas

- `/Ob3` aggressive inlining hides small helpers in their callers — build with
  `/Ob1` if you need them separable.
- `module!?` rows in the table mean unresolved symbols — fix PDBs/symbol path,
  the numbers are otherwise an aggregate, not a real leaf.
- WPR/xperf are installed too, but PerfView is the supported capture path here.
