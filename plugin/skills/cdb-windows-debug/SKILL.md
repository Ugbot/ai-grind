---
name: cdb-windows-debug
description: >
  Debug native Windows programs and crash dumps via the devtools-mcp cdb backend
  (batch-mode CDB). Use when a Windows .exe crashes, you have a .dmp file, or you
  need all-thread stacks / a crash bucket without a GUI debugger. Covers
  !analyze -v triage, ~*k thread stacks, register/locals inspection, installing
  cdb, and symbol setup. Headless and scriptable — the Windows analog of the
  LLDB debug tools.
---

# Windows debugging with CDB (devtools-mcp `cdb:*`)

The `cdb` backend runs **CDB** (the console Windows debugger) in **batch mode** —
a scripted `-c "...;q"` command sequence, captured and parsed into a structured
snapshot. No interactive console, so it's safe to run headless. Three tools:

| tool | CDB script | use for |
|---|---|---|
| `analyze` | `!analyze -v` | crash triage: exception code, faulting module, bucket |
| `stacks` | `~*kn` | every thread's backtrace (hangs, deadlocks, "where is it") |
| `inspect` | `kn; r` | current backtrace + registers |

## Install cdb

cdb ships with **Debugging Tools for Windows** (Windows SDK) or
`winget install Microsoft.WinDbg`. The backend finds it via `$DEVTOOLS_CDB`,
PATH, or the SDK `Debuggers\x64` folder. `devtools_check()` shows whether it's
available and how to install it.

## Triage a crash dump

```
devtools_run(suite="cdb", tool="analyze", binary="", extra_args=["--dump","C:/dumps/app.dmp"])
```

The summary surfaces the key `!analyze -v` fields — `EXCEPTION_CODE`,
`SYMBOL_NAME`, `MODULE_NAME`, `FAILURE_BUCKET_ID` — and the faulting thread's top
frames. Then drill in:

```
devtools_analyze(run_id, function_pattern="mymodule")      # frames in your code
devtools_flamegraph(run_id)                                # all thread stacks as a graph
```

## Debug a live exe

```
devtools_run(suite="cdb", tool="stacks", binary="C:/path/app.exe", args=["--flag"])
```

(`cdb -g -G` runs to the end / through the initial breakpoint, then dumps stacks.)

## Symbols

Set `_NT_SYMBOL_PATH` so frames resolve to `module!function` instead of raw
addresses, e.g.:
```
SRV*C:\symbols*https://msdl.microsoft.com/download/symbols
```
plus the folder holding your app's PDBs. Without symbols, backtraces are just
offsets.

## Reading the output

- **`EXCEPTION_CODE` `c0000005`** = access violation (null/dangling pointer);
  `c00000fd` = stack overflow.
- The **faulting thread** is the one whose top frame matches `SYMBOL_NAME` — its
  frames are shown first.
- All-thread `stacks` + `devtools_flamegraph` makes a deadlock obvious: multiple
  threads parked in the same lock path.

## Notes

- Batch mode covers crash/hang triage and dumps. **Interactive step-through** on
  Windows (breakpoints, stepping) is a planned follow-up (a ConPTY session
  mirroring the LLDB `debug_*` tools).
- For step-through on macOS/Linux today, use the LLDB `debug_start`/`debug` tools.
  Overall workflow: [[devtools-mcp-usage]].
