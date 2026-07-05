---
name: perfview-etw-trace
description: Collect and decode ETW CPU / thread-time traces of MarbleDB native benchmarks via PerfView on Windows, driven by the tools/perf_trace.py Python helper. Reach for this when the inline-RDTSC skill (bench-rdtsc-profile) has told you WHICH function is hot but you need to know WHY — call trees, leaf costs, kernel-time share, wait-time attribution.
---

# PerfView ETW tracing for MarbleDB, via `tools/perf_trace.py`

**Always prefer the Python wrapper for running PerfView.** It handles
symbol path, PDB checks, MSYS path-conv bypass, elevation fallout,
and CSV decoding in one command. Only fall back to raw PerfView CLI
for edge cases the script doesn't cover.

## When to reach for this skill vs `bench-rdtsc-profile`

- **`bench-rdtsc-profile`** first. It's cheap, bench-internal, and
  tells you which sub-phase burns cycles. That answers "which
  function".
- **This skill** when the inline breakdown points at a big function
  you want a call-tree for, or when you need to see wait time,
  context switches, or kernel attribution. That answers "why those
  cycles burn there and who's calling in".

## Prerequisites

1. PerfView at `C:/code/PerfView.exe`.
2. Bench built with PDBs. The normal CMake Release config does NOT
   produce them — MSVC with `/GL /LTCG` needs `/Zi` at compile AND
   `/DEBUG` at link. Configure with:

   ```bash
   MSYS_NO_PATHCONV=1 cmake -S C:/code/marbledb -B C:/code/marbledb/build \
     -DCMAKE_CXX_FLAGS_RELEASE="/O2 /Ob3 /Oi /Ot /GL /Zi /DNDEBUG" \
     -DCMAKE_C_FLAGS_RELEASE="/O2 /Ob3 /Oi /Ot /GL /Zi /DNDEBUG" \
     -DCMAKE_EXE_LINKER_FLAGS_RELEASE="/LTCG /DEBUG /OPT:REF /OPT:ICF" \
     -DCMAKE_MODULE_LINKER_FLAGS_RELEASE="/LTCG /DEBUG /OPT:REF /OPT:ICF" \
     -DCMAKE_SHARED_LINKER_FLAGS_RELEASE="/LTCG /DEBUG /OPT:REF /OPT:ICF" \
     -DBOLT_SIMD_TIER=NATIVE
   ```

   Verify: `ls C:/code/marbledb/build/benchmarks/Release/bench_ycsb_direct.pdb`
   should show a ~3 MB file. The Python wrapper refuses to run
   without this.

3. Admin elevation: some kernel providers require it. PerfView
   self-elevates on first provider-enable; the parent process exits
   with code 2 and the elevated child writes the ETL. **Don't trust
   the parent's exit code — check the ETL file size** (the wrapper
   does this).

4. `python` on PATH (any 3.8+).

## The Python wrapper — `tools/perf_trace.py`

```bash
# One-shot: capture + decode + print top hotspots
python C:/code/marbledb/tools/perf_trace.py \
    C:/code/marbledb/build/benchmarks/Release/bench_ycsb_direct.exe \
    100000 100000 1

# Or with explicit flags
python C:/code/marbledb/tools/perf_trace.py \
    --etl C:/code/bench_trace.etl \
    --process bench_ycsb_direct \
    --module bench_ycsb_direct \
    --top 30 \
    <exe> [args...]

# Skip capture, re-decode an existing ETL
python C:/code/marbledb/tools/perf_trace.py --decode-only --etl C:/code/bench_trace.etl
```

### What the script does for you

- Verifies the bench PDB exists where PerfView expects it.
- Sets `_NT_SYMBOL_PATH` to the build directory + MS public symbol
  server, so both your code AND system DLLs (`ntoskrnl`, `ucrtbase`,
  `vcruntime140`) resolve — this is what makes `memset` /
  `memcpy_avx_ermsb` attributable instead of showing up as module-
  level `ucrtbase!?`.
- Disables MSYS path conversion before spawning PerfView.
- Runs collection with the flags we've converged on:
  `/NoNGenRundown /NoClrRundown /NoV2Rundown /CpuSampleMSec:0.125
  /ThreadTime /FocusProcess:<exe> /Merge:true /Zip:false`.
- Waits for the elevated child to finish merging (polls file size +
  the absence of `.etl.new`).
- Invokes `PerfView UserCommand SaveCPUStacksAsCsv` to dump a flat
  symbol-resolved table.
- Parses the CSV and prints three tables:
  1. **Top-N by Exclusive%** — where cycles actually burn (leaf
     functions). This is the "what's slow" answer.
  2. **Top-N by Inclusive% − Exclusive%** — dispatchers that spend
     most of their time below. Useful for locating "the put path
     burns 22 % total but only 1.6 % is in the put() function
     itself".
  3. **Top-N under each module filter** — same data but filtered
     (`--module bench_ycsb_direct`, `ucrtbase`, etc).
- Saves the resolved CSV next to the ETL for follow-up drilling.

### Output shape

```
=== Top 20 by Exc% (real CPU leaves) ===
  18.7%   ucrtbase!memset
   5.9%   bench_ycsb_direct!marbledb::memtable::zone_probe_view
   5.1%   bench_ycsb_direct!marbledb::memtable::zone_probe
   4.3%   bench_ycsb_direct!bolt::wire::bolt_wire_deserialize
  ...

=== Top 20 by Inc% (subtree dispatch) ===
  21.6%   bench_ycsb_direct!marbledb::put                (Exc 1.6%)
  15.6%   bench_ycsb_direct!marbledb::wal::subscriber_loop (Exc 0.03%)
  13.8%   bench_ycsb_direct!bolt::wire::bolt_wire_serialize (Exc 1.6%)
  ...
```

Read them side by side. If a function has high Inc% and low Exc%,
its work is IN CALLEES — drill down. If a function has high Exc%
(e.g. `memset`), the time is RIGHT THERE — look at its callers.

## Key insights learned while building this

1. **PerfView's default symbol path is the CWD, not the bench dir.**
   Without `_NT_SYMBOL_PATH` set, you get module-level aggregation
   only (`bench_ycsb_direct!?`). The Python wrapper always sets it.
2. **Symbols for system DLLs matter.** `memset` being attributed to
   `ucrtbase!memset` (not `ucrtbase!?`) only works if Microsoft's
   public symbol server is in the path. The wrapper adds
   `SRV*<cache>*https://msdl.microsoft.com/download/symbols`.
3. **Turn off `/Ob3` for truly deep stacks.** `/Ob3` aggressive
   inlining means many small helpers vanish into their callers.
   For a capture where you NEED the inlined function separable, pass
   `--ob1` to the wrapper — it rebuilds the bench with `/Ob1` before
   capture.
4. **`/FocusProcess` saves ~40 MB of ETL.** Always narrow unless
   you're deliberately chasing cross-process interactions.
5. **The elevated-child exit-code-2 trap.** PerfView's parent exits
   non-zero after launching the elevated collector. The ETL is
   still being written. Don't interpret exit code 2 as failure —
   check for `.etl.new` disappearance and `.etl` size > 0.
6. **`/CpuSampleMSec:0.125` catches hot sub-ms loops.** Default 1 ms
   misses them. The wrapper uses 0.125 ms (8 kHz).
7. **`/ThreadTime` is non-optional.** Without it you can't tell
   "burning CPU" from "blocked on a lock / IO". Wrapper includes it.
8. **Inline-RDTSC probes inside the bench add < 0.1 % and don't
   pollute the top-N tables.** Leave them in.

## Hand-running PerfView (when the wrapper isn't enough)

```bash
# Capture
MSYS_NO_PATHCONV=1 "C:/code/PerfView.exe" \
    /AcceptEULA /NoGui \
    /LogFile:"C:/code/perf.log" \
    /DataFile:"C:/code/trace.etl" \
    /NoNGenRundown /NoClrRundown /NoV2Rundown \
    /CpuSampleMSec:0.125 /ThreadTime \
    /Zip:false /Merge:true \
    /FocusProcess:bench_ycsb_direct.exe \
    run "<exe>" <args>

# Extract flat table (needs symbols resolved)
export _NT_SYMBOL_PATH='C:\code\marbledb\build\benchmarks\Release;SRV*C:\Users\Capta\AppData\Local\Temp\SymbolCache*https://msdl.microsoft.com/download/symbols'
MSYS_NO_PATHCONV=1 "C:/code/PerfView.exe" \
    /AcceptEULA /NoGui \
    /LogFile:"C:/code/csv.log" \
    UserCommand SaveCPUStacksAsCsv "C:/code/trace.etl" bench_ycsb_direct
```

The output is `trace.perfView.csv`. Columns: `Name, Exc, Exc%, Inc,
Inc%, Fold, First, Last`. `Exc` is leaf samples; `Inc` is subtree
total; `First/Last` are timestamps (ms since trace start).

## GUI mode (when you need call trees, not flat tables)

```bash
MSYS_NO_PATHCONV=1 "C:/code/PerfView.exe" "C:/code/trace.etl"
```

Useful views:
- **CPU Stacks** → ByName, sort by Exc or Inc.
- **Thread Time Stacks** → per-thread wait + CPU.
- **Context Switch Stacks** → who blocked whom.

GroupPats `[Bolt];[*!.]` collapses external modules. IncPats
`*bench_ycsb_direct*` restricts to our process. Fold % `1.0` hides
< 1 % noise.

## Worked example — YCSB workload A (2026-04-24 evening)

Inline RDTSC said put_row was 18,527 cy/op, 2× reads. Ran:

```bash
python tools/perf_trace.py build/benchmarks/Release/bench_ycsb_direct.exe 100000 100000 1
```

Top Exc% revealed:
- `memset` 18.7 % (callsite TBD — likely ScanIterator init + BoltBatch
  zero-init + arena slab clears).
- `bolt_wire_serialize` 1.6 % Exc / 13.8 % Inc — **every put
  serializes the batch to WAL bytes**.
- `bolt_wire_deserialize` 4.3 % Exc / 10.9 % Inc — **subscriber
  thread decodes the same bytes to apply to memtable**.
- `snprintf` 0.3 % Exc / 9.9 % Inc — bench re-formats field names
  12× per put, pure bench overhead.
- `marbledb::put` 21.6 % Inc total — all puts.

Diagnosis: ~24.6 % of bench CPU is the wire serialize/deserialize
round-trip across an in-process WAL. Architectural cost. The put
function itself is small; the subtree is big because of the WAL
subscriber handshake.

## Checklist before proposing a fix

- [ ] PDB present for the bench binary.
- [ ] Top-N Exc table has resolved function names, not `module!?`.
- [ ] Top-N includes both `bench_ycsb_direct!` entries AND system
      DLLs — otherwise you're missing the leaf costs.
- [ ] Cross-checked Thread Time Stacks for wait time.
- [ ] Attributed the top-3 Exc leaves to specific callers (walk up
      the tree in the GUI or use the wrapper's `--attrib memset`
      mode that prints the top caller chains for that symbol).

## Reference files

- `C:/code/PerfView.exe`
- `C:/code/marbledb/tools/perf_trace.py` — the wrapper
- Companion skill: `.claude/skills/bench-rdtsc-profile.md`
- Latest resolved CSV: `C:/code/bench_trace.perfView.csv`
