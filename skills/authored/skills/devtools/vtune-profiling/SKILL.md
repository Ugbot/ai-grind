---
name: vtune-profiling
description: >
  Profile with Intel VTune Profiler via the devtools-mcp vtune backend: CPU
  hotspots, threading, memory-access, memory-consumption, microarchitecture
  (top-down), and the performance snapshot, plus the raw `vtune` CLI for
  anything deeper. Use when you need Intel-grade depth on Windows or Linux:
  hardware-event analysis, top-down (front-end/back-end/memory-bound)
  breakdowns, false sharing, DRAM-bound questions, or when ETW/perf hotspots
  aren't enough. Follows the Intel VTune Performance Analysis Cookbook.
---

# VTune through devtools-mcp

Intel VTune Profiler is the deepest x86 profiler available: software *and*
hardware-event sampling, microarchitecture attribution, threading and memory
analyses. The `vtune` suite wraps its CLI with the usual contract: bounded
summary plus `run_id`, queryable Polars frame, flame graph. It keeps the result
directory so the VTune GUI can open the full picture later.

Reference: [VTune Performance Analysis Cookbook](https://www.intel.com/content/www/us/en/docs/vtune-profiler/cookbook/2025-0/overview.html)
(methodologies, configuration recipes, tuning recipes).

## Setup

- Install **Intel VTune Profiler** (free, part of oneAPI Base Toolkit or
  standalone).
- The wrapper finds `vtune` via `$DEVTOOLS_VTUNE`, PATH, or the default oneAPI
  install dirs (`C:\Program Files (x86)\Intel\oneAPI\vtune\latest\bin64\`,
  `/opt/intel/oneapi/vtune/latest/bin64/`). Run `devtools_check()` to confirm.
- Symbols are everything: build with PDBs (`/Zi /DEBUG`) on Windows, `-g` on
  Linux, including for Release/O2 binaries.
- **Software sampling** (default `hotspots`) works unprivileged, in VMs and
  containers. **Hardware events** (`uarch`, `memory`, hw hotspots) need the
  Intel sampling driver (or perf-based collection on Linux) and usually
  admin/root; in most VMs they're unavailable, so the cookbook's "Profiling
  Hardware Without Intel Sampling Drivers" recipe covers the fallback.

## The verbs

| `devtools_run(suite="vtune", tool=…)` | VTune analysis | Question it answers |
|---|---|---|
| `cpu` | hotspots | Where does wall/CPU time go, by function? |
| `threads` | threading | Lock contention, sync overhead, core under-utilization? |
| `alloc` | memory-consumption | Who allocates the memory? |
| `memory` | memory-access | DRAM/LLC-bound? Bandwidth, latency, false sharing? |
| `uarch` | uarch-exploration | Top-down: front-end / back-end / memory-bound / retiring? |
| `snapshot` | performance-snapshot | Quick triage: which analysis should I run next? |

```python
devtools_run(suite="vtune", tool="cpu", binary="C:/work/app.exe", args=["--bench"])
# → bounded top-functions table + run_id; then:
devtools_analyze(run_id=..., sort_by="cpu_time")     # query the function frame
devtools_flamegraph(run_id=...)                       # from the top-down report
```

Columns differ by analysis (hotspots: `cpu_time`, `cpu_time_spin_time`…;
memory: `loads`, `stores`, `llc_miss_count`…), then `devtools_query(run_id,
columns=["schema"])` lists them.

### extra_args the wrapper understands

- `--pid N` attaches to a running process instead of launching `binary`
  (add `--duration 30` to bound the collection in seconds).
- `--result-dir D` collects into or reuses a named result dir.
- `--report-only` skips collection and re-decodes an existing result dir.
- Everything else passes through to `vtune -collect` verbatim, e.g.
  `["-knob", "sampling-mode=hw"]`, `["-knob", "enable-stack-collection=true"]`,
  `["-knob", "sampling-interval=0.5"]`.

## Choosing the analysis (cookbook methodology)

1. Start with `snapshot`. Performance-snapshot is cheap and literally
   tells you which deeper analysis is worth running.
2. **Time in your code** → `cpu` (hotspots). Wide flat profile with no single
   hot leaf? Suspect the machine, not the algorithm → step 3.
3. **Hot but "doing nothing wrong"** → `uarch` and read it top-down (the
   cookbook's *Top-down Microarchitecture Analysis Method*): **Retiring** =
   genuinely busy; Front-End Bound = fetch/decode (huge code, i-cache,
   see *Instruction Cache Misses*); **Back-End: Memory Bound** = data stalls
   (→ step 4); **Bad Speculation** = branch mispredicts.
4. **Memory bound** → `memory` (memory-access). High LLC misses + remote DRAM
   = bandwidth/NUMA (*Frequent DRAM Accesses*); contended cache line with low
   utilization = *False Sharing*.
5. **Cores idle / spin time high** → `threads` (threading): waits, lock
   contention, oversubscription, OpenMP imbalance (*OpenMP Imbalance and
   Scheduling Overhead*).
6. **Allocation pressure** → `alloc` (memory-consumption).

## Raw CLI, when you outgrow the wrapper

```bash
vtune -collect hotspots -result-dir r001 -- ./app args      # collect
vtune -collect hotspots -knob sampling-mode=hw -result-dir r002 -- ./app
vtune -report summary   -result-dir r001                    # human summary
vtune -report hotspots  -result-dir r001 -format csv -csv-delimiter comma
vtune -report top-down  -result-dir r001                    # call tree
vtune -report hotspots  -result-dir r002 -result-dir r001   # diff two runs
vtune-gui r001                                               # full GUI on a result
```

Useful report types: `summary`, `hotspots`, `top-down`, `callstacks`,
`hw-events`. `-group-by` accepts `function`, `module`, `source-line`,
`thread`. The result dir is self-contained, so copy it off a server and open it
locally in the GUI.

## Gotchas

- **Exit code ≠ silence**: a failed collect usually means the driver is
  missing (hw modes), the binary path is wrong, or no admin rights. The
  wrapper surfaces vtune's last stderr lines.
- Result dirs are heavy (hundreds of MB for long runs); they're kept on
  purpose. Delete old `devtools-vtune-*` temp dirs when done.
- VTune on Windows wants the app's PDBs next to the exe or on
  `_NT_SYMBOL_PATH`, same rules as the etw-profiling skill.
- For reading the flame graph / Exc-Inc tables, see the flamegraph-reading
  skill; for ETW-based alternatives when VTune isn't installed, see
  etw-profiling.
