---
name: bench-rdtsc-profile
description: Inline-RDTSC micro-profiling in MarbleDB's C++ benchmarks. Use when a YCSB workload (A–F, Load) is slower than expected and you need to know which sub-phase is burning cycles before proposing a fix. Faster than launching PerfView and gives exact cycle-per-op accounting for the specific code path you care about.
---

# Inline-RDTSC micro-profiling for MarbleDB benchmarks

## When to use this

Reach for this skill when:

- A YCSB workload in `benchmarks/bench_ycsb_direct.cpp` is slower than
  expected, Pebble, or a sibling workload.
- You need to know which of several sub-phases inside one op is the
  bottleneck, and the op is short enough (≤ a few µs) that external
  profilers (PerfView, VTune) get swamped by sampling overhead.
- You want a quantitative cycles-per-op answer with zero install and
  zero separate tool invocation — the bench itself reports the
  breakdown on exit.

Do NOT reach for this skill when:

- The hot path is longer than ~100 µs per op — use PerfView / VTune
  instead, the sampling overhead becomes negligible.
- You need flamegraphs / call trees with names you didn't hand-label —
  use a sampler.
- You need kernel-land timing (I/O waits, syscalls, context switches)
  — `xperf -on BASE` or PerfView ETW.

## The mechanism

MSVC exposes `__rdtsc()` in `<intrin.h>` and the compiler knows the
Bolt hardening headers already include it transitively. One sample
site = two `__rdtsc()` calls bracketing the region. Store the delta
in a global `std::atomic<uint64_t>` counter (cache-line padded) plus
an ops counter. Print the per-op average + percentage share at bench
exit.

On an i9-class host @ 2.4 GHz, 1 cycle ≈ 0.417 ns. For a 100K-op
workload the counter accumulates ~10⁹–10¹⁰ cycles — well inside
`uint64_t`.

## The exact template

Drop this into `benchmarks/bench_ycsb_direct.cpp` next to the
existing `scan_profile` / `wla_profile` blocks.

### 1. Add the `rdtsc_now()` helper (once per file)

```cpp
static inline uint64_t rdtsc_now() noexcept {
#if defined(_MSC_VER)
    return __rdtsc();
#else
    return 0;
#endif
}
```

`<intrin.h>` is already on the include path via Bolt's hardening. On
non-MSVC it returns 0 — the whole probe reduces to zero overhead.

### 2. Add per-phase atomic counters

Cache-line pad so the workload threads don't false-share. One
counter per sub-phase you want to time. Always include an ops counter.

```cpp
namespace <workload>_profile {
alignas(64) std::atomic<uint64_t> cy_phase_a{0};
alignas(64) std::atomic<uint64_t> cy_phase_b{0};
alignas(64) std::atomic<uint64_t> cy_phase_c{0};
alignas(64) std::atomic<uint64_t> ops_counted{0};
}
```

### 3. Bracket each sub-phase inside the workload loop

Accumulate in a per-thread local first, then fetch_add once at loop
end — critical for multi-threaded runs. Per-iteration atomic
fetch_adds contend and skew the result.

```cpp
// Inside the workload lambda passed to run_parallel:
uint64_t local_cy_a = 0, local_cy_b = 0, local_ops = 0;
for (int i = 0; i < my_ops; ++i) {
    const uint64_t t0 = rdtsc_now();
    phase_a(...);
    const uint64_t t1 = rdtsc_now();
    phase_b(...);
    const uint64_t t2 = rdtsc_now();

    local_cy_a += t1 - t0;
    local_cy_b += t2 - t1;
    ++local_ops;
}
<workload>_profile::cy_phase_a.fetch_add(local_cy_a, std::memory_order_relaxed);
<workload>_profile::cy_phase_b.fetch_add(local_cy_b, std::memory_order_relaxed);
<workload>_profile::ops_counted.fetch_add(local_ops, std::memory_order_relaxed);
```

### 4. Print the breakdown after the workload row

Right next to the `print_row("X", ...)` call, read the atomics and
print cy/op + percentage share:

```cpp
{
    const uint64_t ops = <workload>_profile::ops_counted.load(std::memory_order_relaxed);
    const uint64_t ca  = <workload>_profile::cy_phase_a.load(std::memory_order_relaxed);
    const uint64_t cb  = <workload>_profile::cy_phase_b.load(std::memory_order_relaxed);
    if (ops > 0) {
        const double tot = double(ca + cb);
        std::printf("\n[<workload> breakdown] %llu ops\n"
                    "  phase_a   %10.0f cy/op  (%.1f%%)\n"
                    "  phase_b   %10.0f cy/op  (%.1f%%)\n",
                    static_cast<unsigned long long>(ops),
                    double(ca) / double(ops), 100.0 * double(ca) / tot,
                    double(cb) / double(ops), 100.0 * double(cb) / tot);
    }
}
```

### 5. Build + run

Single-threaded is plenty for diagnosis; multi-thread adds noise.

```bash
cmake --build C:/code/marbledb/build --config Release --target bench_ycsb_direct
C:/code/marbledb/build/benchmarks/Release/bench_ycsb_direct.exe 100000 100000 1
```

## Interpreting the output

Per-op cycle counts on a 2.4 GHz i9-9980HK:

| cy/op | ns/op | Rough meaning |
|---:|---:|---|
| < 100 | < 42 ns | L1-hit atomics / tight pointer chase |
| 100–1,000 | 42–420 ns | Arena alloc + a few kernel calls |
| 1,000–10,000 | 0.4–4 µs | Typical KV point-op end-to-end |
| 10,000–100,000 | 4–42 µs | Full scan / compaction / flush batch |
| > 100,000 | > 42 µs | Something's wrong — look for O(n) in a per-op path |

The **percentage share** matters more than the absolute cy/op. If one
phase is > 50 % you have a single-bottleneck problem. If four phases
are ~25 % each you have a pipeline-shape problem (won't fix with a
single short-circuit).

## Worked example — YCSB E (done 2026-04-24)

Starting point: 33 K ops/s, 4× behind Pebble. Bracketed the three
calls inside `scan_rows`:

```
[E scan breakdown] 94721 scan ops
  mdb_scan (init)        631 cy/op  (0.7%)
  mdb_scan_next       86677 cy/op  (99.2%)
  mdb_scan_close         103 cy/op  (0.1%)
```

**99.2 % in `mdb_scan_next`, so the problem isn't iterator setup —
it's inside the memtable driver.** Drilled into `drive_memtable` and
found three O(zone_size) leaks (`apply_time_range` no full-range
short-circuit, `apply_tombstones` no-tombs short-circuit missing,
`emit_projected` ignored `batch_rows`). Fix landed in Wave 18c.10.
After: `mdb_scan_next` = 7,319 cy/op — 11.8× reduction → E hit 294 K.

If the first column had instead shown `mdb_scan (init)` at ~90 %, the
fix would have been iterator pooling — completely different surgery.
The one line of data told us which surgery.

## Worked example — YCSB A (in progress)

```
[A phase breakdown] 50336 gets / 49664 puts
  get_row         9185 cy/op
  put_row        18527 cy/op   (2.0x vs get)
```

Put is 2× get on A. Next step is to bracket inside `put_row` —
separate `mdb_batch_reset` + the 12 `mdb_batch_add_column_i64` calls
from the actual `mdb_put` — to know whether the bottleneck is the
columnar batch construction or the engine-side insert.

## Gotchas

1. **Per-iteration atomic fetch_add contends on multi-thread runs.**
   Always accumulate in a stack-local `uint64_t`, flush once at loop
   end.
2. **`__rdtsc()` doesn't serialise.** Out-of-order cores may interleave
   the surrounding loads/stores around the read. For µs-scale regions
   the noise is single-digit cycles — fine. For sub-100 cy regions
   use `__rdtscp()` (serialising) or lfence-rdtsc pairs.
3. **Turbo Boost / frequency scaling.** Modern CPUs vary frequency by
   ~30 %. Cycle counts are stable; derived wall-time is not. Always
   quote cy/op, then convert to ns at the end using a declared clock
   rate.
4. **Cold caches.** The first run warms L2; subsequent runs are faster.
   The bench already does a load phase before A — good. For sub-phase
   timing, discard the first 1–2 K ops if you see a warm-up transient.
5. **Leave the probes in a run or two, then gate them.** The
   instrumentation is ~4 cy overhead per RDTSC pair (negligible for
   µs-scale ops, ~10 % for < 100-cy regions). For shipping-quality
   bench runs, gate with a macro like `#ifdef MARBLEDB_BENCH_PROFILE`.

## Reference files

- `C:\code\marbledb\benchmarks\bench_ycsb_direct.cpp` — look for
  `scan_profile::`, `wla_profile::`, `rdtsc_now()`.
- Wave 18c.10 reference write-up:
  `C:\code\marbledb\docs\research\wave-18c-perf.md` — the E
  investigation worked example, end to end.

## Checklist before asking for a fix

- [ ] Bracketed every sub-phase of the op — no "unaccounted"
      cycles > 10 % of the total.
- [ ] Ran single-threaded (noise is lower).
- [ ] Have 3+ runs to confirm the share is stable (run-to-run
      variance < 5 %).
- [ ] Know the dominant phase and its cy/op.
- [ ] Mapped cy/op to the function that owns those cycles — not just
      a syscall stub or a wrapper.

Only then propose the surgery.
