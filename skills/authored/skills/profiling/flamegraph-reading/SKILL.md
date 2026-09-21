---
name: flamegraph-reading
description: >
  How to read and reason about flame graphs (and the devtools-mcp text flame-tree
  / Exc%-Inc% tables). Use when you've generated a flame graph or CPU profile and
  need to interpret it: find the real bottleneck, tell a wide plateau from a tall
  tower, distinguish exclusive (leaf) from inclusive (subtree) time, and avoid the
  classic misreadings. Applies to output from etw, jvm jfr/asprof, dtrace profile,
  perf, and cdb stacks.
---

# Reading flame graphs

A flame graph turns thousands of sampled stacks into one picture. In the
devtools-mcp SVG (icicle layout) the **root is at the top** and depth grows
**downward**; **width is inclusive time** (how often that frame was on the stack).

## The two numbers that matter

- Exclusive (Exc%), or "self", counts samples where this function was the leaf
  (the CPU was *in* it). This is where cycles actually burn.
- Inclusive (Inc%), or "total", counts samples where this function was anywhere on
  the stack** (it or something it called). This is how much work happened *under*
  it.

`Inc% − Exc%` ≈ time spent in callees. The devtools-mcp summary prints both a
**hottest-leaves** table (sort by Exc%) and a **top-dispatchers** view
(sort by Inc−Exc).

## How to read it in 30 seconds

1. **Scan the bottom edge.** The widest boxes at the deepest points are the leaves
   eating CPU. Start there (high Exc%).
2. **Wide = hot, tall = deep.** Width matters, not height. A tall narrow tower is
   deep recursion that costs little. A wide flat plateau is the real cost.
3. **Follow a wide column down** from a high-Inc% dispatcher to find *where* its
   time goes. The point where one wide box splits into many narrow ones is where
   the cost spreads.
4. Plateaus of `memset`, `memcpy`, alloc, or GC mean copying and allocation cost, so look
   at their *callers* (walk up) to find who's doing it.

## Common misreadings (avoid these)

- Optimising a tall tower that's only 1% wide. It's deep, not expensive.
- Chasing a high Inc% function whose own Exc% is ~0. The time is in its
  callees; drill down, don't rewrite the dispatcher.
- Trusting left-to-right order. Siblings are ordered by width (or name), not
  time. The x-axis is *not* a timeline.
- **Ignoring unsymbolised frames** (`module!?`): fix symbols first (see
  [[etw-profiling]]) or you're reading noise.

## In devtools-mcp

- `devtools_flamegraph(run_id)` → writes the SVG (open in a browser) + prints a
  bounded text flame-tree and the Exc%/Inc% table. Narrow with `min_pct` and
  `max_depth` if the tree is large.
- The full per-stack data stays queryable: `devtools_analyze(run_id, sort_by=…)`,
  or group by module/namespace to see which component dominates.

Rule of thumb: fix the widest leaf first, then re-profile. Flame graphs
shift dramatically once the top bottleneck is gone.
