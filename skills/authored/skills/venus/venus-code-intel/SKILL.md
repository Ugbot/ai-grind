---
name: venus-code-intel
description: Query Venus C code structurally with the llm-station TreeSitter daemon via tools/llm_station.py , per-function cyclomatic complexity (the Tiger Style <70-line rule nothing else gates), AST-confirmed references that never match inside strings or comments, and ranked symbol search. Use when asked to find complexity hotspots, find the real callers/uses of a C symbol, or check whether something is actually referenced. Also read this BEFORE trusting llm-station's unused_symbols, call_graph or code_quality_check , all three are measured wrong on Venus C.
---

# What this is

`~/llm-station` is a separate C++ daemon that TreeSitter-indexes a workspace and
exposes ~118 tools. Venus drives a **three-tool slice** of it through
`tools/llm_station.py`. The rest is duplicated by devtools-mcp, needs an LLM
Venus has no key for, or is broken on C.

Full measurements: `docs/notes/llm-station.md`. Inventory: `tools/MAP.md`.

# Use it

```bash
python3 tools/llm_station.py complexity src/entities
python3 tools/llm_station.py refs       src/audio voice_alloc
python3 tools/llm_station.py symbols    src/networking net_quantize
```

First run on an area indexes it and takes **minutes**; afterwards queries are
instant. The daemon auto-starts.

| Command | Use it for |
|---|---|
| `complexity <area>` | Per-FUNCTION cyclomatic complexity + hotspots. **The only check on CLAUDE.md's "functions < 70 lines"** , `check_file_length.py` ratchets FILE length only. |
| `refs <area> <sym>` | AST-CONFIRMED references. Beats grep: never matches the name in a string/comment, and says so when a name is overloaded or shadowed. |
| `symbols <area> <q>` | Ranked symbol search with signatures + snippets. ripgrep is competitive; use whichever is faster to read. |

# Scope is mandatory , and it is the main limitation

**One area per invocation.** The script refuses the repo root. Indexing all of
Venus does not converge: BM25 re-seals its whole FST on every symbol insert, so
the daemon pins a core and stops writing to disk entirely (measured: identical
stack in two `sample(1)` captures six minutes apart, zero bytes written).
Areas up to ~400 C/H files are fine , `src/entities` (21 files, 351 functions)
indexes in minutes.

**So cross-area questions are OUT OF REACH.** `refs src/entities entity_db_swap`
correctly returns its two definitions and **zero** references, because every
caller lives in `src/actors/`. That is a true answer to a scoped question, not a
dead symbol. For repo-wide contract questions use the real gates ,
`tools/check_ticktock.py` for the tick-tock barrier, `tools/run_gates.py` for
the rest.

# Do NOT use these , measured wrong on Venus C

- **`unused_symbols`** , on `src/audio` it declared **48 of 49** functions dead,
  including `voice_alloc`, called 46 lines below its definition. Verified by
  grep. Reporting its output as dead code would be inventing work.
- **`call_graph`** , returns nothing for one C function and, for another, the
  assertion macro `VA_ASSERT` as a "callee" plus raw function bodies as
  identifiers. It does not understand C macros. `unused_symbols` is built on it,
  which is why that fails too.
- **`code_quality_check`** , returns empty on Venus C.
- **`stats`** , always prints `Indexed files: 0` even when queries demonstrably
  work. It reads a store the indexer never fills. To test an index, run a query.

This matters because dead-API detection is exactly what Venus most wants
(cf. `net_predict.h`, which advertised client prediction with zero callers
repo-wide). llm-station does **not** deliver it. Use `refs` and read the result.

# Gotchas

- `llm-station run` **exits 0 even when no daemon is running** and reports the
  failure on stderr. A bare invocation that "succeeds" with empty output usually
  means no daemon. `tools/llm_station.py` checks both streams for you.
- The MCP server is deliberately **not** in `.mcp.json`: it exposes all ~118
  tools unfiltered, most duplicating devtools-mcp. Use the CLI.
- `station_link` / `station_session` / `station_sync` in devtools-mcp are a
  **different product** , a hosted platform, not this daemon.
- The llm-station tree **does not currently build on macOS** (both local
  inference backends fail). The working binary predates the break; don't
  "fix" it by rebuilding llama.cpp , that breaks the dylib ABI and the binary
  stops launching. See `docs/notes/llm-station.md`.
