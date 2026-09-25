---
name: venus-packet-preflight
description: Implementer pre-flight for a Venus wave packet — the checklist and the tools/check_packet.py script that catch the defect classes reviewers rejected in every Stage A wave (bare assert() in Release, engine getenv levers, vacuous WILL_FAIL controls, unmeasured numbers, 70-line functions, comment terminators). Run BEFORE handing off any packet; paste its block into the handoff.
---

# Why this exists

Doc 34 (`docs/design/game/34-stage-a-retrospective-20260908.md`): 141 reviewer
findings over four waves; three classes recurred in EVERY wave after being
called out — bare `assert()` (16), non-discriminating WILL_FAIL controls (13),
numbers written before measurement (11). Implementers fixed only the lines a
reviewer enumerated. This skill makes the checks mechanical.

# Run it

```bash
cd C:/code/Venus
python tools/check_packet.py --base <wave base SHA> --handoff
# restrict to your packet's files:
python tools/check_packet.py --base <sha> --files src/physics/x.c tests/x_test.c --handoff
```

Exit 1 = ERROR class present (E1 bare assert in src/, E2 getenv in an engine
TU, E3 WILL_FAIL without a PASS_REGULAR_EXPRESSION sentinel). Fix, re-run,
paste the `PRE-FLIGHT` block into the handoff's `notes`.

# The checklist (each item is a grep or a printed line in the handoff)

1. **Asserts**: `assert(` count in your diff under `src/` = 0. Every new
   function has >= 2 `VENUS_ASSERT` including negative space; none is a
   store/readback or `x == true || x == false` tautology. Data conditions
   (world geometry, input, I/O) get error paths, not asserts.
2. **Levers**: no new `getenv()` in engine TUs. In-process test levers are
   explicit setters called from the test's `main()` BEFORE any world/server
   exists (precedent `venus_physics_set_force_serial`,
   `venus_mesh_active_mask_force`, `job_system_set_legacy_retry`,
   `wyrd_net_server_set_phase_tally_enabled`). MSVC trap: `uv_os_setenv` /
   `SetEnvironmentVariable` never refreshes the CRT copy `getenv()` reads —
   an env var set in-process is invisible to the engine. ctest `ENVIRONMENT`
   is fine (it is in the initial process block).
3. **Cross-thread globals**: `_Atomic` with a live
   `VENUS_ASSERT(atomic_is_lock_free(&g))`, release store / acquire load; or
   the handoff proves single-thread-before-start.
4. **Falsifiers**: one printed sentinel line `NAME key=value ...`, registered
   with `PASS_REGULAR_EXPRESSION` on that sentinel, NO `WILL_FAIL`, no line
   numbers in the regex. See `venus-falsifier`.
5. **Numbers**: a number in a handoff, MAP note or comment is copied from a
   named log line (`scratchpad/<wave>/... :line`) or written as `UNMEASURED`.
   Never a cross-run ratio. Never a triage figure after a build measured it.
6. **Findings ledger**: `findings_addressed` + `unfinished` together list
   EVERY finding id of the review round, by the reviewer's numbering.
   Dropping one silently is a major on the next review.
7. **Lengths**: every touched function's post-length; a touched > 70-line
   function is split or the handoff says why not. A touched > 1500-line file
   ends shorter or is split. New features go in NEW TUs.
8. **Fixture conventions**: read the API header before writing a fixture and
   quote the convention in the leg's comment. Known ones: capsule query
   `origin` is the CENTRE (`venus_pquery.c`, `c1 = origin - half_height`);
   the server drains hellos then steps in the SAME tick; admission traces
   have fixed capacities; `system()` on cmd.exe strips quotes.
9. **Compile proxy** (you may not build): brace/paren balance on every edited
   file; grep `*/` inside doc comments in edited headers; every called symbol
   opened in its header and the signature checked; re-read the whole diff.
10. **Ownership**: touch ONLY the packet's files; never `git add/commit/stash/
    checkout/reset`; MAP notes go to `src/<area>/MAP.waveN-<packet>.md`.

# Handoff shape

Production consumer (`file:line`) · changed/new files · gates with their
negative control and how it is switched · targets to build · findings
addressed / unfinished (all ids) · owner-only questions · the PRE-FLIGHT
block. A claim about a gate result you did not run is "reasoned, unrun".
