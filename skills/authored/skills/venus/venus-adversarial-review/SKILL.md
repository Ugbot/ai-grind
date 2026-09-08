---
name: venus-adversarial-review
description: The adversarial reviewer's checklist for a Venus wave packet — refute the handoff from the diff and the build report: knob and red line per gate, numbers diffed against logs, production-consumer grep, prior findings ledger, Tiger Style greps, lever plumbing, and the structured verdict. Use when reviewing any packet before its per-ticket commit.
---

# Stance

Default to refuting. The handoff is a claim; the diff and the build owner's
logs are the evidence. Wave 1 review caught: a spawn ladder that rescued zero
seats, a far-field bound falsified by its own control, "every solver body gets
the Voronoi fallback" (false for boxes), an O(n^2) hash, two dead levers, a
2 MiB per-chunk malloc on the sim thread. None of those were visible from
the handoff text.

# Inputs

`git diff <wave base SHA> -- <packet files>` read whole, then the files
whole; the build owner's report (`scratchpad/<wave>/build-owner-report.json`
and its logs); the previous round's findings for this packet.

# Checklist (each produces a finding or an explicit "shown")

1. **Per gate: knob, line, shown?** Name the knob that reddens each leg, the
   line that goes red, and whether the build report SHOWS it (control raw rc,
   sentinel, different line than the positive). Write "NOT SHOWN" explicitly.
   A control red on the positive's own line is a major.
2. **Numbers vs logs.** Every number in the handoff/MAP note is found in a
   named log or it is a finding. Cross-run ratios (control count over the
   positive's denominator) are a finding. Triage figures repeated after a
   build measured something else are a finding.
3. **Production consumer.** `grep -rn <new symbol> src/ cmake/` — is it called
   from the shipping client/server/headless path, or only tests/examples?
   "harness only" / "no consumer" is a verdict the MAP must carry; a claimed
   consumer that is test-only is a major.
4. **Prior findings ledger.** Every finding of the previous round is fixed in
   the diff or listed in `unfinished`; a silently dropped finding is a major.
5. **Lever plumbing.** The lever is an explicit setter from `main()`; no
   `getenv` added in engine TUs; not `uv_os_setenv` read by `getenv` in-process
   (MSVC trap). Cross-thread globals are `_Atomic` with a lock-free assert.
6. **Tiger Style greps.** `grep -n "assert(" <files>` — bare asserts in
   engine/game code are compiled out in Release (a finding, with the line).
   >= 2 asserts per new function including negative space; no tautologies
   (store/readback, `bool == true || false`); data conditions are error paths.
   `wc -l` every touched file; a touched > 1500-line file that did not get
   shorter is a finding; a touched > 70-line function likewise.
7. **Hot paths.** No allocation, blocking wait, or mutex on the sim/render/
   net paths; loops bounded; explicit integer sizes. Tick-tock: `db_write()`
   only from sim, no `process_queued`/`swap` outside the framework.
8. **Levers not rewrites.** The old behaviour is still runnable behind the
   lever; a design call listed in doc 33 s6.5 is not taken unilaterally.
9. **Engine vs game placement.** Mechanisms in `src/<engine area>`; games
   author data and consumers (`venus-engine-vs-game`).
10. **Falsification list.** For every gate leg, the knob you would flip and
    what should go red; say whether the report shows it.

# Verdict

`approve` (nothing above the minor line), `approve_with_fixes` (majors that
do not invalidate the gate), `reject` (a blocker: the gate does not prove
the claim, a regression to a green gate, or a hot-path violation). Findings
carry `file:line`, severity, the issue, and the concrete fix. Do not edit,
do not build.
