---
name: venus-map-record
description: How to write a Venus area MAP.md record (and the MAP.waveN-<packet>.md note that becomes one), newest-first dated section with the ticket key, the three completion states named separately, measured numbers cited to logs, production consumer file:line, open owner questions by doc 33 §6.5 number, remaining known defects. Use when closing a packet, folding wave notes, or when a MAP claim needs correcting.
---

# Placement

Record work in the AREA's `MAP.md` (`src/<area>/MAP.md`, `tests/MAP.md`,
`tools/venus_cook/SURFACE_CAPTURE.md` for the cooker), never in
`PROJECT_MAP.md` (the router changes only when an area is added/removed/
renamed). During a wave an implementer writes `src/<area>/MAP.waveN-<packet>.md`;
the lead's merge agent folds it at packet close and deletes the note.

# Section shape (newest first, directly under the MAP's preamble)

```
### <what changed, in one line> (2026-09-08, VENG-1539, Stage A wave 1d)

**Component-implemented, connected-NOT-run, acceptance-open.**
<mechanism, 3-8 sentences, naming the files and the lever>
Production consumer: `src/physics/venus_pcontact_stage.c:381` (solver),
`src/wyrdholm/wyrdholm_terrain_collider.c:565` (stream add).
Measured (scratchpad/wave1d-20260908/build-owner-report.json; gates_packets.log):
`physics_mesh_edges` 20.03 s, 320,417 checks / 0; `_falsify_face_only` rc=1 on
6 named legs; `_falsify_all_active` rc=1 on the 2 seam legs.
Known defects (review round 3): <one line each, with the finding id>.
Open for Ben: Q1 (collision product shape), Q5 (retry vs MPSC ring).
Not done: <what the packet did not do, honestly>.
```

# Rules

- **Three states, never collapsed**: component implemented (gate >= 20 s
  green with a discriminating control; consumer named) / connected passed
  (real server + connected clients, receipt with SHA) / acceptance passed
  (route criterion recorded by the lead). "Validation pending" is not a
  state; say which of the three is missing.
- **A number is a claim until a log carries it.** Cite the file (and line
  where useful). No cross-run ratios. A pre-measurement figure is replaced
  by the measured one or by `UNMEASURED`.
- **Production consumer by `file:line`** or the words "no production
  consumer (harness/test only)". A component with no consumer is `partial`.
- **Levers**: name the env/setter, the default, and which side the old
  behaviour is.
- **Known defects and owner questions** stay in the record until closed;
  cite the review round/finding and the §6.5 question number.
- **Contradictions**: when a later record supersedes an earlier claim in the
  same MAP, say so in the new one ("supersedes the 2026-09-07 'validation
  pending' section: the join path was never run connected").
- **Length**: a MAP is an inventory, not a diary; a wave's record is one
  section per packet. Long falsification narratives go in the receipt files
  under `scratchpad/<wave>-<date>/` and are linked.
- Doc 33's status log gets ONE line per landed wave with the SHAs and the
  headline numbers, citing the receipt directory.
