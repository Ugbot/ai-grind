---
name: venus-build-owner
description: The single build owner's recipe for a Venus wave, one target per cmake invocation with a survivable tally, the plain-client boot check, serial gates (-j 1) with per-control raw rc + sentinel, the receipt JSON every wave must carry, disk/process hygiene, and what "NOT FINISHED" must say. Use when you are the only agent allowed to build/test after implementers hand off, or when producing the VENG-1583-style receipt for a SHA.
---

# Contract

You are the ONLY process building or running tests in the tree. Implementers
have finished and may have left code that does not compile; you may make
MINIMAL correcting edits inside packet files and must list every one.
`VENUS_NEON_AUDIO=0` for everything you launch. Not-run is never passed;
built is not receipted until a log names the SHA, warning count and gate result.

# 1. Build, one target per invocation, tally survives a cutoff

```bash
cd C:/code/Venus
# reconfigure ONLY if a new TU/test was added (never delete the cache):
cmake -S . -B build-msvc > "$OUT/configure.log" 2>&1
cat > "$OUT/build_all.sh" <<'EOF'
for t in venus_engine venus_engine_server venus_physics <packet targets> wyrdholm wyrdholm_headless_client wyrdholm_server <every wyrd*/wyrdholm*/physics_* exe>; do
  s=$(date +%s); cmake --build build-msvc --target $t --config Release > "$OUT/build_$t.log" 2>&1; rc=$?
  w=$(grep -c "warning C\|warning LNK\|warning MSB" "$OUT/build_$t.log")
  echo "$t rc=$rc warnings=$w seconds=$(( $(date +%s)-s ))" >> "$OUT/tally.txt"
done; echo ALL_DONE >> "$OUT/tally.txt"
EOF
bash "$OUT/build_all.sh" &   # background; poll tally.txt — never block on one huge command
```

`touch` every packet TU first so the logs prove they recompiled. Zero warnings
is the rule (house: warnings are errors); report each warning verbatim.
Target names: `grep -h "add_executable(" cmake/*.cmake`. Disk: check free GB
before (the 2026-09-07 evaluation build died at 0 bytes free) and after.

# 2. Boot checks (the numbers the receipt must quote)

- Plain `build-msvc/Release/wyrdholm.exe` for ~30 s (cwd = repo root,
  `VENUS_LOG_LEVEL=INFO`), then kill: quote the `[WYRD_TERRAIN] STREAMING ARMED
  ... N candidate chunks` line (catalogue READY) and any REFUSED line.
- `wyrdholm_server.exe` headless ~30 s: quote `venus_pworld_terrain(w) == NULL`
  (Stage A criterion 1) and the island collider READY counts once P2 lands.

# 3. Policy gates and greps

`ctest -C Release -R "^(boundary_gate|shader_policy_gate|file_length_gate|assert_side_effect_gate|mutex_free_gate|engine_include_boundary|test_registration_gate)$"`,
`python tools/check_mutex_free.py`, `python tools/check_packet.py --base <sha>`.
Report remaining violations per file, not counts alone.

# 4. Packet gates, then the receipt, SERIAL

```bash
cd build-msvc
ctest -C Release --output-on-failure --timeout 900 -R "^(<packet gates>)" > "$OUT/gates_packets.log" 2>&1
ctest -C Release -L gate -R "^(wyrd|wyrdholm|physics|venus_jobs|net_)" --timeout 900 --output-on-failure -j 1 > "$OUT/receipt_gate_run.log" 2>&1
```

`-j 2` produced two contention reds in wave 1 (`wyrdholm_listen`,
`wyrdholm_input_latch`); serial receipts attributed every red. Always check
`Total Tests != 0`. For EVERY falsifier, also run the underlying binary by
hand with its lever and record: raw rc, the sentinel line, and whether it is
red for the INTENDED reason (a different line than the positive). Long
soaks (`wyrdholm_load` ~6 min, `wyrdholm_listen` ~3 min) run standalone once.

# 5. Receipt (JSON + md under scratchpad/<wave>-<date>/, committed)

Always print: SHA built; per-target rc/warnings (the tally); boot lines;
policy gate results with remaining violations; packet-gate run AND receipt
run paths with total/passed/failed/not_run and each failure's first CHECK
line; per control: raw rc, sentinel, `red_for_intended_reason`; every edit
you made as its own list; free disk GB at the end; a **NOT FINISHED** list
naming what was cut off (a forced structured-output cutoff mid-build must
say which targets never built, never report them as green).

# 6. Hygiene

Kill leftover `wyrd*|neon*|venus*` test processes at the end. Do not run
git. Do not reconfigure twice. If disk < 5 GB, stop and say so.
