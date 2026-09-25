---
name: venus-smoke-golden
description: Run and interpret the flagship smoke test + VK golden-image regression — the gating visual check for renderer work. Use when verifying renderer changes, chasing view FAIL/var=0/exit-code failures, recording new goldens after a visual sign-off, or when asked "is the renderer still green".
---

# The harness

`flagship_smoke_test` (build it + `flagship_demo_vk` first; needs `-DVENUS_BUILD_SMOKE_TESTS=ON` in the cache) spawns the demo, cycles 5 scripted views via `build-msvc/smoke_signal.txt`, snapshots each to `build-msvc/smoke_view_<backend>_<n>.png`, analyses them, and (VK) diffs against goldens.

```bash
cd C:/code/Venus   # ALWAYS repo root — golden + screenshot paths are relative
./build-msvc/Release/flagship_smoke_test.exe --backend=vk13            # gating leg
./build-msvc/Release/flagship_smoke_test.exe --backend=gl46            # frozen GL: boot-only gate
./build-msvc/Release/flagship_smoke_test.exe --backend=vk13 --record-goldens
./build-msvc/Release/flagship_smoke_test.exe --backend=vk13 --dump-stdout   # always print demo stdout
```

- **VK13 is the gating leg** (full view analysis + golden diff). GL46 is FROZEN → gates on boot/exit/crash only.
- Goldens: `tests/golden/vk13/view_<n>.png`, thresholds WARN > 0.15 / FAIL > 0.35 (doc 19). Missing golden = WARN + skip, never a hard fail. **Only record goldens after Ben visually signs off the frames** — re-record at each wave's sign-off.
- Stale screenshots are deleted at run start (a snap timeout counts as MISSING, not silently re-analysing an old frame).

# Reading failures

| Symptom | Meaning |
|---|---|
| every view `var=0.0` + depth/hud FAIL | screenshots never produced — demo crashed or init exceeded the 30 s startup grace (4K HDR CPU IBL bake is the classic cause) |
| `Exit clean: FAIL`, status 3221225477 | 0xC0000005 — if AFTER "frame loop exited cleanly", it's shutdown teardown order (device resources must be destroyed in `render_shutdown_fn`, never after `venus_actor_vk13_run` returns) |
| `hud=FAIL var<50` | no HUD in the top-left 320×80 — Clay init/font failure (check `--dump-stdout` for CLAY_VENUS errors) |
| `depth=FAIL` view 3 | bottom-up centre column found non-grass first — grass missing/black/occluded |
| golden scores 0.09–0.23 WARN on identical code | wind-animation phase drift between runs (known; freeze-time-in-smoke hardening is queued) |
| `(previous message repeated N times)` in dump | a per-frame/per-draw error loop in the demo — find the first occurrence of the message, not the repeats |

`--dump-stdout` is the reliable way to see the demo's own log (direct-run redirects are flaky); the demo's INFO lines are dropped in Release (WARN+ only) unless the tag is registered at INFO.

ctest names: `flagship_smoke_vk13` (gating), `flagship_smoke_gl46`. Related gates: `rg_v2_smoke` (headless graph, 37 asserts/20 s), `venus_perceptual_diff` is embedded in the harness.
