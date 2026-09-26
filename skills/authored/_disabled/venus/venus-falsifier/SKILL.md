---
name: venus-falsifier
description: How to write a Venus negative control (falsifier) that actually discriminates, explicit atomic setter lever called from main(), one printed sentinel line, PASS_REGULAR_EXPRESSION-only ctest registration (never bare WILL_FAIL), the CTest inversion semantics, and the MSVC env-lever trap. Use when adding a WILL_FAIL twin, when a control passes vacuously, or when a reviewer says "not discriminating".
---

# The rule

A gate is trusted only when its negative control goes red **for the intended
reason**, a different line than the positive, switched by a lever the engine
actually reads. 13 of the Stage A wave controls were vacuous on first pass
(doc 34 s2). Every one of them had one of these shapes:

| Vacuous shape | Why it passes without proving anything |
|---|---|
| `WILL_FAIL TRUE` alone | ANY nonzero exit passes: a crash, an unrelated CHECK, a missing fixture. |
| lever via `uv_os_setenv` in the test process, engine reads `getenv` | On MSVC `SetEnvironmentVariable` never refreshes the CRT environment; the lever is never taken. |
| regex with a line number | The first fixture edit moves the line; the control silently stops matching. |
| control red on the same line as the positive | The positive is red too; the pair discriminates nothing. |

# The shape that works

**1. Lever = explicit setter, called from the test's `main()` before any world
or server exists.** Precedents: `venus_physics_set_force_serial(bool)`,
`venus_mesh_active_mask_force(mode)` (`src/physics/venus_collide_tri.h:170`),
`job_system_set_legacy_retry(bool)`, `wyrd_net_server_set_phase_tally_enabled`,
`wyrd_catalogue_set_ceiling_extra_layers`. Store in `_Atomic` with
`VENUS_ASSERT(atomic_is_lock_free(...))`. The test's `main()` may read an env
var or argv to decide whether to call the setter, that read happens in the
test, never in the engine.

**2. One sentinel line, printed once, after the soak, with the numbers:**

```c
LOG_INFO(TAG, "MESH_EDGES_FORCE_STATE mode=%s failures=%llu stream_leg_red=%d",
         mode_name, (unsigned long long)failures, stream_leg_red);
/* or on the falsify leg only: */
LOG_ERROR(TAG, "FALSIFY_NO_RETRY inline_fallbacks=%u legacy_calls=%u", ...);
log_flush();
```

**3. Registration, regex only, no WILL_FAIL:**

```cmake
venus_add_test(NAME physics_mesh_edges_falsify_face_only
        TARGET physics_mesh_edges_test ARGS --face-only
        LABELS "physics;gate;falsifier" TIMEOUT 120)
set_tests_properties(physics_mesh_edges_falsify_face_only PROPERTIES
        PASS_REGULAR_EXPRESSION "MESH_EDGES_FORCE_STATE mode=FACE_ONLY failures=[1-9][0-9]* ")
```

CTest evaluates `PASS_REGULAR_EXPRESSION` first, then `WILL_FAIL` INVERTS the
verdict, combining them makes a red-for-the-right-reason run report Failed.
Use the regex alone; the exit code is irrelevant to the verdict. Live
examples: `cmake/tests_platform.cmake` (venus_jobs_admission_falsify_no_retry),
`cmake/tests_physics_units.cmake` (physics_mesh_edges_falsify_*),
`cmake/tests_wyrdholm.cmake` (wyrd_terrain_catalogue_falsify_ceiling),
`cmake/targets_wyrdholm_server.cmake` (wyrd_spawn_falsify_ladder).

**4. Prove it by hand once** and record raw rc + the sentinel line in the
receipt: run the binary with the lever, confirm the red line differs from the
positive's, and that the positive prints the sentinel with the green values.

# Also required

- The control runs >= 20 s like the positive (same soak loop; "each round
  must refuse by name; exit 1 iff every round did").
- The regex names NO line numbers and NO absolute counts that a fixture edit
  changes; use `[1-9][0-9]*` / `[0-9]+`.
- Where a real fixture cannot make the knob visible (e.g. a face-only mover
  still stops on two wall planes), add the leg that does (edge-on approach to
  a lone wall), a knob that no leg reddens is an unfalsified claim.
- The falsifier's own `main()` prints "lever X set to <value> (falsification
  lever)" so a log reader can see it was armed.
