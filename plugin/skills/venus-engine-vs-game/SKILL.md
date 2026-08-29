---
name: venus-engine-vs-game
description: Where code goes in Venus — engine vs game. Use BEFORE writing or placing any rendering/lighting/physics/streaming feature, when reviewing a diff that adds engine-shaped code to src/neon, src/wyrdholm, src/networked_rts, src/showcase or examples/, or when deciding whether to promote game code into the engine. Encodes the standing centralisation rule (Ben, 2026-08-14), the LOOK-vs-MECHANISM test, the promotion checklist, and the red-flag list from the 2026-08-14 audit.
---

# Engine vs game code in Venus

## The standing rule (Ben, 2026-08-14, verbatim)

> "we have an engine and some games. all lighting features should be in the
> engine. shadows, antialiasing, forward+, nanite etc. but at the moment they
> are dotted about in the wrong places. its now a standing rule to centralise
> reusable features in the renderer."

Corollary already in force: **a missing or wrong engine API is fixed IN THE
ENGINE, never worked around in the game/harness.** Fix the engine, migrate the
consumer, delete the game copy. Never patch a game-side copy of a mechanism.

Why it's load-bearing, twice proven on 2026-08-14 alone:
- VENG-482's shadow stability fix (sphere fit + world-anchored snap) landed in
  the engine — and NEON, `vk_pbr_test` and `space_env_vk`, which hand-roll
  their own CSM light-VPs, **silently didn't get it**. Scattered mechanisms
  mean every fix lands 1-of-N times.
- The city streamer hit `VK_ERROR_DEVICE_LOST` in `mesh_destroy`. The fix
  (deferred buffer retirement) went into the engine mesh tier, so every
  future streaming consumer gets it — not into the showcase.

## The test: LOOK vs MECHANISM

**MECHANISM = engine-only.** Sampling, culling, fitting, binning, light
selection, shadow/AO/GI math, tonemapping/exposure composition, fog models,
bloom chains and their barriers, pipeline/layout ownership, descriptor
lifecycle, buffer lifecycle, camera-relative rebasing, AA decisions,
render-target/HDR/format decisions, streaming/retirement policy.

**LOOK = game-legal.** Colours, material constants, art-directed intensities,
glyph shapes, faction hues, per-game shader *appearance* code — PROVIDED the
shader `#include`s the shared `shaders/inc/` mechanism bodies (brdf, ibl,
cluster_light, shadow_pcf, fog, forward_clustered_frag_body) rather than
re-implementing them. `nrts_ship_cluster_vk.frag` is the model citizen;
`city_interior_vk.frag` (own fog, own light lookup, own clamps) is the
counter-example.

Gray zone rule: if the engine exports a calibration constant (e.g.
`SPACE_ENV_BLOOM_THRESHOLD/KNEE`), the game includes it — never restates the
number.

## How a game gets a feature: DECLARE, don't implement

- **`VenusRenderFeatures`** — the tier ladder. The game says
  `venus_render_features_high()`; the engine decides what that buys (the
  MSAA/VENG-650 precedent: WYRDHOLM ships antialiased with zero AA code).
- **`venus_scene_view_vk`** — the turnkey ground-game view: prims, static-lit
  meshes, terrain look, water, grass, characters, GI, shadows, capture. One
  desc + per-frame submits.
- **`venus_space_view_vk`** — the turnkey space view (env → hulls → FX,
  phase-asserted).
- **Scene presets (`src/scene/scene_preset.c`)** — sun/IBL/fog/shadow
  extents/camera are named preset DATA. Harnesses pick by id; no inline magic
  numbers.
- **Program seams** — where a game legitimately owns the look, the engine
  declares the interface and the game supplies the SPIR-V:
  `VenusInstancedHullProgram`, `venus_screen_icon_vk`'s program,
  `venus_mesh_program_vk`, `VenusSkinnedProgram`. Required config REFUSES
  LOUDLY (Debug assert AND Release refusal) — never a silent default of
  someone else's look.

## The promotion checklist (the T'-6/7/8 shape, four times proven)

1. Move the TU verbatim first when possible; prove pixel-identity or state
   why it cannot be proven (frame nondeterminism → envelope, and SAY so).
2. Split by cohesion: `_build.c` (built once, incl. create/destroy) / main TU
   (per frame) / `_internal.h` (shared struct + sizes). TUs ≤ ~1000 lines.
3. Everything the engine has no business knowing becomes REQUIRED declared
   config; NULL refuses loudly. COPY caller tables at create (caller's table
   is routinely a render_init stack local).
4. The boundary allowlist (`tools/boundary_allowlist.txt`) must SHRINK —
   every wave leaves it smaller; entries get deleted, never laundered with
   `--update-baseline`.
5. Reflected layouts (SH-8), never hand-coded descriptor tables. Nothing
   under `shaders/` moves when only ownership moves.
6. Record the work in the AREA's MAP.md; the router only changes when an
   area appears/disappears.

## Red flags in review (each one was found live in the 2026-08-14 audit)

- A game TU writing engine struct internals (`sav->slice_vp[]` by hand).
- A camera-centred, unsnapped shadow matrix built game-side.
- A second lighting path (own light list/sort/lookup, own fog, own clamps)
  inside one subsystem's renderer.
- Hand-built descriptor sets against an engine driver's DSL in a demo main
  when `cluster_forward_vk_make_env_set` + `_set_env_*` already exist.
- A hand-copied barrier "transplant" around an engine pass — fix the pass to
  self-barrier; deleting N copies at once is the payoff.
- Bloom/exposure constants restated per subsystem.
- Engine-quality policy (light selection with hysteresis, LOD ladders,
  residency) living under `src/procedural/` or a game dir — promote it.
- A game with a bespoke renderer and ZERO `VenusRenderFeatures` declarations
  (NEON is the standing example; its endgame is scene-view consumption).
- GL46 code: FROZEN. Never repair or migrate it; port the scenario or delete
  (commit → tag → push first, per the deletion rule).

## Where the bodies are buried

- Epic **VENG-844** tracks the migration backlog with sizes.
- `src/rendering/AGENTS.md` — the working rules for the renderer itself.
- `docs/design/engine/42-aaa-bar-program.md` — AD-42-17 (procedural =
  engine primitive, multi-output), AD-42-18 (the abstraction pass: every
  wave ends by folding hand-wiring into declarative surfaces).
- STARFALL (`src/networked_rts/main_vk.c`) is the reference consumer: one
  desc, engine-included shader bodies, engine-exported constants.
