---
name: venus-render-debug
description: Diagnose Venus rendering bugs (VK-first) — logging discipline, validation layers, per-pass GPU timers, visual bisection, and the failure signatures already catalogued (black output, garbage polygons, stripes, Y-flips, descriptor pool errors). Use when a demo renders wrong/black/corrupt, validation errors appear, or perf budgets need checking.
---

# First moves

1. **Look at the frame** — capture (`cap_window.ps1` or smoke screenshots) and Read the PNG. Visual signatures beat log spelunking:
   - **Solid black geometry** → lighting inputs zeroed: uninitialized/misbound UBO (the grass `GrassVkGlobalsUBO gg;` stack-garbage bug), or a debug-scale field = 0.
   - **Giant flat triangles / "broken polygons"** → drawing uninitialized buffer memory: an upload path that silently no-ops (VK `ghi_buffer_update` was one), or draw-count > written-count (stale SSBO slots).
   - **Content in stripes/bands that shift with camera** → emit/candidate-grid truncation (cap enforced by loop order instead of stochastic thinning).
   - **UI upside-down at the wrong edge** → spurious Y-flip; Clay y-down == Vulkan y-down NDC, NO flip in VK UI shaders.
   - **Black frame with full asset pack** → descriptor pool smaller than the set layout (count the DSL bindings, size the pool to match, log alloc failures loudly).
2. **Logging law**: `LOG_*` + registered tag, never printf. Release drops INFO — register tags at INFO or set `VENUS_LOG_LEVEL=INFO`. Headless/test contexts need `log_flush()` at failure points. `(previous message repeated N million times)` = per-frame error loop: find the FIRST occurrence; also that log call itself is a hot-path bug — make such errors warn-once.
3. **Validation layers**: Debug config enables `VK_LAYER_KHRONOS_validation`; the standard gate is a 20 s+ run with zero errors/VUIDs. No native debugger on this box — deterministic repro + logs, not cdb.

# Instruments

- **Per-pass GPU timers** (RW6): graphs auto-bracket passes; read `rg_graph_pass_gpu_ms(graph, ordered_index)` (frames-in-flight latency; -1 = no sample). GHI level: `ghi_gpu_timer_begin/end/ms` (256 slots). Budgets per wave live in doc 33's ledger.
- **Graph bisection**: `VENUS_USE_GRAPH_V2=0` forces the legacy path even when compiled ON, `=1` forces v2.
- **Live keyboard toggles** are the engine's iteration law — when adding an effect, add its bisect toggle (no F-keys). vk_pbr_test has `DBG_NO_*` shader toggles; 'T' flips grass_v2 vs legacy clumps; scene knobs come from `scene_preset`, never inline.
- **Perceptual diff**: the smoke harness embeds `venus_perceptual_diff` (mean RGB L2, WARN 0.15 / FAIL 0.35) — usable for before/after via saved frames.

# VK-specific traps already hit

- Device teardown order: all VkDevice-resource destruction belongs in the actor's `render_shutdown_fn` (device alive). After `venus_actor_vk13_run()` returns, the device is GONE — touching it = 0xC0000005 + VMAC live-allocation leaks.
- `ghi_buffer_update` semantics: CPU_TO_GPU buffers = mapped memcpy at offset; device-local = staged copy honouring dstOffset. Per-flush data must go to per-flush offsets AND per-frame-slot regions (buffers sized × frames-in-flight) or in-flight frames race.
- Compressed textures: only `ghi_texture_create_from_mips` (2D, explicit chain); auto-mip/`ghi_texture_update` assert on BCn. VK needs `textureCompressionBC`; images skip COLOR_ATTACHMENT usage.
- Descriptors: use `GHI_VkDescFactory` (per-frame pool reset via `venus_vk_backend_active_frame_slot()`) — never vkUpdateDescriptorSets on a persistent set that in-flight frames read.
- The four grass emit copies (CPU / embedded GL / on-disk GL + VK comps) must stay in lockstep until Wave SH single-sources them — check all four when touching emit constants.
