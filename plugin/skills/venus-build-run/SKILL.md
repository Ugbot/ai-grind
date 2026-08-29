---
name: venus-build-run
description: Build Venus targets (CMake/MSVC build-msvc dir) and run demos correctly — target names, config flags, CWD/asset-path gotchas, env vars, and how to capture + inspect a demo frame. Use when asked to build, run, or screenshot any Venus executable, or when a demo "renders nothing"/"can't find assets".
---

# Build

One build tree: `build-msvc/` (MSVC multi-config; Release is the default working config, Debug enables VK validation layers).

```bash
cd C:/code/Venus
cmake --build build-msvc --target <target> --config Release
```

- Filter output: `2>&1 | grep -E "error|\.exe$"` — full MSVC output floods the context.
- Reconfigure only when CMakeLists changed: `cmake -S . -B build-msvc`. **`option()` defaults do NOT override an existing cache** — flipping a default in CMakeLists needs `-DTHE_OPTION=ON` once to update the cache.
- Zero-warnings law: pre-existing offenders are jobs.c/camera_rts.c C4133 + voxel macro redefs + libuv (don't add new ones; don't "fix" those in passing without a task).
- zig build exists (`build.zig`) but MSVC is the daily driver; smoke zig before claiming cross-build parity.

Key targets: `flagship_demo` (GL), `flagship_demo_vk` (VK flagship — THE visual bench), `vk_pbr_test` (VK PBR harness + presets), `Venus`, `space_game`, `space_rts`, `venus_editor`, `venus_playground`, `venus_cook` (asset cooker), `flagship_smoke_test`, and unit tests (`vasset_unit_test`, `ghi_format_unit_test`, `rg_v2_smoke`).

# Run demos

- **GL demos load shaders/assets CWD-relative → run from the repo root** (`cd C:/code/Venus && ./build-msvc/Release/flagship_demo.exe`). Running from the exe dir = "Failed to load shader file" errors.
- **VK demos** copy their `.spv` (and the HUD font) next to the binary post-build; they run from anywhere, but repo root is still safest.
- Asset pack: `VENUS_HELLO_ASSETS` (default probe: `C:/code/mars/external/HelloVulkan/Assets`) enables Sponza + skinned models + pack HDRIs. The pack's 4K HDRs make the CPU IBL bake take minutes — prefer the in-repo 1K `assets/textures/hdr/kloppenheim_07_puresky_1k.hdr`.
- Useful env: `VENUS_LOG_LEVEL=INFO` (Release defaults WARN), `VENUS_USE_GRAPH_V2=0|1` (two-way graph bisection), `VENUS_SCENE_PRESET=<name>` (vk_pbr_test), `VENUS_HELLO_IBL_HDR=<path>` (explicit HDR override).
- Backgrounding from bash: `(./demo.exe > log 2>&1 & echo $! > pid) && sleep 20 && kill $(cat pid)`. Direct-run logs are sometimes empty (pipe-vs-console quirk) — the smoke harness's `--dump-stdout` is the reliable capture (see venus-smoke-golden).

# Capture + inspect a frame

```bash
powershell -ExecutionPolicy Bypass -File C:/code/Venus/cap_window.ps1 -Match "<window title substr>" -Out C:/code/Venus/build-msvc/cap.png
```
Then **Read the PNG with the Read tool** — you can see the frame and diagnose visually (black = unlit/zeroed UBO; giant flat triangles = garbage/uninitialized buffers; stripes = emit truncation). Smoke screenshots land at `build-msvc/smoke_view_vk13_<n>.png`.

Kill stray demos before rebuilding (exe lock ⇒ LNK1104):
`powershell -Command "Get-Process flagship_demo_vk,flagship_smoke_test -ErrorAction SilentlyContinue | Stop-Process -Force"`
