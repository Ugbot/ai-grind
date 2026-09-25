---
name: venus-asset-cook
description: Cook and validate Venus assets with venus_cook (.vtex/BCn textures, VAsset containers, the binary manifest) and extend the cooker with new asset types. Use when cooking textures, adding cookable formats, debugging a .vtex/.vasset load, or working on doc-34 asset-pipeline stages.
---

# Cooking

```bash
cd C:/code/Venus
cmake --build build-msvc --target venus_cook --config Release
./build-msvc/Release/venus_cook.exe <assets_dir> <cooked_dir> [--force] [--verify]
# e.g. the CI corpus:
./build-msvc/Release/venus_cook.exe assets/textures/pbr build-msvc/cooked_test
./build-msvc/Release/venus_cook.exe assets/textures/pbr build-msvc/cooked_test --verify
```

- **Internally incremental**: the manifest (`<cooked_dir>/manifest.vman`) stores per-source content hashes; a clean rerun no-ops in ~0.2 s. `--force` recooks; `--verify` re-opens every entry and validates header/chunks/levels (headless, no GPU — the CI gate).
- Usage class from the SOURCE filename: albedo/basecolor/diffuse/emissive → **BC7 sRGB**; normal → **BC5** (shader reconstructs Z); roughness/metallic/ao/height/opacity/mask → **BC4**; unknown → BC7 sRGB. Expect ~4× compression (pbr set: 370 MB RGBA8 → 88 MB).
- Encoders: BC7 = `3rdparty/bc7enc16` (vendored C99, hard-forkable); BC4/BC5 = `3rdparty/stb/stb_dxt.h`. Mips: `stb_image_resize2` — **always from mip 0**, sRGB-aware for colour, linear + per-mip renormalize for normals. HDR is uncompressed RGBA16F until a C BC6H lands (doc 34 Stage 4b).

# Format layer (src/asset/)

- `vasset_format.h` — container spec: 64 B header (magic "VSET", `asset_id` = `vasset_hash_path()` FNV-1a of the canonical source-relative path — lowercased, slash-folded; **tool and runtime share the inline impl**), 32 B chunk entries, **256 B-aligned payloads**, LE-only, version = recook-never-migrate, unknown chunks skipped. `.vtex` payload = `VtexInfo` (literal VkFormat) + mip-major `VtexLevel` table.
- `vasset_reader.{h,c}` — no-I/O, no-alloc `vasset_view_open/find/data`; every malformed case is an explicit `VAssetReadResult` (never assert on file content).
- `vasset_manifest.{h,c}` — fixed 8192 entries + 512 KB strings, atomic tmp+rename save, **asset-id collision = hard error at cook**.

Tests: `vasset_unit_test` (145 checks incl. malformed matrix + truncation sweep), ctest label `asset` (`venus_cook_pbr`, `venus_cook_verify`).

# Extending

- New texture class: extend `classify()` + `class_vk_format()` in `tools/venus_cook/cook_texture.c`.
- New asset type: add a `VASSET_TYPE_*` + chunk FOURCCs to `vasset_format.h` (additive — never renumber), a `cook_<type>.c` in the tool, a case in `cook_main.c`'s scan, and `--verify` coverage. Runtime consumption goes through the async actor (doc 34 Stage G), GHI upload via `ghi_texture_create_from_mips` (the ONLY legal path for compressed formats).
- Keep `_stricmp`→`strcasecmp` and dirent/Win32 walk portability (`cook_main.c` shows the pattern); the tool must build with NO engine link (Linux CI cooks headless).
