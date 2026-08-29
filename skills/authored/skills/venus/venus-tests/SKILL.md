---
name: venus-tests
description: Run Venus's test suites the right way — ctest labels, the key gates per subsystem, the 20-second rule, and headless SERVERSIDE testing. Use when verifying changes, deciding which tests gate a diff, or adding a new test target.
---

# Running

```bash
cd C:/code/Venus
ctest --test-dir build-msvc -C Release -L <label> --output-on-failure
# or a single suite:
ctest --test-dir build-msvc -C Release -R vasset_unit --output-on-failure
# or run the exe directly (faster iteration):
./build-msvc/Release/vasset_unit_test.exe
```

Labels → gates:
- `asset` — `vasset_unit` (format/reader, 145 checks), `venus_cook_pbr` + `venus_cook_verify` (cook the pbr corpus headless, then validate every output).
- `rendering` — `ghi_format_unit` (BCn/mip byte math, 46 checks); `rg_v2_smoke` (headless graph: compile/order/barriers + 20 s runtime floor, 37 asserts).
- `flagship` — `flagship_smoke_vk13` (GATING: views + golden diff), `flagship_smoke_gl46` (frozen GL: boot-only). Needs a display + `-DVENUS_BUILD_SMOKE_TESTS=ON`.
- `core` — `venus_ds_unit`, `entity_hash_index`, tick-tock gates (`entity_ticktock_test`), physics suites (`physics_*`, `net_session*` etc. per `tests/MAP.md`).

# Rules

- **Run tests for at least 20 seconds** (project law) — soak-style suites (rg_v2_smoke, physics, net) embed their own runtime floor; don't trim them.
- Headless tests compile with `SERVERSIDE` and must not touch GL/VK/GLFW — GHI symbols resolve via `src/rendering/render_graph/rg_v2_ghi_stub.c` (add a stub there for every new `ghi_*` you introduce, or every SERVERSIDE test breaks at link).
- Asserts are the test surface (engine convention): tests count CHECKs and print a total; validation-clean 20 s+ runs are the acceptance bar for anything visual.
- New unit test = `add_executable` + `add_test` + `set_tests_properties(... LABELS "<area>" TIMEOUT <s>)` in CMakeLists (follow the `vasset_unit_test` block), file in `tests/`.

# What gates what (quick map)

| Change area | Minimum gates before commit |
|---|---|
| GHI / formats | `ghi_format_unit`, build GL + VK demo targets, 20 s demo run |
| render_graph_v2 | `rg_v2_smoke`, `flagship_smoke_vk13` |
| grass/UI/flagship visuals | `flagship_smoke_vk13` (+ eyeball the PNGs; goldens only re-record on sign-off) |
| asset format/cook | `vasset_unit`, `venus_cook_pbr` + `venus_cook_verify` |
| entity/tick-tock | `entity_ticktock_test`, `entity_hash_index` |
