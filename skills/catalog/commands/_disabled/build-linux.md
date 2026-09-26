# build-linux — CMake + LLVM/Clang on Linux

You are executing a CMake-based C++ build on Linux using the Bash tool.
Follow every rule below exactly.

---

## Compiler constraint

**LLVM/Clang only. Never GCC.**
- Do NOT use `gcc`/`g++`, even if they are the system default.
- Do NOT let cmake auto-detect the compiler — always pass it explicitly.
- The project uses MSVC as the primary compiler. Clang is the closest-parity alternative on Linux. GCC may produce different warnings, accept non-portable code, or diverge from MSVC on subtle C++20 corners.
- Minimum: Clang 14. Recommended: Clang 17+.

### Install Clang
```bash
# Debian/Ubuntu
sudo apt-get install -y clang-17 lld-17 ninja-build cmake

# RHEL/Fedora/Rocky
sudo dnf install -y clang llvm ninja-build cmake

# From LLVM's official apt repo (newest version):
wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && sudo ./llvm.sh 17
```

### Verify
```bash
clang-17 --version    # should show "clang version 17.x"
clang++-17 --version
```

---

## Step-by-step build procedure

### 1. Submodule init
```bash
git submodule update --init --recursive
```

### 2. Configure
```bash
cmake -S . -B build \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang-17 \
  -DCMAKE_CXX_COMPILER=clang++-17 \
  -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=lld"
```

If `clang-17` is not versioned (just `clang`):
```bash
cmake -S . -B build \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++
```

**Never omit `-DCMAKE_C_COMPILER` / `-DCMAKE_CXX_COMPILER`** — cmake will default to GCC if Clang is not the system default.

### 3. Build
```bash
cmake --build build --parallel $(nproc)
```
Single target:
```bash
cmake --build build --target test_e2e_sql --parallel $(nproc)
```

### 4. Run tests
```bash
ctest --test-dir build --output-on-failure
```
Single test:
```bash
ctest --test-dir build -R chukonu_e2e_sql --output-on-failure
```

### 5. Run binary directly
```bash
./build/tests/test_e2e_sql 2>&1
```

---

## Chukonu-specific configuration

- **Primary development OS is Windows.** Linux is a secondary target; CI will eventually enforce it.
- **No vcpkg / conan.** All deps are submodules under `external/`.
- **Expected test baseline:** 35/38 passing (three pre-existing failures: `sqllogictest_driver`, `op_filter_col_eq`, `e2e_sql_equijoin`). Any failure beyond these three is a regression.
- **SIMD:** The project uses AVX2 kernels from Bolt. On x86_64 Linux, these will fire if the CPU supports AVX2. On ARM64, Bolt falls back to scalar automatically.
- **LTO/LLD:** Using `-fuse-ld=lld` (LLVM's linker) is strongly preferred — it's faster than `ld` and avoids GNU ld TLS emulation quirks that can bite Bolt's `thread_local` state.
- **Address Sanitizer (optional, debugging only):**
  ```bash
  cmake -S . -B build-asan -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
    -DCMAKE_C_FLAGS="-fsanitize=address" \
    -DCMAKE_CXX_FLAGS="-fsanitize=address" \
    -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address -fuse-ld=lld"
  ```

---

## Common failure modes

| Symptom | Fix |
|---|---|
| CMake picks up GCC | Explicitly pass `-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++` |
| `error: use of undeclared identifier '__builtin_ia32_*'` | CPU/target doesn't support AVX2; Bolt's SIMD tier detection should handle this — check `cmake/` outputs |
| `fatal error: 'bolt/bolt_arena.h' not found` | Submodules not initialized: `git submodule update --init --recursive` |
| Link errors about `std::filesystem` | Add `-lstdc++fs` or use Clang 11+ which links it automatically |
| `GLIBCXX_3.4.XX not found` at runtime | stdlibc++ version mismatch; link against libc++ instead: add `-stdlib=libc++` to CXXFLAGS |
| Tests time out | The 30s TIMEOUT guard will catch them; a deadlock in `ParallelDriver` is the most likely cause |

---

## Quick-reference

```bash
git submodule update --init --recursive
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=lld"
cmake --build build --parallel $(nproc)
ctest --test-dir build --output-on-failure
```
