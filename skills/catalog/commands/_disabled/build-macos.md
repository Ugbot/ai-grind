# build-macos — CMake + LLVM/Clang on macOS

You are executing a CMake-based C++ build on macOS using the Bash tool.
Follow every rule below exactly.

---

## Compiler constraint

**LLVM/Clang only. Never GCC.**
- macOS ships Apple Clang (LLVM-based) as the default `clang`/`clang++`. This is acceptable.
- Homebrew LLVM (`brew install llvm`) gives a newer/more standard LLVM and is preferred for C++20.
- Never use `gcc`/`g++` — even if present, GCC lacks MSVC-parity for the project's Windows-first codebase and produces different warnings.
- If `gcc --version` shows "Apple clang", it's actually Clang and is fine. But pass `-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++` explicitly to be safe.

### Checking what compiler you have
```bash
clang --version    # should say "Apple clang" or "LLVM"
clang++ --version
```

### Using Homebrew LLVM (recommended for C++20 completeness)
```bash
brew install llvm
export CC="$(brew --prefix llvm)/bin/clang"
export CXX="$(brew --prefix llvm)/bin/clang++"
```
Or pass directly to cmake:
```bash
cmake -S . -B build \
  -DCMAKE_C_COMPILER="$(brew --prefix llvm)/bin/clang" \
  -DCMAKE_CXX_COMPILER="$(brew --prefix llvm)/bin/clang++"
```

---

## Step-by-step build procedure

### 1. Prerequisites
```bash
# Xcode command-line tools (gives Apple Clang + make + git)
xcode-select --install

# Ninja (faster than Make for incremental builds)
brew install ninja cmake

# Java 11+ for ANTLR grammar regen (only needed if editing .g4 files)
brew install openjdk@17
```

### 2. Submodule init
```bash
git submodule update --init --recursive
```

### 3. Configure
```bash
cmake -S . -B build \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++
```
With explicit Homebrew LLVM:
```bash
cmake -S . -B build \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER="$(brew --prefix llvm)/bin/clang" \
  -DCMAKE_CXX_COMPILER="$(brew --prefix llvm)/bin/clang++"
```

### 4. Build
```bash
cmake --build build --parallel
```
Single target:
```bash
cmake --build build --target test_e2e_sql --parallel
```

### 5. Run tests
```bash
ctest --test-dir build --output-on-failure
```
Single test:
```bash
ctest --test-dir build -R chukonu_e2e_sql --output-on-failure
```

### 6. Run binary directly
```bash
./build/tests/test_e2e_sql 2>&1
```

---

## Chukonu-specific configuration

- **Primary development OS is Windows.** macOS is a secondary target.
- **No vcpkg / conan.** All deps are submodules under `external/`.
- **Expected test baseline:** 35/38 passing (three pre-existing failures: `sqllogictest_driver`, `op_filter_col_eq`, `e2e_sql_equijoin`).
- **SIMD:** The project uses `bolt::branchless` AVX2 kernels. On macOS Apple Silicon (ARM64), AVX2 doesn't exist — CMake/Bolt will fall back to scalar paths automatically. Do not add `-march=native` without checking Bolt's SIMD tier detection first.
- **BOLT_SIMD_TIER:** set by Bolt's CMake. On Intel Mac it will be `AVX2` or `NATIVE`. On Apple Silicon it will be `SCALAR` or `NEON`. Do not override.

---

## Common failure modes

| Symptom | Fix |
|---|---|
| `clang: error: unsupported option '-mavx2'` | Apple Silicon; Bolt falls back to scalar — rebuild with Bolt's SIMD=SCALAR |
| `ld: symbol(s) not found for architecture arm64` | Mixing ARM64 and x86_64 objects; ensure all deps built for same arch |
| CMake picks up Homebrew GCC instead of Clang | Pass explicit `-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++` |
| `fatal error: 'bolt/bolt_arena.h' not found` | Submodules not initialized: `git submodule update --init --recursive` |

---

## Quick-reference

```bash
git submodule update --init --recursive
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```
