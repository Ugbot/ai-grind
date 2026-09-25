# build-windows — CMake + MSVC/Clang-CL on Windows (PowerShell)

You are executing a CMake-based C++ build on Windows using PowerShell.
Follow every rule below exactly. Never use Bash for builds on Windows — use the PowerShell tool.

---

## Compiler constraint

**MSVC (`cl.exe`) or Clang-CL only. Never GCC.**
- Default generator `"Visual Studio 17 2022"` uses MSVC automatically.
- To use Clang-CL instead, add `-T ClangCL` to the cmake configure command.
- Never pass `-DCMAKE_C_COMPILER=gcc` or `-DCMAKE_CXX_COMPILER=g++`.

---

## Finding MSBuild

`MSBuild.exe` is NOT on the system PATH by default. Never call it bare.
Use one of these two patterns:

**Pattern A — cmake --build (preferred, always works):**
```powershell
cmake --build build --config Release
```
cmake knows where MSBuild is; this is the safest call.

**Pattern B — direct MSBuild via vswhere (when you need per-target builds):**
```powershell
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (Test-Path $vswhere) {
    $vsPath = & $vswhere -latest -requires Microsoft.Component.MSBuild -property installationPath
    $msbuild = Join-Path $vsPath "MSBuild\Current\Bin\MSBuild.exe"
} else {
    $msbuild = "C:\Program Files\Microsoft Visual Studio\2022\Community\MSBuild\Current\Bin\MSBuild.exe"
}
if (-not (Test-Path $msbuild)) {
    Write-Error "MSBuild not found. Install Visual Studio 2022 with C++ workload."; exit 1
}
```
Then call as:
```powershell
Set-Location build
& $msbuild ALL_BUILD.vcxproj /p:Configuration=Release /p:Platform=x64 /m /nologo 2>&1 | Select-Object -Last 20
```

---

## Step-by-step build procedure

### 1. Configure
```powershell
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
```
Re-run this only when CMakeLists.txt or cmake/ files change.
CMake will regenerate automatically during `cmake --build` if needed, but explicit re-configure is cleaner.

### 2. Build
```powershell
cmake --build build --config Release
```
To build a single target (e.g. one test):
```powershell
cmake --build build --config Release --target test_e2e_sql
```

### 3. Run tests
```powershell
ctest --test-dir build -C Release --output-on-failure
```
To run a single named test:
```powershell
ctest --test-dir build -C Release -R chukonu_e2e_sql --output-on-failure
```

### 4. Run a test binary directly (when you need stderr output)
```powershell
& "build\tests\Release\test_e2e_sql.exe" 2>&1
```

---

## Chukonu-specific configuration

This project lives at `C:\code\chukonu`. Key facts:

- **Generator:** `"Visual Studio 17 2022"` with `/A x64`
- **Config:** always `Release` (Debug is untested and much slower due to no optimizations on Bolt kernels)
- **No vcpkg / conan.** External deps are git submodules under `external/`.
- **Submodule check:** before building after a fresh clone or pull, run:
  ```powershell
  git submodule update --init --recursive
  ```
- **Expected test baseline:** 35/38 passing. Three pre-existing failures are **normal**:
  - `chukonu_sqllogictest_driver`
  - `chukonu_op_filter_col_eq`
  - `chukonu_e2e_sql_equijoin`
  Any failure beyond these three is a regression.
- **CMake regen:** required after editing `.g4` grammar files or after the `cmake/` directory changes. Run configure again.
- **ANTLR4 jars:** `tools/antlr-4.13.2-complete.jar` is checked in. Grammar regen needs Java 11+.
- **Parallel build:** `/m` (MSBuild) or `--parallel` (cmake --build) is safe to use.

---

## Common failure modes

| Symptom | Fix |
|---|---|
| `MSBuild.exe: command not found` | Use `cmake --build` or find MSBuild via vswhere (see above) |
| `LINK : fatal error LNK1181: cannot open input file 'marbledb.lib'` | Run `git submodule update --init --recursive` |
| CMake error `The C++ compiler ... is not able to compile` | VS C++ workload not installed; install "Desktop development with C++" in VS Installer |
| Tests spin forever / hang | Pre-existing TIMEOUT=30 guard will kill them; if they all hang, suspect a deadlock in ParallelDriver |
| `error C2220: warning treated as error` | A new warning was introduced; fix the warning, don't disable it |

---

## Quick-reference: full build + test in one block

```powershell
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

Expected last line: `35 tests passed, 3 tests failed` (the three known failures).
After adding a new task's test: `36 tests passed, 3 tests failed`, etc.
