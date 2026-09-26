---
name: debug-windows-msvc
description: Step-through debugging chukonu test executables with Visual Studio's MSVC debugger on Windows. Use when a test fails locally, an `.exe` segfaults, or you need to watch operator state mid-pipeline. Prefer this over `fprintf` debugging — one breakpoint beats five rebuild-and-print cycles.
---

# debug-windows-msvc — Step-through chukonu .exe files with Visual Studio

You're debugging on Windows with the MSVC toolchain (compiler `cl.exe`,
debugger `vsjitdebugger.exe` / Visual Studio IDE). **No mingw, no
GCC, no gdb.** Project default is `RelWithDebInfo` since SE-0 (full
optimisation + PDBs); plain `Release` has no debug info.

## Build prerequisite

Top-level CMake configures both `Release` and `RelWithDebInfo`. PDBs
land in `build/<Config>/` next to each `.exe`. To rebuild a single
test for debugging:

```powershell
cd C:\code\chukonu
cmake --build build --config RelWithDebInfo --target test_<name>
```

After this, both `build/tests/RelWithDebInfo/test_<name>.exe` AND
`build/tests/RelWithDebInfo/test_<name>.pdb` exist.

## Three ways to attach

### 1. devenv /DebugExe (fastest)

```powershell
devenv /DebugExe build\tests\RelWithDebInfo\test_e2e_sql.exe
```

Visual Studio opens with the .exe loaded and **stops at WinMain**.
Set breakpoints (F9 at any source line), then F5 to run.

If `devenv` isn't on PATH:

```powershell
# Find the right install
& "C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe" -latest -property productPath
# Use the printed devenv.exe directly.
```

### 2. Attach to a running process

When a test hangs:

1. Open Visual Studio.
2. Debug → Attach to Process… (Ctrl+Alt+P).
3. Pick `test_<name>.exe` from the list.
4. Symbol-loaded by PDB next to the .exe — set breakpoints, then
   step. Stop the test from Visual Studio when done.

### 3. JIT debugger on crash

For a SEGV / assertion failure, Windows offers Visual Studio as the
debugger when the .exe crashes. Click "Debug" in the popup; the IDE
opens at the faulting frame. Works without a separate VS launch
because chukonu builds register with the JIT debugger when the
crash dialog appears (Windows default).

## What to watch

### Bolt column shapes (most common SEGV)

When a filter / project / join reads a column index out of range,
the symptom is "ARITHMETIC_OVERFLOW" or "ACCESS_VIOLATION" at the
column `data` pointer dereference. Set a breakpoint on the operator's
`run_fn` entry, step in, and watch:

```
in_b->num_cols
in_b->columns[in_b->read_epoch][col_idx].data    // null = data missing
in_b->columns[in_b->read_epoch][col_idx].type
in_b->columns[in_b->read_epoch][col_idx].type_size_bytes
```

### StringView contents (Utf8 column reads)

A `bolt::StringView` is `{ uint32 length; char prefix[4]; char inline_data[8]; }`.
For `len ≤ 12`, inline; otherwise the suffix lives in a spilled
buffer pointed to by the column's `str_overflow_base`. In the watch
window:

```
sv.length                              // 0 = empty (often a bug)
sv.prefix                              // first 4 bytes always inline
(char*)(&sv.inline_data)               // remaining inline bytes
```

If `length > 12`, the data is at `str_overflow_base[sv.offset()]`
(check `bolt::StringView::offset()` in the source).

### Selection vectors

`Morsel::sel` is the active row indirection. To see which physical
rows the morsel actually points at:

```
m.sel_len
m.sel[0..m.sel_len]
m.batch->num_rows
```

If `sel == nullptr`, every row 0..num_rows-1 is live (dense morsel).

## Conditional breakpoints

Right-click a breakpoint → Conditions. Example: only stop when a
specific table is being scanned:

```
sp->table_id == 1027
```

Use these when the bug is on the 10000th row and `F10` is too slow.

## Tiger Style note

Step-through > printf for diagnosis. One breakpoint with a watch
expression beats five rebuild-and-print cycles. SE-0's
`CHUKONU_LOG_*_FMT` macros are for **production** structured
diagnostics — use them for things that need to surface in CI logs.
For local "what is this value right now" questions, use the
debugger.

## Symbol-server gotcha

Visual Studio by default tries to download symbols for system DLLs
from a Microsoft server. On a closed network this stalls for 30
seconds at startup. Disable in:

  Tools → Options → Debugging → Symbols → uncheck Microsoft
  Symbol Servers.

Local PDBs (the ones next to your `.exe`) load instantly.

## Quick reference

| Key | Action |
|---|---|
| F5  | Run / continue |
| F9  | Toggle breakpoint at cursor |
| F10 | Step over |
| F11 | Step into |
| Shift+F11 | Step out |
| Ctrl+F10 | Run to cursor |
| Ctrl+Shift+F9 | Delete all breakpoints |
| Ctrl+Alt+I | Immediate Window (evaluate expressions) |
| Ctrl+Alt+W,1-4 | Watch window 1-4 |
| Ctrl+Alt+C | Call Stack |
| Ctrl+Alt+G | Disassembly |
