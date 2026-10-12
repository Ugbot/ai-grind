---
name: debug-linux-lldb
description: Step-through debugging chukonu test executables under Linux (in a Docker container) with LLDB and AddressSanitizer. Use when a Windows-only bug needs Linux reproduction, or you need ASAN to catch a heap-OOB / use-after-free the MSVC debugger can't see. Clang/LLD only — never GCC, never mingw.
---

# debug-linux-lldb — Step-through chukonu under LLDB + ASAN in Docker

You're on Windows but want a Linux build, ASAN coverage, and LLDB
step-through. The toolchain is **Clang only — no GCC**, linked with
**LLD — no GNU ld**. We run everything inside a Docker container so
the Linux toolchain doesn't pollute the Windows host.

## Container

Base image is `silkeh/clang:19`. Chukonu uses a derived image
`chukonu-clang:19` that adds Ninja + git so CMake can build out of
the box. If the image isn't local, build it from:

```dockerfile
# Dockerfile.chukonu-clang
FROM silkeh/clang:19
RUN apt-get update && apt-get install -y --no-install-recommends \
    ninja-build git \
 && rm -rf /var/lib/apt/lists/*
```

Build with: `docker build -t chukonu-clang:19 -f Dockerfile.chukonu-clang .`

## One-shot interactive session

```powershell
# Windows PowerShell (MSYS_NO_PATHCONV stops Git Bash from mangling /work)
docker run --rm -it `
    --cap-add=SYS_PTRACE `
    --security-opt seccomp=unconfined `
    -v C:/code/chukonu:/work `
    -w /work `
    chukonu-clang:19 bash
```

The two `--cap-add=SYS_PTRACE` / `--security-opt seccomp=unconfined`
flags let LLDB attach to processes inside the container. Without
them, `lldb` says "operation not permitted" on every `attach`.

`MSYS_NO_PATHCONV=1` is only needed if you run this from Git Bash;
PowerShell doesn't mangle paths.

## Build inside the container

```bash
# Once per shell — initialise submodules and configure with ASAN.
git submodule update --init --recursive

cmake -B build-asan -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DCMAKE_C_FLAGS="-fsanitize=address" \
    -DCMAKE_CXX_FLAGS="-fsanitize=address" \
    -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address -fuse-ld=lld"

# Speeds up re-runs in fresh containers (skip the ANTLR fetch).
# -DFETCHCONTENT_SOURCE_DIR_ANTLR4_RUNTIME=/work/build/_deps/antlr4_runtime-src

cmake --build build-asan -j
```

The build dir is `build-asan/` — separate from the Windows `build/`
so the two trees don't clobber each other.

## Run with ASAN, no debugger

For a quick OOB / use-after-free triage:

```bash
ASAN_OPTIONS=abort_on_error=0:print_stacktrace=1 \
  ./build-asan/tests/test_<name>
```

ASAN prints a full stack trace at the first violation. Read it from
the top — the offending frame is usually within 2-3 lines of the
crash. **Half the time this is all you need.**

For the other half — when ASAN says "ABORTING" before printing
anything useful, or when the bug is "wrong answer" not "crash" — use
LLDB.

## LLDB session

```bash
lldb ./build-asan/tests/test_<name>

# Common workflow inside lldb:
(lldb) b filter_op.cpp:200          # breakpoint at line 200
(lldb) b chukonu::ops::ingest_row   # breakpoint at a function
(lldb) r                            # run
# ...stops at first breakpoint...
(lldb) frame variable               # show locals
(lldb) p st->col_idx                # print one variable
(lldb) p in_b->num_cols             # ditto
(lldb) p *st                        # full state struct
(lldb) bt                           # backtrace
(lldb) n                            # step over
(lldb) s                            # step into
(lldb) c                            # continue
(lldb) q                            # quit
```

### Attaching to a running test

When a test hangs:

```bash
# Terminal A: run the test
./build-asan/tests/test_<name>

# Terminal B (same container — open with `docker exec -it <id> bash`)
lldb -p $(pgrep -f test_<name>)
(lldb) bt all                       # see every thread's stack
(lldb) thread select 1
(lldb) frame variable
```

## What to watch (same as Windows skill)

The structures are identical — Bolt column shapes, StringView
inline/spilled layout, selection vectors. The LLDB syntax differs
from MSVC's watch window but the data is in the same fields.

```
(lldb) p in_b->columns[in_b->read_epoch][col_idx].data
(lldb) memory read --size 1 --count 16 sv.inline_data
(lldb) p m.sel_len
```

## When ASAN's stack trace is enough (the 90% case)

Most foundation-test failures don't need LLDB. The pattern:

1. Test fails locally (Windows) or in CI.
2. Reproduce with ASAN: `./build-asan/tests/test_<name>`.
3. Read the ASAN report. It says exactly which heap region was
   touched, by what code, after what allocation.
4. Fix.

LLDB earns its keep when you need to **observe state mid-execution**
(values across iterations, predicate evaluation in a complex
expression tree, etc.) — the same niche as MSVC step-through.

## Tiger Style note

Same as the Windows skill: step-through and ASAN beat printf for
diagnosis. The `CHUKONU_LOG_*_FMT` macros (SE-0) are for production
structured logs; for local "what does this look like right now"
questions, use the debugger or ASAN.

## Quick reference

| Command | Action |
|---|---|
| `b foo.cpp:42` | breakpoint at line |
| `b ns::Class::fn` | breakpoint at function |
| `br list` | list breakpoints |
| `br delete N` | delete breakpoint N |
| `r` / `run` | start / restart |
| `c` / `continue` | resume |
| `n` / `next` | step over |
| `s` / `step` | step into |
| `finish` | step out |
| `p expr` | print expression |
| `frame variable` | show all locals |
| `bt` | backtrace |
| `thread list` | list threads |
| `thread select N` | switch to thread N |
| `q` / `quit` | exit |
