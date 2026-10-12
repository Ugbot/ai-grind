---
name: chukonu-dev
description: >
  Build, test, and fuzz chukonu through one driver (tools/ckdev.py) across both
  toolchains: Windows MSVC Release (the perf + correctness gate) and the Linux
  Clang+ASAN Debug container (asserts ON, finds masked bugs). Use when the user
  asks to build, run tests/ctest, run the seeded fuzzer, replay a fuzz seed,
  check the TPC-H coverage gate/board, run the ASAN synthetic all-22, or "run
  the whole loop / everything". The fuzzer is deterministic — every failure
  prints a seed that replays exactly.
---

# chukonu-dev — the build / test / fuzz driver

Everything goes through **`python tools/ckdev.py <cmd>`** (run from the repo
root, `C:\code\chukonu`). One command per task; `all` runs the CI loop.

## Two toolchains (this is the whole mental model)

| | command suffix | dir | what it's for |
|---|---|---|---|
| **Windows MSVC Release** | *(default)* | `build/` | perf + the TPC-H CSV correctness gate |
| **Linux Clang+ASAN Debug** | `--asan` | `build-asan/` | asserts ON + AddressSanitizer; surfaces release-masked bugs. Runs in the `chukonu-clang:19` container mounted at `/work` |

`--asan` works on every subcommand (e.g. `fuzz --asan`). The ASAN path needs a
running container; if there isn't one, start it (see `debug-linux-lldb`):
`docker run --rm -dit -v C:/code/chukonu:/work -w /work chukonu-clang:19 bash`.

## Commands

```bash
# Build (configure + compile). --target T for one target.
python tools/ckdev.py build
python tools/ckdev.py build --asan --target test_tpch_coverage

# Test (ctest). -R REGEX to filter.
python tools/ckdev.py test
python tools/ckdev.py test --asan -R decorrelate

# Fuzz — seeded canonical/lineage fuzzer. Sweep a range, or replay ONE seed.
python tools/ckdev.py fuzz --count 50000        # MSVC sweep
python tools/ckdev.py fuzz --asan --count 50000 # ASAN sweep (asserts ON)
python tools/ckdev.py replay --seed 107         # exact deterministic replay

# TPC-H CSV coverage board (the correctness gate, prints N/22 reached EXEC).
python tools/ckdev.py gate

# Debug+ASAN synthetic all-22 run — the "onion peel": asserts ON catch masked
# bugs; it aborts at the first one (read the assert + use gdb to root-cause).
python tools/ckdev.py asan-synthetic

# The whole loop: build + test + fuzz(20k) + gate  (add --asan for the ASAN loop
# ending in asan-synthetic instead of the CSV gate).
python tools/ckdev.py all
python tools/ckdev.py all --asan
```

Exit code is non-zero on the first failing step, so `all` gates cleanly.

## How to use it (workflow)

- **"run the fuzzer" / "fuzz it"** → `fuzz --count 50000`, then `fuzz --asan
  --count 50000` (the ASAN sweep with asserts ON is where bugs hide). On a
  failure it prints `--seed N`; reproduce with `replay --seed N`, then debug
  with `gdb` in the container (per `debug-linux-lldb`).
- **"build and test" / "run everything"** → `all` (MSVC) and/or `all --asan`.
- **"check the gate"** → `gate` (CSV correctness) and/or `asan-synthetic`
  (Debug+ASAN all-22; a crash there is a real masked bug to root-cause, NOT to
  silence — the asserts are correctness guards).
- After any engine change, the standard confidence pass is:
  `all` then `all --asan` — both green = MSVC gate held + ASAN/asserts clean.

## Discipline

- A failing assert / ASAN report is a real bug. Root-cause it with the debugger
  (`debug-linux-lldb`), don't relax the guard. Fuzz failures replay by seed.
- The fuzz/asan gates sweep FIXED seed ranges so they're reproducible — a
  regression always trips the same seed.
