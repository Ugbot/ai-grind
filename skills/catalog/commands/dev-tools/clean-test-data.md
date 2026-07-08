# clean-test-data — remove ephemeral test artefacts from a project root

Remove test databases, scratch output files, and log dumps that accumulate at a
project's root directory during manual testing. These are never checked in but
clutter `git status` and waste disk.

## What to remove

Scan only the **repository root** (the directory containing `CMakeLists.txt`,
`package.json`, `Cargo.toml`, `pyproject.toml`, or `.git`). Never recurse into
source, extern, build, or node_modules directories.

Remove items that match these patterns:

| Pattern | Rationale |
|---|---|
| `*_data/`, `*_test*/`, `*_fresh*/`, `*_local*/`, `*_brand_new*/`, `*_diag*/` | Ad-hoc test database directories |
| `*_stderr.txt`, `*_stdout.txt`, `*_out.txt`, `*_err.txt` | Process output captures |
| Scratch JSON files at root that aren't config files | e.g. `rt_in.json`, `rt_out.json`, `demote.json`, `missing.json`, test payloads |
| `*.log` files at root that aren't part of the project | Run-time log dumps |

**Never remove:**
- Any file or directory tracked by git (`git ls-files` is the authoritative list)
- Directories named `src/`, `include/`, `lib/`, `extern/`, `build/`, `dist/`,
  `node_modules/`, `target/`, `.git/`, `.claude/`, `frontend/`, `docs/`, `tests/`,
  `cmake/`, `scripts/`, `tools/`, `examples/`
- Config files: `*.toml`, `*.yaml`, `*.yml`, `*.json` at root that are project
  config (e.g. `package.json`, `CMakePresets.json`, `pyproject.toml`, `.mcp.json`)
- Any `.md` / `.h` / `.cpp` / `.ts` / `.tsx` / `.py` / `.rs` / `.go` file

## Procedure

1. Find the repo root:
   ```powershell
   git rev-parse --show-toplevel
   ```

2. List candidates — items at root that match the removal patterns and are NOT
   tracked by git:
   ```powershell
   $root = git rev-parse --show-toplevel
   $tracked = git ls-files --others --exclude-standard   # untracked
   # show only the items we'd actually delete
   Get-ChildItem $root -Force | Where-Object {
     $_.Name -match '_data$|_test|_fresh|_local|_brand_new|_diag' -or
     $_.Name -match '_(stderr|stdout|out|err)\.txt$' -or
     ($_.Extension -in '.log') -and $_.Name -notmatch '^\.git'
   } | Where-Object { -not (git ls-files --error-unmatch $_.FullName 2>$null) }
   ```

3. Show the candidate list to the user. If anything looks unexpected, confirm
   before deleting.

4. Delete confirmed items:
   ```powershell
   Remove-Item -Recurse -Force <path>
   ```

5. Run `git status --short` to verify only the `extern/` submodule dirty marker
   and legitimate uncommitted changes remain.

## Safety rules

- **Check git tracking first.** Never delete a file that `git ls-files
  --error-unmatch <path>` exits 0 for.
- **Ask before deleting anything that might be real data** — e.g. a directory
  named `myapp_data/` could be a real user database. Default is to show the list
  and ask unless the items obviously match the ad-hoc test patterns above.
- **One project root only.** Never walk up or down into sibling repos.
