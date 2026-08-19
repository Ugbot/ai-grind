# Unified skills library

One browsable home for every Claude Code skill, slash command, and agent we use.
Assets come from this machine's projects (`C:\code\*`), the global `~/.claude`
config, and open-source collections we vendor as we go (see
[Vendored external skills](#vendored-external-skills-borrowed-with-thanks)).
Everything here is copied, never moved. Upstream projects stay untouched, and
every borrowed skill keeps its origin, author, and license on record.

## Why two trees

Claude Code discovers skills only as flat `<name>/SKILL.md` folders under a
`skills/` root. It does not recurse into category sub-folders, so the library
keeps the browsable tree and the loadable mirror separate:

- `catalog/` holds harvested skills, organized by category. Browse this one.
  `harvest.py` wipes and rebuilds it from upstream.
- `authored/` holds skills written for this library, such as the PowerShell set.
  Committed source. `harvest.py` never touches it.
- `loadable/` is the flat mirror Claude can load. `sync.py` generates it by
  merging `catalog/` and `authored/`. Gitignored, because it is derived output.

## Workflow

```bash
python harvest.py                 # upstream  -> catalog/   (+ MANIFEST.json)
python sync.py --target local     # catalog/  -> loadable/             (default)
python sync.py --target plugin    # catalog/  -> <repo>/plugin/  (committed plugin bundle)
python sync.py --target project   # catalog/  -> <repo>/.claude/       (load here)
python sync.py --target global    # catalog/  -> ~/.claude/   (load everywhere)
```

The `plugin` target writes the flat, committed bundle that
`.claude-plugin/plugin.json` points at (`plugin/{skills,commands,agents}/`).
Re-run it after editing a skill, then commit `plugin/`. `sync.py` warns loudly
and skips any manifest source missing from the checkout, so a partial clone still
builds.

`harvest.py` is idempotent. It wipes `catalog/` and rebuilds it from the explicit
list in `sources.toml`. `sync.py --target local` wipes and rebuilds `loadable/`.
The `project` and `global` targets overwrite per item, so unrelated skills
already living there survive.

## What's inside

| Type | Count | Categories |
|---|---|---|
| Skills (harvested, local) | 4 | `debug/` `profiling/` `project-drivers/` |
| Skills (harvested, vendored external) | 79 | `planning/` `build/` `review/` `ship/` `web/` `meta/` `writing/` `understanding/` `principles/`, listed below |
| Skills (authored) | 29 | `powershell/` (9), `profiling/` (7), `debugging/` (2), `devtools/` (5), `tracker/` (4), `collab/` (2) |
| Commands | 5 | `build/` (3) `dev-tools/` (2) |
| Agents | 6 | `docs/` `testing/` `integration/` `review/` (3) |

Total loadable: 112 skills (83 harvested plus 29 authored).

Run `python ../scripts/unslop_check.py` before committing prose changes. It
encodes the `unslop` skill's rules as a check over the markdown and Python this
repo owns, and skips vendored and generated trees.

### Sidelined

Some skills stay in `catalog/` for reference and never reach the loadable
mirrors or the router index. `sync.sidelined()` enforces the rule: any category
with an `experimental` segment, or one starting with `_`. That currently covers
the Story Engine `se-*` skills under `experimental/narrative/`, the retired
MCP-era skills under `narrative/_archive/`, the four retired `llm-station-*`
skills under `_disabled/`, and the `llm-station/` commands including the `ned-*`
variants.

Keep the on-disk category in step with `sources.toml`. The router builds its
index by walking the tree, so a skill marked `_disabled` in `sources.toml` but
still sitting in an active category folder gets advertised as loadable when it
is not.

### Skills (local harvest)

- `debug/`: `debug-windows-msvc`, `debug-linux-lldb`
- `profiling/`: `bench-rdtsc-profile`
- `project-drivers/`: `chukonu-dev`
- `experimental/narrative/` (sidelined): 14 `se-*` Story Engine skills plus
  `start-engine`, kept for reference, never synced or indexed
- `_disabled/` (sidelined): `llm-station-analyze`, `llm-station-search`,
  `llm-station-patterns`, `llm-station-debug`

These four active ones only fire where their checkout exists: `chukonu-dev` and
both `debug-*` skills need the chukonu tree, `bench-rdtsc-profile` needs
MarbleDB. Use `skill_live action="disable"` to drop them from a machine that
lacks the repo.

## Vendored external skills (borrowed with thanks)

We vendor selected open-source skills as we go: copied verbatim into `catalog/`,
credited here, and cross-referenced in
[`THIRD_PARTY_SKILLS.md`](THIRD_PARTY_SKILLS.md) with a per-skill keep or skip
rationale. Upstream clones live in `C:/code/vendor-skills/`. Full credit and
thanks to the authors:

| Collection | Author | License | Took | Categories |
|---|---|---|---|---|
| [addyosmani/agent-skills](https://github.com/addyosmani/agent-skills) | Addy Osmani | MIT | 17 | planning, build, review, ship, web |
| [obra/superpowers](https://github.com/obra/superpowers) | Jesse Vincent (obra) | MIT | 8 | planning, review, ship |
| [mattpocock/skills](https://github.com/mattpocock/skills) | Matt Pocock | MIT | 9 | planning, review, ship |
| [anthropics/skills](https://github.com/anthropics/skills) | Anthropic | Apache-2.0 | 3 | meta, web |
| [cursor/plugins](https://github.com/cursor/plugins) | Lauren Tan (`pstack`), Cursor (`cursor-team-kit`, `thermos`, `cli-for-agent`) | MIT | 42 plus 3 agents | writing, understanding, planning, review, meta, build, ship, principles |

`MANIFEST.json` records each vendored skill's exact upstream `origin` path. We
left behind repo-owner-specific, creative, Office-document, and
proprietary-licensed skills, plus anything that duplicated a skill already here.
`THIRD_PARTY_SKILLS.md` names every rejection and why, including the 10 skills
retired in the 2026-08-19 dedup pass.

The cursor/plugins harvest added three categories:

- `writing/` (3): `unslop` cuts AI tells from prose and is the standing pass over
  any writing, `deslop` does the same for AI-generated code, `technical-writing`
  layers Diátaxis structure over Google style and Simplified Technical English.
- `understanding/` (4): `how` explains how a subsystem works, `why` recovers the
  reasons behind its shape from git, tickets, docs, chat, and observability,
  `teach` runs both and blends the result, `recall` reconstructs where you left
  off.
- `principles/` (15): terse engineering principles that `architect`, `arena`, and
  `figure-it-out` cite by name. Six upstream principles duplicated skills already
  here, so we dropped them.

### Commands

- `build/`: `build-windows`, `build-macos`, `build-linux`
- `dev-tools/`: `clean-test-data`, `sync-to-ai-grind`
- `llm-station/` (sidelined): `build callers grep refs rename search start status
  stop task` plus the `ned-*` variants, kept in `catalog/`, never synced

### Agents

- `docs/`: `living-docs-writer`
- `testing/`: `test-bench-runner`
- `integration/`: `frontend-backend-connectivity-checker`
- `review/`: `comment-sicko`, `thermo-nuclear-review-subagent`,
  `thermo-nuclear-code-quality-review-subagent` (cursor/plugins, MIT)

### Authored skills in `authored/skills/`

Written for this library:

- `powershell/` (9) covers using PowerShell properly, with 5.1 and 7 side by
  side: `pwsh-core-idioms`, `pwsh-native-commands`, `pwsh-errors`,
  `pwsh-filesystem`, `pwsh-text-and-data`, `pwsh-jobs-async`,
  `pwsh-env-and-packages`, `pwsh-scripting-style`, `pwsh-non-interactive`.
- `profiling/` (7): `flamegraph-reading`, `etw-profiling`, `jvm-profiling`,
  `jvm-threads-heap`, `python-profiling`, `js-node-profiling`,
  `renderdoc-frame-analysis` for GPU frame capture and replay.
- `debugging/` (2): `cdb-windows-debug`, `unified-debugging`.
- `devtools/` (5): `devtools-mcp-usage`, `devtools-visualizer`, `build-tools` for
  maven, gradle, npm, pnpm, yarn, and cargo, `vtune-profiling`, and `skills-sync`
  for this library's harvest and sync workflow plus the `skills_sync` MCP tool.
- `tracker/` (4): `tracker-usage`, `tracker-breakdown`, `tracker-acceptance`,
  `tracker-github-sync`.
- `collab/` (2): `agent-collab`, `live-skills`.

To add another authored skill, drop a `<name>/SKILL.md` anywhere under
`authored/skills/` and re-run `sync.py`. The folder name must match the
frontmatter `name:`.

## Provenance and dedup

`MANIFEST.json` records every item's upstream origin path and a sha256 of its
defining file. To add or refresh an asset, edit `sources.toml`, the single
explicit work-list, then re-run `harvest.py`.

Duplicates collapsed to one canonical copy:

- The four `llm-station-*` skills exist in three identical upstream copies
  (`.claude`, `.agents`, `src/ralph`) plus a global one. Only the project
  `.claude` copy is harvested, and all four are now retired.
- `build-*` commands come from chukonu, identical to the global copies.
- `debug-windows-msvc` and `debug-linux-lldb` live only in global `~/.claude`.

`harvest.py` asserts that skill names are globally unique and fails loudly on a
collision instead of overwriting.
