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
python sync.py --target plugin    # cross-platform -> <repo>/plugin/          (committed; plugin ai-grind-skills)
python sync.py --target plugin-windows  # Windows-only -> <repo>/plugin-windows/ (committed; plugin ai-grind-windows)
python sync.py --target project   # cross-platform -> <repo>/.claude/   (load here)
python sync.py --target global    # cross-platform -> ~/.claude/        (per-item overwrite)
python sync.py --target global --prune   # ... and remove retired library items from ~/.claude
python sync.py --target agents-global    # cross-platform skills -> ~/.agents/skills/ (wiped mirror)
```

### Targets

| Target | Writes | Ownership | Platform |
|---|---|---|---|
| `local` | `skills/loadable/{skills,commands,agents}/` | wiped (gitignored) | cross-platform |
| `plugin` | `plugin/{skills,commands,agents}/` | owned subfolders wiped, `plugin/.claude-plugin/plugin.json` kept | cross-platform |
| `plugin-windows` | `plugin-windows/skills/` | owned subfolders wiped, manifest kept | Windows-only |
| `agents` | `<repo>/.agents/skills/` | wiped (gitignored), skills only | cross-platform |
| `agents-global` | `~/.agents/skills/` | `skills/` wholly owned and wiped, so retired skills vanish; other `~/.agents` content survives; skills only (Codex, Cursor) | cross-platform |
| `project` | `<repo>/.claude/` | per-item overwrite | cross-platform |
| `global` | `~/.claude/{skills,commands,agents}/` | per-item overwrite; `--prune` also removes retired items | cross-platform |

A skill is Windows-only when its category has a `windows` path segment
(`authored/skills/windows/powershell/pwsh-*`, `windows/debugging/cdb-windows-debug`,
`windows/profiling/etw-profiling`; the same rule applies to harvested catalog
categories). Every target except `plugin-windows` excludes those; `plugin-windows`
includes only those.

`global --prune` removes entries in `~/.claude/{skills,commands,agents}` that the
library does not currently publish, but only names the library has listed: any
`sources.toml` row (including `_disabled` ones) or any authored skill or agent
(including `_disabled`). Unrelated user skills are never touched. Windows-only
skills count as "not published" globally, so prune clears them from `~/.claude`;
install them through `ai-grind-windows` instead.

### Plugins

Generic skills reach agents only through a Claude Code plugin. The marketplace
(`.claude-plugin/marketplace.json`, name `ai-grind`) lists:

- `ai-grind-skills` (`./plugin`): 119 cross-platform skills, 1 command, 6 agents.
  No MCP server, so it never collides with an HTTP devtools-mcp server.
- `ai-grind-windows` (`./plugin-windows`): 11 Windows-only skills (9 `pwsh-*`,
  `cdb-windows-debug`, `etw-profiling`). Install on Windows machines only.
- `devtools-mcp` (`.`): the stdio MCP server plus the same cross-platform bundle.
  Do not install it next to an HTTP devtools-mcp server.
- `project-map` (`./plugins/project-map`): unchanged.

```bash
claude plugin marketplace add <path-to-ai-grind-or-github:Ugbot/ai-grind>
claude plugin install ai-grind-skills@ai-grind
claude plugin install ai-grind-windows@ai-grind     # Windows only
```

Re-run the plugin targets after editing a skill, then commit `plugin/` and
`plugin-windows/`. `sync.py` warns loudly
and skips any manifest source missing from the checkout, so a partial clone still
builds.

`harvest.py` is idempotent. It wipes `catalog/` and rebuilds it from the explicit
list in `sources.toml`. `sync.py --target local` wipes and rebuilds `loadable/`.
The `project` and `global` targets overwrite per item, so unrelated skills
already living there survive (`global --prune` is the explicit, name-scoped exception).

## What's inside

| Type | Count | Categories |
|---|---|---|
| Skills (harvested, vendored external) | 99 | `planning/` `build/` `review/` `ship/` `web/` `meta/` `writing/` `understanding/` `principles/`, listed below |
| Skills (authored) | 31 | `windows/` (11: `powershell/` 9, `debugging/` 1, `profiling/` 1), `profiling/` (6), `debugging/` (1), `devtools/` (7), `tracker/` (4), `collab/` (2) |
| Commands | 1 | `dev-tools/` (`clean-test-data`) |
| Agents | 6 | `integration/` (3, including authored Reuse Harvester and Cloudflare Controller), `review/` (3) |

Committed plugins: `ai-grind-skills` has 119 skills (99 harvested plus 20 authored), 1 command and 6 agents;
`ai-grind-windows` has the 11 Windows-only authored skills.

Cloudflare integration was selectively published on 2026-10-21: 19 official
`cloudflare/skills` source rows, authored `devtools/cloudflare-control`, and the
authored `integration/cloudflare-controller` agent. All 749 prior output files
and 125 prior manifest entries survived unchanged. Repeating the same selected
publication changed no output or manifest bytes. Official
Cloudflare skills stay verbatim and authoritative. The authored skill coordinates
MCP/cf/Wrangler authentication boundaries and secret-free S3 configuration.
Focused qualification: 63 tests pass; source lint/format and new prose checks
pass. Existing prose findings elsewhere remain separate. The older local and
`.agents` mirrors received only selected assets; they were not fully rebuilt.
[Evidence](../docs/research/cloudflare-integration-20261021.md).

`harvest.py` expands `~` in explicit sources. For partial checkouts, both scripts
support `--additive --select NAME` (repeat `--select` per exact asset). Selected
mode stages/preflights requested inputs, refuses conflicting destinations,
preserves unrelated entries/directories, skips identical files and resumes
compatible partial output. Metadata follows successful output publication.
Selected sync permits plugin/local/agents only; agents is skills-only. Default
full harvest/sync behavior is unchanged and may wipe generated directories, so
do not use it to publish Cloudflare while legacy sources are unavailable.

Run `uv run --no-project python ../scripts/unslop_check.py` before committing prose changes. It
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

The local project-specific skills are no longer published.
`debug-windows-msvc`, `debug-linux-lldb`, `bench-rdtsc-profile` and `chukonu-dev`
are retained in `_disabled/`, alongside the retired `llm-station-*` skills.
Story Engine skills remain in `experimental/narrative/`; authored Venus skills
are retained under `authored/_disabled/`. None are published in generated mirrors.

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

- `_disabled/`: `build-windows`, `build-macos`, `build-linux`, `sync-to-ai-grind`
- `dev-tools/`: `clean-test-data`
- `llm-station/` (sidelined): `build callers grep refs rename search start status
  stop task` plus the `ned-*` variants, kept in `catalog/`, never synced

### Agents

- `_disabled/`: `living-docs-writer`, `test-bench-runner` (retained, not published)
- `integration/`: `frontend-backend-connectivity-checker`, authored `reuse-harvester`
- `review/`: `comment-sicko`, `thermo-nuclear-review-subagent`,
  `thermo-nuclear-code-quality-review-subagent` (cursor/plugins, MIT)

### Authored skills in `authored/skills/`

Written for this library:

- `windows/powershell/` (9, Windows-only) covers using PowerShell properly, with 5.1 and 7 side by
  side: `pwsh-core-idioms`, `pwsh-native-commands`, `pwsh-errors`,
  `pwsh-filesystem`, `pwsh-text-and-data`, `pwsh-jobs-async`,
  `pwsh-env-and-packages`, `pwsh-scripting-style`, `pwsh-non-interactive`.
- `profiling/` (6; `etw-profiling` lives in `windows/profiling/`): `flamegraph-reading`, `jvm-profiling`,
  `jvm-threads-heap`, `python-profiling`, `js-node-profiling`,
  `renderdoc-frame-analysis` for GPU frame capture and replay.
- `debugging/` (1): `unified-debugging`; `cdb-windows-debug` lives in `windows/debugging/`.
- `devtools/` (6): `devtools-mcp-usage`, `devtools-visualizer`, `build-tools` for
  maven, gradle, npm, pnpm, yarn, and cargo, `vtune-profiling`, and `skills-sync`
  for this library's harvest and sync workflow plus the `skills_sync` MCP tool;
  `reuse-harvesting` curates reusable artifacts, qualification notes and roles.
- `tracker/` (4): `tracker-usage`, `tracker-breakdown`, `tracker-acceptance`,
  `tracker-github-sync`.
- `collab/` (2): `agent-collab`, `live-skills`.

To add another authored skill, drop a `<name>/SKILL.md` anywhere under
`authored/skills/` and re-run `sync.py`. The folder name must match the
frontmatter `name:`.

Authored Markdown agents live under `authored/agents/<category>/<name>.md`.
`sync.py` publishes them to each target that supports agents (not the skills-only `agents`, `agents-global` and `plugin-windows`), preserving
unrelated entries in shared targets and rejecting case-insensitive collisions with catalog or
other authored names. Categories with an `_` prefix or `experimental` segment
remain sidelined. `--target agents` publishes skills only. Native Codex TOML
roles travel as bundled skill assets; sync does not convert or register them.
The Reuse Harvester skill installs no global configuration or scheduler.
Its [review ledger](../docs/research/reuse-harvest-integration-20261020.json)
records reviewed source assets, decisions and executed checks.

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
