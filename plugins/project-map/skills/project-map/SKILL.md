---
name: project-map
description: Template any repository (code, documentation, or anything else) so a coding agent finds things without re-searching - root CLAUDE.md + AGENTS.md pointer, a PROJECT_MAP.md that says what lives where and WHY, one CLAUDE.md per major area/module, an engineering-principles doc (DRY, SOLID, KISS, YAGNI, tests), and a local SQLite FTS5/BM25 index (optional embeddings, CLI + MCP tools) with agent notes that go stale when their files change. Use when bootstrapping or retrofitting a project, when asked to "map the project", "add CLAUDE.md files", "set up agent docs", "index the repo", or before searching a project that has .claude/kb.json.
---

# project-map: pre-map a project so the agent stops re-researching it

The goal: an agent opening the repo cold answers **"where is X, why is it
shaped like that, and what must I not break"** from files it reads in the
first minute, uses a local BM25 index for anything finer, and never
rediscovers the same fact twice.

| Layer | File(s) | Answers | Loaded |
|---|---|---|---|
| 1. Agreement | `CLAUDE.md` (+ `AGENTS.md` pointer) | how we work here, hard rules, build/test | every turn |
| 2. Map | `PROJECT_MAP.md` + `<area>/CLAUDE.md` (+ optional `<area>/NOTES.md`) | what exists, where, **why**, gotchas | map on demand; area file auto-loads in its dir; NOTES.md only when needed |
| 3. Index | `.claude/kb/index.sqlite` via `kb.py` / MCP `project-kb` | "which file mentions Y", "what did we already learn about Z" | by query |

"Area" = the project's natural unit: a module/package (code), a
chapter/section folder (docs), a service, a dataset family (other).

## Tool locations

- **Bundled with this skill.** `${CLAUDE_PLUGIN_ROOT}/skills/project-map/scripts/kb.py`
  (if that variable is not substituted, use the `scripts/` folder next to this
  SKILL.md). Templates: `${CLAUDE_PLUGIN_ROOT}/skills/project-map/templates/`.
- **In a mapped project.** `.claude/tools/kb.py`. `template` (or `vendor`) copies it
  so the project works for anyone, with or without this plugin. All commands
  below use `KB=.claude/tools/kb.py`.
- **MCP.** when the plugin is enabled (or the project ran `vendor --mcp`),
  the tools `kb_search`, `kb_symbol`, `kb_notes`, `kb_note_add`,
  `kb_note_done`, `kb_index`, `kb_check`, `kb_areas`, `kb_stats` are
  available; prefer them over shelling out.

## Pick the mode

1. **No `.claude/kb.json` yet** (new or existing repo) → §A, then §B.
2. **Templated but `kb.py check` shows stubs / unfilled markers** → §B.
3. **Mapped** → §C, the per-session routine.

## A. Template a project (new or existing): one command

```sh
python3 "${CLAUDE_PLUGIN_ROOT}/skills/project-map/scripts/kb.py" template [--kind code|docs|mixed] [--dry-run] [--mcp]
```

It detects the name, kind, languages, build/test/lint commands (npm/pnpm/yarn,
uv/pytest/ruff, cargo, go, CMake, Gradle, Maven, make, mkdocs), entry points,
the README's opening paragraph, the layout and the areas; then creates
`CLAUDE.md`, `AGENTS.md` (pointer), `PROJECT_MAP.md` (layout tree + one row
per area pre-filled), `docs/ENGINEERING.md` (or `docs/STYLE.md` for docs
projects), `docs/JOURNAL.md`, a stub `CLAUDE.md` per area, vendors the tools
into `.claude/tools/` with a SessionStart index hook, and builds the index.
**It never overwrites an existing file.** It reports what it kept, so it is
safe on an existing repo. Run with `--dry-run` first on anything non-trivial.

Detection fills facts; it cannot know intent. Every remaining gap is a
visible `{{…}}` marker, and `kb.py check` counts them. The template is only
done when an agent has filled them from the code, which is §B.

## B. Fill the map (the part that needs judgement)

Survey, don't rewrite. Existing docs are sources: link them, don't delete them.

1. `$KB areas`: reconcile the detected areas with the build's own units
   (CMake targets, packages, workspaces, chapters). Tune `area_roots`,
   `top_level_areas`, `area_exclude` in `.claude/kb.json`; record the reason
   for every exclusion.
2. For each area, **read the code before writing the doc.** Every gotcha
   names its evidence (file:line, test, command). Unverifiable → write
   "unverified:" or leave it out. Parallelise with one subagent per area;
   tell them: verify, don't infer; stay within budget; edit only their file.
   Change `STATUS: stub` to `STATUS: current` when done.
3. Fill `PROJECT_MAP.md`: the one-paragraph shape, each area's purpose and
   **why**, "where does a new X go", dependency direction, glossary, traps.
   Fill `CLAUDE.md`: the 2 to 7 hard rules and why, anything detection missed.
4. Oversized existing docs: keep what an agent needs *every time* in
   CLAUDE.md; move deep reference/history **verbatim** into a sibling
   `NOTES.md` (not auto-loaded) and link each section. Prove nothing was
   dropped (every original line present in one of the two files).
5. Pre-existing `AGENTS.md`/`.cursorrules`/`CONTRIBUTING.md` rules → merge into
   `CLAUDE.md`, then make those files pointers (template keeps them untouched).
6. `$KB check` → 0 errors and no unfilled markers; report any remaining
   warnings to the user.

## C. Maintain (every session / every change)

- **Search before exploring.** `kb_search` / `$KB search "<terms>"`. It also
  returns matching notes. `$KB symbol <name>` for definitions. Grep only
  when the index misses.
- **Record non-obvious findings** (a gotcha, why something is odd, where a
  concept really lives, a dead end): `kb_note_add` / `$KB note add --topic …
  --paths … --body "<fact + how verified>"`. Notes auto-export to
  `docs/kb-notes.md` (commit it) and are flagged STALE when a named file changes.
- **Graduate** a note that every reader of an area needs into that area's
  CLAUDE.md, then `note done <id>`.
- **Structure changes** (area added/renamed/removed) update PROJECT_MAP.md and
  the area doc in the same change.
- **Wrong claim found.** fix in place, mark `[CORRECTED YYYY-MM-DD: …]`.
- **History goes to `docs/JOURNAL.md`**, never into the map.

## Rules (learned the hard way)

1. **One source of truth per fact.** AGENTS.md points at CLAUDE.md; area docs
   link the root rules instead of restating them. Copies drift within weeks
   and the agent then follows the stale one. (`check` warns at >50% overlap.)
2. **The map explains why, not just where.** "`src/lineage/`: lineage BFS" is
   a listing; "…pure logic, no HTTP, so handlers stay thin; linear-scan
   visited set on purpose (≤256 nodes)" is a map.
3. **Budgets in characters** (≈ tokens × 4), because one table row can be
   5,000 chars: root CLAUDE.md ≤ 12k, PROJECT_MAP.md ≤ 24k, area CLAUDE.md ≤ 12k.
   Configurable in `.claude/kb.json` → `budgets`.
4. **Verified or labelled.** A confident wrong doc costs more than a gap.
5. **Stubs are loud** (`STATUS: stub`); never leave a plausible empty template.
6. **The index is derived.** Rebuild it any time. Notes are the only authored
   content and live in git via `docs/kb-notes.md` (`import-notes` restores).
7. **Principles with judgement.** See `templates/ENGINEERING.md` for each
   principle's failure mode (DRY vs. the wrong abstraction, SOLID vs. ceremony).

## kb.py reference

```sh
$KB init                        # schema + .claude/kb.json + gitignore entry
$KB index [--full] [--quiet] [--if-initialized]
$KB search "q" [-k 10] [--kind md|code|text|note] [--path 'src/%'] [--mode auto|bm25|semantic|hybrid]
$KB symbol NAME                 # regex-extracted definitions (py/js/ts/go/rust/c/c++/java/kt/cs/sh/sql)
$KB areas | check | scaffold [--dry-run] | stats
$KB note add --topic T --body B [--paths p…] | note done ID | notes "q"
$KB export-notes | import-notes
$KB embed [--limit N]           # optional; see below
$KB template [--kind K] [--name N] [--dry-run] [--no-hook] [--mcp]
$KB vendor [--hook] [--mcp]     # copy tools into .claude/tools/, wire hook / MCP
```

Ranking: markdown is chunked by heading, code by top-level definition,
other text by fixed windows; columns `path, heading, symbols, body` carry
BM25 weights 2/5/8/1. camelCase/snake_case symbols are split into words.
Porter stemming; FTS5 syntax works (`"phrase"`, `a OR b`, `pre*`,
`NEAR(a b, 5)`); plain words are ANDed, falling back to OR if nothing matches.

Files: `git ls-files` (tracked + untracked-not-ignored), filtered by
`include`/`exclude` globs and `max_file_kb`. Vendored/submodule dirs such as
`extern/`, `vendor/`, `third_party/` are excluded by default.

### Optional embeddings (hybrid search)

Off by default. Set in `.claude/kb.json`, then `$KB embed` (or
`"auto": true` to embed during `index`). With vectors present, `search`
defaults to **hybrid** (reciprocal-rank fusion of BM25 + cosine).

```json
"embeddings": {"backend": "ollama", "model": "nomic-embed-text", "url": "http://localhost:11434"}
"embeddings": {"backend": "openai", "model": "text-embedding-3-small", "api_key_env": "OPENAI_API_KEY"}
"embeddings": {"backend": "openai", "model": "bge-m3", "url": "http://localhost:8080/v1"}
"embeddings": {"backend": "sentence-transformers", "model": "all-MiniLM-L6-v2"}
"embeddings": {"backend": "fastembed", "model": "BAAI/bge-small-en-v1.5"}
"embeddings": {"backend": "hash"}
```

`openai` = any OpenAI-compatible endpoint (LM Studio, vLLM, llama.cpp, TEI,
Ollama `/v1`). Keys come from the env var named by `api_key_env`, never the
config file. `hash` is stdlib feature hashing, **not semantic**, for tests
and air-gapped use. Changing backend/model drops old vectors automatically.
Sending code to a hosted embedding API sends your source off-machine, so only
configure one when that is acceptable for the project.

## Done criteria

- `CLAUDE.md`, `AGENTS.md` (pointer), `PROJECT_MAP.md`, principles doc exist
  with no unfilled `{{…}}` markers.
- Every area from `$KB areas` has a non-stub CLAUDE.md or a recorded exclusion.
- `$KB check` → 0 errors; any remaining warnings reported to the user.
- Three probe searches in the project's own vocabulary return the right files.
