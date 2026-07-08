# sync-to-ai-grind — add this project's skills to the ai-grind unified library

Register the current project's `.claude/commands/*.md` and `.claude/skills/`
items in `C:\Users\Capta\ai-grind\skills\sources.toml`, then rebuild the
harvested catalog and the flat loadable mirror.

## What ai-grind is

`C:\Users\Capta\ai-grind\` is the unified skills library. It pulls skills and
commands from every project on this machine into one browsable `catalog/` tree
and a flat `loadable/` mirror that Claude Code can load:

```
ai-grind/skills/
  sources.toml    <- the explicit work-list of upstream assets
  harvest.py      <- sources.toml -> catalog/  (run first)
  sync.py         <- catalog/ + authored/ -> loadable/  (run second)
  catalog/        <- harvested copies, organized by category
  authored/       <- hand-written skills (never touched by harvest.py)
  loadable/       <- flat mirror Claude loads (gitignored, derived)
```

## Procedure

### 1. Find the current project's skills/commands

List what exists at `.claude/commands/*.md` and `.claude/skills/*/SKILL.md`
relative to the current working directory (the project root).

```powershell
Get-ChildItem .claude/commands -Filter "*.md" -ErrorAction SilentlyContinue
Get-ChildItem .claude/skills   -Filter "SKILL.md" -Recurse -ErrorAction SilentlyContinue
```

### 2. Check which items are already in sources.toml

Read `C:\Users\Capta\ai-grind\skills\sources.toml` and find any `[[item]]`
blocks whose `src` path matches the files found above. Skip items already
registered — never add duplicates.

### 3. Add new items to sources.toml

For each new file, append an `[[item]]` block to the end of `sources.toml`
(before any trailing comment if present):

```toml
[[item]]
src      = "C:/code/<project>/.claude/commands/<name>.md"
type     = "command"
category = "<project-name>"
note     = "<one-line description>"
```

For skills (folder-form with SKILL.md):
```toml
[[item]]
src      = "C:/code/<project>/.claude/skills/<name>"
type     = "skill"
category = "<project-name>"
note     = "<one-line description>"
```

Choose a category name that matches the project — e.g. `gestalt2`, `chukonu`,
`marbledb`. Check existing entries in `sources.toml` for the naming convention.

### 4. Run harvest then sync

```powershell
Set-Location "C:\Users\Capta\ai-grind\skills"
python harvest.py
python sync.py --target local
```

`harvest.py` reads `sources.toml`, copies each item into `catalog/`, and
writes `MANIFEST.json`. `sync.py` flattens `catalog/` + `authored/` into
`loadable/`.

If `harvest.py` exits non-zero (duplicate name collision, missing file, etc.),
fix the `sources.toml` entry and re-run before proceeding to `sync.py`.

### 5. Verify

```powershell
Get-ChildItem "C:\Users\Capta\ai-grind\skills\loadable\commands" | Select-Object Name
Get-ChildItem "C:\Users\Capta\ai-grind\skills\loadable\skills"   | Select-Object Name
```

Confirm the newly registered items appear in `loadable/`.

## Safety rules

- **Never modify `authored/`** — those are hand-written originals.
- **Never delete `catalog/` or `loadable/` manually** — let `harvest.py` /
  `sync.py` manage them.
- **Never add duplicate `src` paths** to `sources.toml`. Check first.
- If a skill name (frontmatter `name:` in SKILL.md) conflicts with an existing
  entry, `harvest.py` will fail with a clear error — rename the upstream skill
  and re-run.
- `sources.toml` is the single source of truth. Edit only that file; never
  edit files under `catalog/` directly.
