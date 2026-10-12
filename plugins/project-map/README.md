# project-map: a Claude Code plugin

Pre-maps any project (code, documentation, anything) so a coding agent knows
**what is where and why** without re-searching it every session:

- a root `CLAUDE.md` (the working agreement) and an `AGENTS.md` **pointer** to it,
- a `PROJECT_MAP.md` that records each area's purpose *and the reason it is shaped that way*,
- one `CLAUDE.md` per area/module (auto-loaded by Claude Code in that directory),
- an engineering-principles doc (DRY, SOLID, KISS/YAGNI, errors, bounds, tests), each with its failure mode,
- a local **SQLite FTS5 / BM25 index** (`kb.py`) with symbol lookup, agent notes that
  go **STALE** when the files they cite change, a map linter, optional
  **embeddings** for hybrid search, and an **MCP server** exposing it all as tools.

Everything is Python 3.9+ standard library. No server, no network, no
dependencies unless you opt into an embedding backend.

## Install

```sh
/plugin marketplace add Ugbot/ai-grind
/plugin install project-map@ai-grind
```

To offer it to everyone who opens a repository, commit this to that repo's
`.claude/settings.json`:

```json
{
  "extraKnownMarketplaces": {
    "ai-grind": { "source": { "source": "github", "repo": "Ugbot/ai-grind" } }
  },
  "enabledPlugins": { "project-map@ai-grind": true }
}
```

## Use

Ask Claude to *"map this project"* (or invoke the `project-map` skill). It
will survey the repo, write the docs, vendor the tools into
`.claude/tools/`, build the index, and run `kb.py check` until it is clean.
Vendoring means the mapped project keeps working for collaborators who
don't have the plugin.

Day to day:

```sh
python3 .claude/tools/kb.py search "wal replay tombstone"
python3 .claude/tools/kb.py symbol find_path
python3 .claude/tools/kb.py note add --topic "why X" --paths src/x.py --body "… verified by …"
python3 .claude/tools/kb.py check
```

or the MCP tools `kb_search`, `kb_symbol`, `kb_notes`, `kb_note_add`,
`kb_note_done`, `kb_index`, `kb_check`, `kb_areas`, `kb_stats`.

The SessionStart hook refreshes the index incrementally, and only in
projects that have opted in (`.claude/kb.json` exists). On a 2,200-file C++/TS
repository a full index takes ~5 s and a no-change refresh ~0.15 s.

## Layout

```
.claude-plugin/plugin.json        plugin manifest
.mcp.json                         project-kb MCP server (stdio)
hooks/hooks.json                  SessionStart incremental index (opt-in projects only)
skills/project-map/SKILL.md       the workflow (bootstrap / retrofit / maintain)
skills/project-map/templates/     CLAUDE.root, AGENTS, PROJECT_MAP, CLAUDE.area, ENGINEERING, STYLE.docs, JOURNAL
skills/project-map/scripts/       kb.py (CLI) · kb_embed.py (optional vectors) · kb_mcp.py (MCP)
tests/test_kb.py                  python3 -m unittest discover -s tests
```

## Optional embeddings

See the "Optional embeddings" section of `skills/project-map/SKILL.md`.
Backends: any OpenAI-compatible endpoint (incl. local Ollama/LM Studio/vLLM/llama.cpp),
native Ollama, sentence-transformers, fastembed, or a stdlib `hash` fallback
(not semantic). Search becomes hybrid BM25 + cosine via reciprocal-rank fusion.

## License

Covered by the repository's root `LICENSE`.
