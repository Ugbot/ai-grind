# Archived MCP-era skills

These skills assume Claude Code is driving the Story Engine through its
stdio MCP server (`.mcp.json` + `run-mcp-stdio.sh`). The project has
since moved to CLI-driven automation via `./story-cli`, so these files
are parked here rather than under `.claude/skills/` — Claude Code does
not load anything under a subfolder.

Kept because:

1. The MCP server still exists and works. Anyone who prefers the MCP
   flow can copy these back up one level.
2. The workflow notes (create-story phases, action verbs, MCP tool
   semantics) are still useful reference for the equivalent CLI skills.

To re-enable, move the file up:

```bash
git mv .claude/skills/archive-mcp/story-status.md .claude/skills/
```

Then run `claude mcp reset-project-choices` and restart Claude Code so
it re-prompts to approve the `story-engine` MCP server from
`.mcp.json`.
