# Agent notes

## Use the engineering knowledge base

Before any spec, design, research, estimate or new ticket, ask the machine KB:
`eng_kb.context` on the topic and each component it touches (MCP:
`panel_submit_task` with `capabilities_required=["eng_kb"]`, then
`panel_get_execution`; skill `eng-kb`). Cite what it returns (title, status,
source) or say nothing exists. Decided/implemented records are constraints;
going against one is an explicit, argued choice. Brief subagents the same way.
After landing work or writing a design/research doc, commit it where the KB
reads it (docs/research, docs/plans, docs/decisions, module CLAUDE.md) and
trigger `eng_kb.ingest`. If the KB is disabled or unreachable, say so and fall
back to the repo's docs and the tracker.
