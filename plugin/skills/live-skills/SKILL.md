---
name: live-skills
description: >
  Create and evolve "live skills": SKILL.md files backed by CRDT documents
  (pycrdt) that agents edit in place and sync between machines, so a skill
  improved on one box updates everywhere. Use when asked to make a skill
  live/shared/self-updating, to record a newly-learned technique into a skill,
  to sync skills with another machine, or when a skill should accumulate
  team knowledge instead of being a static file.
---

# Live skills

A live skill's source of truth is a CRDT text document stored in
`<data_root>/skilldocs.db`, not the file. Every edit is materialized to
`~/.claude/skills/<name>/SKILL.md` (override: `DEVTOOLS_MCP_LIVE_SKILLS_DIR`),
so skill loaders always see the current merged state. Concurrent edits from
different agents and machines merge at character level, so nobody's improvement is
lost to a last-writer-wins clobber.

## Driving it with the `skill_live` tool

```
skill_live(action="create", name="team-lore",
           content="---\nname: team-lore\ndescription: ...\n---\n\n# ...")
skill_live(action="append", name="team-lore", content="- new trick learned today\n")
skill_live(action="patch",  name="team-lore", old="- outdated advice", new="- corrected advice")
skill_live(action="get",    name="team-lore")     # bounded preview
skill_live(action="list")
skill_live(action="sync",   url="http://other-box:8765")   # peer dashboard
skill_live(action="publish")                       # re-materialize all to disk
skill_live(action="delete", name="team-lore")      # local only; peers keep theirs
```

Rules the tool enforces (and why):

- Frontmatter must declare the same `name:` and a `description:`,
  otherwise the doc is stored but not materialized (skill loaders would
  reject it anyway).
- **`patch` old-string must match exactly once** (Edit-tool semantics). Add
  surrounding context to disambiguate.
- **Prefer `append` and `patch` over full rewrites.** Two concurrent full
  rewrites BOTH survive a CRDT merge, so you get the document twice. Surgical
  edits merge cleanly.

## Syncing between machines

Both machines run the devtools service (`scripts/devtools-service.ps1 start`).
`sync` exchanges state-vector diffs with the peer's `/api/skilldoc/` API in
both directions; syncing is idempotent and order-independent, and changes
relay transitively (A→server←B converges all three).

## When to reach for it

- A skill that should **accumulate knowledge** as agents work (perf tricks,
  gotchas, project lore). Append findings as you learn them.
- Skills shared across your machines that must stay identical without a git
  round-trip.
- NOT for the curated library under `skills/`, which is reviewed, committed
  files. Live skills complement them for fast-moving knowledge; graduate a
  stabilized live skill into `skills/authored/` when it settles.
