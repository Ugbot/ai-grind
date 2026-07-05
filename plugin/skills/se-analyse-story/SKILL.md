---
name: se-analyse-story
description: Produce a narrative shape analysis of a story — tension curve, plot points, pacing pattern, theme weights, character arc progress, and warnings. Use when the user asks "how's the pacing", "where's the tension sagging", "analyse this story", "does the midpoint hold".
user_invocable: true
---

# Analyse a story's narrative shape

Cheap post-generation analysis that reads scenes, beats, arcs, and prose
to surface structural metrics. No LLM call — just keyword dictionaries
and position tracking over what's already in the database.

## Input

A story id (JPA or domain). That's it.

## Run

```bash
./story-cli analyse <storyId>
```

Human-readable output shows:
- Tension curve per scene, with a unicode bar chart
- Detected plot points (opening / inciting / first plot point / midpoint
  / crisis / climax / resolution), labelled "(from beat)" when an author-
  tagged beat fired them and "(inferred)" when the position heuristic did
- Pacing pattern (words/scene, avg sentence length, per-scene fast /
  slow / dialogue-heavy / balanced tag)
- Theme weights (occurrences per 1000 words, for 8 theme buckets)
- Character arc progress (type, status, completion %, scene appearances)
- Warnings (sagging tension, under-used arcs, out-of-balance completion)

Prefer `--json` when Claude is consuming it programmatically:
```bash
./story-cli --json analyse <storyId>
```

## What to do with the output

**Tension curve.** If tension dips below 0.35 in the middle third of a
multi-scene story and the max is above 0.6, the analysis will flag it.
Surface that as "the midpoint sags — consider a reveal or escalation."

**Plot points "(inferred)".** Where the analysis had to guess because no
beat was tagged. That's a good prompt to the user: "scene 5 looks like
your climax by tension; shall I mark a CLIMAX beat on it?"

**Pacing tags.** Long runs of the same tag are a smell:
- 5 scenes tagged "slow" in a row → the middle may be dragging
- Every scene "dialogue-heavy" → consider inter-cutting action

**Theme weights.** If the user said the story is about "grief and
forgiveness" and the analysis shows zero weight for `love` / `death` /
`redemption` markers, the prose isn't carrying the themes the scaffold
claims.

**Character arc warnings.** "Arc X has only 2 scenes of on-page presence"
usually means the arc is being *told* in the summary rather than *shown*
in prose.

## When to run this

- Before a writing session: "where are we weak, what scene should I work on"
- After a batch of scene edits: "did this fix the midpoint?"
- When the user can't tell whether a story is "done": the warnings list
  will tell them what's still missing.

## REST equivalent

```
GET /api/stories/{storyId}/analysis
```

Returns the same envelope as the CLI's `--json` mode, useful from the
frontend or MCP tools.

## Related skills

- `se-progress-report` — different view: phase + % complete + open threads
- `se-explore-story` — full structural dashboard
- `se-query-timeline` — bitemporal queries over who/what/where
