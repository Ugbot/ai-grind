---
name: se-query-timeline
description: Bitemporal queries over a story — who was at a place, what happened when, a character's most recent action, a whole-story timeline. Use when the user asks "who was at X", "what did Y do last", "show me the timeline", "when did Z arrive at the inn".
user_invocable: true
---

# Query the story timeline

Answer the D&D-narrator class of question: who was where when, what did
they do last, what happened at the yew hedge between Tuesday and the
proposal. Backed by the `./story-cli timeline` subcommands, which query
over `Action` and `StoryEvent` records in the database.

## Prerequisites

- Backend up (`start-engine` if not).
- Story has scenes that reference characters + places. Scene creation
  auto-emits an Action per viewpoint + present-character. Stories from
  before that hook need a one-time backfill:
  ```bash
  curl -s -X POST "http://localhost:9876/api/stories/<id>/timeline/backfill" | jq
  ```

## Common questions and how to answer them

### "Who was at <place> at <time>?"

```bash
./story-cli timeline who-was-at <placeId> \
  --start=2026-05-12T09:00 \
  --end=2026-05-12T10:30 \
  --json | jq '.[] | .name'
```

Omit `--start` / `--end` for open windows (beginning of time / now).
Accepts JPA id or domain placeId.

### "What happened at <place> between <t1> and <t2>?"

```bash
./story-cli timeline what-happened-at <placeId> \
  --start=<iso> --end=<iso> --json
```

Returns interleaved actions + events, sorted by start time. Each entry
has a `kind` of either `"action"` or `"event"`.

### "What is <character>'s last action?"

```bash
./story-cli timeline last-action <characterId>
```

Returns the most recent Action record involving that character, in any
place, across all scenes.

### "Where has <character> been?"

```bash
./story-cli timeline places-visited <characterId> --json
```

One row per distinct place with `firstSeen`, `lastSeen`, and `visits`
count.

### "Show me the whole story timeline"

```bash
./story-cli timeline show <storyId>
```

Combined event + action stream, ordered chronologically.

## Recording ad-hoc actions

When the prose doesn't carry the information you need (e.g. "Darcy spent
the night in Lambton between scenes 3 and 4"), record it directly:

```bash
curl -s -X POST "http://localhost:9876/api/stories/<storyId>/timeline/actions" \
  -H "Content-Type: application/json" \
  -d '{
    "actionType": "RESTING",
    "description": "Overnight at the Lambton inn.",
    "startTime": "1812-05-12T22:00",
    "endTime":   "1812-05-13T06:00",
    "placeId":   "<placeId>",
    "characterIds": ["<charId>"]
  }'
```

ActionType values: TRAVELING, ARRIVING, DEPARTING, RESTING, WAITING,
SPEAKING, LISTENING, FIGHTING, PURSUING, HIDING, SEARCHING,
INVESTIGATING, OBSERVING, READING, WRITING, CRAFTING, CASTING, EATING,
DRINKING, TRADING, NEGOTIATING, INTERROGATING, HEALING, SLEEPING,
DYING, WATCHING, PLANNING, CUSTOM.

## Narrative use — the D&D case

When the user is running a session and asks "who did Elsa last talk
to?", call `last-action <elsaId>` — if the returned action is
`SPEAKING`, the `involvedCharacters` list tells you the other
participant(s). Build the narration from:

1. `timeline show` to get the chronological frame.
2. `character-actions <pc>` for each player character.
3. `who-was-at <place>` whenever a scene is about to start to remind
   yourself which NPCs are on stage.

## Related skills

- `se-scene-context` — wider context (characters, place, beats) for one scene
- `se-analyse-story` — narrative shape (tension / pacing / arc progress)
- `se-character-trace` — full character biography across the story
