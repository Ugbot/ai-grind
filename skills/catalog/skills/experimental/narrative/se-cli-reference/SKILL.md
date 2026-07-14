---
name: se-cli-reference
description: Complete reference for the Story Engine CLI (./story-cli). Use when you need to know what CLI subcommand handles an operation or what flags a command accepts.
user_invocable: true
---

# Story Engine CLI reference

`./story-cli` is the canonical way for Claude Code to interact with the
Story Engine. The user drives the UI at `http://localhost:3002/` — you
drive the CLI. Do **not** call MCP tools or hit the REST API directly;
everything below goes through the CLI.

## Shape

```
./story-cli [--json] <entity> <action> [--flags...]
```

- `--json` is **always** preferred so the output is parseable with `jq`.
  The human-readable form is for terminal users, not for Claude.
- Every subcommand accepts `--json` at either the root or after the
  entity, e.g. `./story-cli --json story list` or
  `./story-cli story list --json`.
- Ids are UUIDs. The CLI accepts either the JPA primary key or the
  domain-specific id (storyId, characterId, sceneId, etc.) — pass
  whichever you have.

## Performance

Each invocation boots a Quarkus JVM which takes ~3–5 seconds. Batch where
you can: run `jq` locally over a JSON dump instead of re-invoking the CLI.
If you need ten character details, `./story-cli character list --json
--story <id> | jq ...` is one JVM boot; ten `character get` calls is ten.

## Prerequisite

The backend must be up. If `curl -s http://localhost:9876/health` fails,
invoke the `start-engine` skill first.

## Entity commands

### `story`

| Action | Required | Optional | Notes |
|---|---|---|---|
| `create` | `--title` | `--genre`, `--description`, `--synopsis`, `--premise` | |
| `get` | `<id>` | | |
| `list` | — | `--genre`, `--status` | Use `--json` + `jq` to pick fields |
| `update` | `<id>` | `--title`, `--genre`, `--description` | |
| `delete` | `<id>` | | |
| `structure` | `<id>` | | Plots + arcs + chapters + scenes tree |
| `statistics` | `<id>` | | Word count, scene count, etc. |
| `publish` | `<id>` | | |
| `unpublish` | `<id>` | | |

### `character`

| Action | Required | Optional |
|---|---|---|
| `create` | `--story`, `--name` | `--role`, `--physical-description`, `--personality`, `--backstory`, `--motivations`, `--strengths`, `--weaknesses`, `--is-main-character` |
| `get` | `<characterId>` | |
| `list` | `--story` | |
| `update` | `<characterId>` | same flags as create |
| `delete` | `<characterId>` | |

Roles: `PROTAGONIST`, `ANTAGONIST`, `DEUTERAGONIST`, `SUPPORTING`,
`MINOR`, `MENTOR`, `HERALD`, `THRESHOLD_GUARDIAN`, `SHAPESHIFTER`,
`SHADOW`, `ALLY`, `TRICKSTER`.

### `scene`

| Action | Required | Optional |
|---|---|---|
| `create` | `--story`, `--title` | `--summary`, `--place`, `--characters` (comma-sep), `--viewpoint`, `--narrative-text`, `--dialogue-text`, `--action-text`, `--order`, `--scene-type`, `--emotional-tone` |
| `get` | `<sceneId>` | |
| `list` | `--story` | |
| `update` | `<sceneId>` | same as create |
| `delete` | `<sceneId>` | |

### `place`

| Action | Required | Optional |
|---|---|---|
| `create` | `--story`, `--name` | `--description`, `--type`, `--parent` |
| `get` | `<placeId>` | |
| `list` | `--story` | `--hierarchy` for tree view |
| `update` | `<placeId>` | same as create; `--parent=none` clears parent |
| `delete` | `<placeId>` | |

PlaceType: `ROOM`, `BUILDING`, `HOUSE`, `SHOP`, `TAVERN`, `CASTLE`,
`TEMPLE`, `LIBRARY`, `LABORATORY`, `PRISON`, `CAVE`, `FIELD`, `FOREST`,
`MOUNTAIN`, `BEACH`, `DESERT`, `RIVER`, `LAKE`, `GARDEN`, `COURTYARD`,
`STREET`, `MARKET`, `SHIP`, `CARRIAGE`, `TRAIN`, `AIRSHIP`, `VEHICLE`,
`MAGICAL`, `OTHERWORLDLY`, `ABSTRACT`.

### `plot`

| Action | Required | Optional |
|---|---|---|
| `create` | `--story`, `--title`, `--type` | `--description`, `--central-conflict`, `--objective`, `--stakes`, `--is-main-plot`, `--protagonist`, `--antagonist`, `--inciting-incident`, `--climax`, `--resolution` |
| `get` | `<plotId>` | |
| `list` | `--story` | |
| `update` | `<plotId>` | same + `--status`, `--completion`, `--phase` |
| `delete` | `<plotId>` | |

BeatType: `HOOK`, `INCITING_INCIDENT`, `FIRST_PLOT_POINT`, `MIDPOINT`,
`CRISIS`, `CLIMAX`, `RESOLUTION`, `DENOUEMENT`, `PLOT_INTRODUCTION`,
`ESCALATION`, `CONFRONTATION`, `REVELATION`, `REVERSAL`,
`CHARACTER_ESTABLISHMENT`, `CHARACTER_DECISION`, `CHARACTER_GROWTH`,
`INTERNAL_CONFLICT`, `RELATIONSHIP_DEVELOPMENT`, `WORLD_BUILDING`,
`TRANSITION`, `FORESHADOWING`, `CALLBACK`, `CUSTOM`.

BeatImportance: `MINOR`, `STANDARD`, `SIGNIFICANT`, `CRITICAL`, `CLIMACTIC`.

EmotionalTone: `JOYFUL`, `HOPEFUL`, `ROMANTIC`, `TRIUMPHANT`, `HUMOROUS`,
`WARM`, `TENSE`, `FEARFUL`, `SORROWFUL`, `ANGRY`, `DESPERATE`,
`OMINOUS`, `NEUTRAL`, `CONTEMPLATIVE`, `MYSTERIOUS`, `BITTERSWEET`,
`NOSTALGIC`, `SOLEMN`.

Note: `./story-cli beat create --scene=<id>` wants the JPA id. If you
only have the domain `sceneId` at hand, resolve it first via
`./story-cli scene list --story <id> --json | jq '.[] | {jpa: .id, domain: .sceneId}'`.

PlotType: `MAIN_PLOT`, `SUBPLOT`, `QUEST`, `MYSTERY`, `ROMANCE`,
`REVENGE`, `RESCUE`, `ESCAPE`, `PURSUIT`, `COMPETITION`, `WAR`,
`POLITICAL`, `SURVIVAL`, `DISCOVERY`, `TRANSFORMATION`, `HEIST`,
`CONSPIRACY`, `FAMILY_DRAMA`, `CUSTOM`.

### `arc`

| Action | Required | Optional |
|---|---|---|
| `create` | `--story`, `--character`, `--title`, `--type` | `--description`, `--starting-state`, `--desired-ending`, `--internal-conflict`, `--growth-theme` |
| `get` | `<arcId>` | |
| `list` | `--story` | |
| `update` | `<arcId>` | same + `--status`, `--completion`, `--phase`, `--turning-point`, `--resolution` |
| `delete` | `<arcId>` | |

ArcType: `HERO_JOURNEY`, `REDEMPTION`, `FALL_FROM_GRACE`,
`COMING_OF_AGE`, `SELF_DISCOVERY`, `LOVE_AND_RELATIONSHIPS`,
`OVERCOMING_FEAR`, `LEARNING_HUMILITY`, `REVENGE_ARC`, `SACRIFICE_ARC`,
`TRANSFORMATION`, `STATIC_ARC`, `CUSTOM`.

### `chapter`

| Action | Required | Optional |
|---|---|---|
| `create` | `--story`, `--title` | `--number`, `--subtitle`, `--summary`, `--epigraph` |
| `get` | `<chapterId>` | |
| `list` | `--story` | |
| `update` | `<chapterId>` | same + `--status` |
| `delete` | `<chapterId>` | |
| `add-scene` | `<chapterId>`, `--scene` | `--number` |

### `beat`

| Action | Required | Optional |
|---|---|---|
| `create` | `--scene`, `--type` | `--importance`, `--title`, `--description`, `--emotional-tone`, `--pacing-hint`, `--emphasis-notes`, `--order` |
| `list` | `--scene`, `--story` | |
| `get` | `<beatId>` | |
| `update` | `<beatId>` | same as create |
| `delete` | `<beatId>` | |

### `item`

| Action | Required | Optional |
|---|---|---|
| `create` | `--story`, `--name` | `--description`, `--owner`, `--location`, `--is-portable`, `--is-hidden`, `--plot-relevance`, `--significance` |
| `get` | `<itemId>` | |
| `list` | `--story` | |
| `update` | `<itemId>` | same as create |
| `delete` | `<itemId>` | |
| `transfer` | `<itemId>` | `--new-owner`, `--new-location` (`none` clears) |

### `progress`

```
./story-cli progress [--json] <storyId>
```

Returns the full progress envelope: overall %, current phase, plot
threads open/closed, arcs needing attention, word counts, reading time.

### `search`

```
./story-cli search [--json] [--story=<storyId>] [--limit=N] <query>
```

Full-text search across stories, characters, scenes, places, items.
`--story` scopes to one story. Without `--json` you get a terminal-
friendly list; with `--json` you get `{results: [{type, id, title,
content, score, metadata}], totalResults, searchTimeMs}`.

## jq recipes

```bash
# Story ids by title
./story-cli story list --json | jq -r '.[] | "\(.storyId[:8])  \(.title)"'

# One story's scene titles in order
./story-cli scene list --story <id> --json | jq -r 'sort_by(.globalOrder // 9999)[] | "\(.globalOrder)  \(.title)"'

# Progress as a one-liner
./story-cli progress --json <id> | jq -r '"\(.completionPercentage)%  \(.currentPhase)  words=\(.currentWordCount)"'

# Find a character by name
./story-cli search --json --limit 5 "Calla" | jq '.results[] | select(.type=="character")'
```

## Error conventions

- Missing required flags exit non-zero with a Picocli-style "Missing
  required option" message on stderr.
- Not-found errors go to stdout as `{"error":"..."}` (when `--json`) or
  a formatted message otherwise.
- If you get a connection refused or startup error, the backend isn't
  running — invoke the `start-engine` skill.

## Companion skills

- `start-engine` — get the backend up
- `se-list-stories` — quick list + filter
- `se-explore-story` — deep-dive on one story
- `se-search` — full-text search across stories
- `se-check-plot` — plot thread status
- `se-character-trace` — a character's scenes, arcs, relationships
- `se-play-story` — scene-by-scene walkthrough
- `se-write-story` — create a new story end-to-end
