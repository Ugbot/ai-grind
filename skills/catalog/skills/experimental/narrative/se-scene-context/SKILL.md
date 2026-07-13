---
name: se-scene-context
description: Pull every piece of already-established context for a single scene before writing prose. Use before generating or updating narrative/dialogue/action text so the prose stays consistent with the structure already in the database.
user_invocable: true
---

# Scene context recall

The prose Claude writes for a scene has to agree with every fact
already in the database: who the characters are, what the place looks
like, what the plot and arc beats before and after this scene expect,
and what prose (if any) already exists in nearby scenes. This skill
pulls all of that into one working-set dump before any prose is
written.

## When to run it

Every time — no exceptions — before:

- creating a scene's `--narrative-text` / `--dialogue-text` / `--action-text`
- updating any of those fields
- letting the agent generate prose via `POST /api/agent/chat`
- revising a scene a human has already written

If the user just asked "write scene 4", run this first, then write.

## Input

- `storyId` (JPA id or domain id — either works)
- `sceneId` for the target scene, OR `order=<n>` if identified by number

## Steps (all cheap — mostly reading existing data)

1. **Story-level spec**:
   ```bash
   ./story-cli story get <storyId> --json \
     | jq '{title, genre, subGenres, themes, premise, synopsis, description, targetAudience}'
   ```
   This is the voice contract. Every scene has to obey it.

2. **The target scene**:
   ```bash
   ./story-cli scene get <sceneId> --json \
     | jq '{title, summary, globalOrder, sceneType, emotionalTone,
           place: .place.id, viewpoint: .viewpointCharacter.id,
           characters: [.presentCharacters[]?.id],
           existingNarrative: (.narrativeText // null),
           existingDialogue: (.dialogueText // null),
           existingAction: (.actionText // null)}'
   ```

3. **Characters in this scene** — full profile for each:
   ```bash
   for cid in $(echo "viewpoint + characters from step 2"); do
     ./story-cli character get "$cid" --json \
       | jq '{name, narrativeRole, physicalDescription, personality, backstory, motivations, strengths, weaknesses}'
   done
   ```

4. **Place description**:
   ```bash
   ./story-cli place get <placeId> --json \
     | jq '{name, placeType, description, parent: .parentPlace.name}'
   ```

5. **Beats scheduled for this scene**:
   ```bash
   ./story-cli beat list --scene <sceneId> --story <storyId> --json \
     | jq 'sort_by(.order)[] | {order, beatType, importance, title, description,
         emotionalTone, pacingHint,
         advancesPlotId, plotProgressDelta,
         advancesArcId, arcProgressDelta}'
   ```
   The beats are the scene's job. The prose has to execute them.

6. **Neighbouring scenes** (one before + one after) — for tonal /
   narrative continuity:
   ```bash
   ./story-cli scene list --story <storyId> --json \
     | jq --argjson ord <n> 'sort_by(.globalOrder)[] |
        select(.globalOrder == ($ord - 1) or .globalOrder == ($ord + 1)) |
        {order: .globalOrder, title, summary,
         narrative: (.narrativeText[:500] // null)}'
   ```

7. **Plots this scene advances** — for each `advancesPlotId` in step 5:
   ```bash
   ./story-cli plot get <plotId> --json \
     | jq '{title, plotType, status, currentPhase, completionPercentage,
           centralConflict, objective, stakes}'
   ```

8. **Arcs this scene advances** — for each `advancesArcId` in step 5:
   ```bash
   ./story-cli arc get <arcId> --json \
     | jq '{title, arcType, status, completionPercentage, currentPhase,
           startingState, desiredEnding, internalConflict, growthTheme,
           character: .character.name}'
   ```

## Output

Produce a concise working-brief for Claude to consume as context when
actually writing the prose. Keep it under ~100 lines. Structure:

```
# Scene <n>: <title>

## Story voice
  Genre:    <genre> / <subGenres>
  Themes:   <themes>
  Premise:  <premise>
  Tone:     <one line distilled from synopsis + description>

## This scene
  Place:    <place name> — <one-line description>
  POV:      <character name> — <key personality trait>
  Present:  <names of other characters in scene>
  Summary:  <one-sentence description of what changes>

## Beats to hit, in order
  1. <beatType> — <title>: <description>
     advances: <plot>(+<delta>%) / <arc>(+<delta>%)
  2. ...

## Characters — quick profile
  <viewpoint>: <personality in one sentence> — motivated by <motivation>
  <other>:     <...>

## Place
  <type>, <one-sentence evocation of atmosphere>

## Continuity
  Previous scene ended with: <one-line from scene n-1 last paragraph>
  Next scene opens with:     <one-line from scene n+1 summary>

## Plot state
  <plot title>: <status> — <completion>% — objective: <objective>

## Arc state
  <character's arc>: <arcType> — <status> — <completion>%
  Turning from: <startingState>
  Turning toward: <desiredEnding>
```

## Then, and only then, write

Hand this brief to yourself as the working context, and only then
generate prose. The prose:
- must use the viewpoint character's POV and voice
- must deliver each beat in order
- must not invent facts that contradict the profiles or place
- must match the tone set by the story spec

If a beat can't be executed without facts the database doesn't carry
(e.g. a character's hair colour, a place's floor plan), decide now
and either invent consistent with the existing description OR pause
and ask the user.

## Related skills

- `se-write-story` — upstream; builds the structure this recalls
- `se-explore-story` — if you need a wider view than one scene
