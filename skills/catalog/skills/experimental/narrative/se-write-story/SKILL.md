---
name: se-write-story
description: Create a new story end-to-end via the CLI — structure first, prose last. World → plot → cast → arcs → beats → prose, in that order. Use when the user asks "write me a story", "build a new story about X", "let's create a novella".
user_invocable: true
---

# Write a story via CLI

Drive the full create-story flow from concept through first prose. The
user can do the same thing in the `/stories/new` wizard — this is the
CLI-native equivalent.

## The rule

**Structure first. Prose last.** Build the world, the plot, the cast,
and the arcs *before* writing any narrative text. The scaffolding is
what keeps the prose honest — without it you'll drift, rename things,
forget which scene Lady Catherine is in, and end up with six
inconsistent paragraphs instead of a short story.

The order, in every case, is:

1. **Concept** — genre, tone, length, themes, setting, conflict, title.
2. **Story** entity — `story create`, record the `id` and `storyId`.
3. **World** — `place create` for every location a scene could happen
   in, plus any off-screen place the story references. Build hierarchy
   via `--parent`. Don't skimp; descriptions cost nothing and pay off
   when you're writing a scene six days later and can't remember what
   the tavern looked like.
4. **Cast** — `character create` for every named character, including
   the ones who only appear in one scene. Fill in `--personality`,
   `--backstory`, `--motivations`. A character with one line in scene
   4 gets two sentences of backstory now so that when you write the
   scene you know what colour their gloves are.
5. **Items** — `item create` for props the plot turns on. A letter, a
   weapon, a locket. Attach owner/location so the graph stays tight.
6. **Plots** — `plot create` for the main plot and each subplot.
   Fill in `--central-conflict`, `--stakes`, `--objective`. Link a
   `--protagonist` and `--antagonist` character id to each.
7. **Arcs** — `arc create` per character that has an internal change.
   Pick an `--type` from the ArcType enum. State the starting and
   desired-ending states.
8. **Chapters + scenes** — create each scene with title, summary,
   `--order`, `--place`, `--viewpoint`, `--characters`. Leave
   `--narrative-text` **unset** for now, or set it to a one-paragraph
   placeholder. This is the scene list — the outline, not the prose.
9. **Beats** — `beat create` per scene. Beat types describe the scene's
   job (HOOK, SETUP, ESCALATION, REVEAL, DECISION, TURNING_POINT,
   CLIMAX, RESOLUTION). Link beats to plots via `--advances-plot` and
   to arcs via `--advances-arc` with `--plot-delta` / `--arc-delta`
   so progress auto-rolls up.
10. **Prose, finally** — go back through each scene and fill in
    `--narrative-text`, `--dialogue-text`, `--action-text`.
    **Before writing each scene's prose**, invoke the
    `se-scene-context` skill for that scene. It pulls the story voice
    spec, the target scene's beats, the characters' profiles, the
    place description, and the neighbouring scenes into one working
    brief. Only then write. `./story-cli scene update <sceneId>
    --narrative-text="..."`.
11. **Refresh word counts**:
    `curl -s -X POST http://localhost:9876/api/stories/<id>/update-word-count`
12. **Hand off** to the UI: tell the user to open
    `http://localhost:3002/stories/<jpaId>` to review.

## Concept questions (phase 1)

Ask (or decide with the user), in this order:

- Genre + any subgenres
- Tone (a sentence, not a word; "sardonic, gallows humour" not "dark")
- Length — short story / novella / novel
- Themes (2–3)
- Setting — where, when, at what social scale
- Central conflict
- Title — derive from the setting if the user doesn't care

Write these down in a comment block before you invoke the CLI. They
are the spec the whole rest of the flow has to obey.

## Command cheat-sheet

```bash
# story
./story-cli story create --json --title="..." --genre="..." \
  --description="..." --premise="..." --synopsis="..."

# place (repeat per location)
./story-cli place create --story $SID --name="..." --type=<PlaceType> \
  --description="..." [--parent=<placeId>]

# character (repeat per named character)
./story-cli character create --story $SID --name="..." --role=<CharacterRole> \
  --physical-description="..." --personality="..." \
  --backstory="..." --motivations="..." [--is-main-character]

# item
./story-cli item create --story $SID --name="..." --type="..." \
  --description="..." [--owner=<charId>] [--location=<placeId>]

# plot (one main + subplots)
./story-cli plot create --story $SID --title="..." --type=<PlotType> \
  --description="..." --central-conflict="..." --stakes="..." \
  --objective="..." [--is-main-plot] \
  [--protagonist=<charId>] [--antagonist=<charId>]

# arc (per character who grows)
./story-cli arc create --story $SID --character=<charId> \
  --title="..." --type=<ArcType> --description="..."

# scene (SUMMARY ONLY, no prose yet)
./story-cli scene create --story $SID --title="..." --order=<n> \
  --summary="..." --place=<placeId> --viewpoint=<charId> \
  --characters=<c1>,<c2>

# beat (per scene, 2-4 beats typical)
./story-cli beat create --story $SID --scene=<sceneId> \
  --type=<BeatType> --importance=HIGH \
  --title="..." --description="..." \
  [--advances-plot=<plotId> --plot-delta=0.15] \
  [--advances-arc=<arcId>  --arc-delta=0.10]

# prose (LAST)
./story-cli scene update <sceneId> --narrative-text="..." \
  --dialogue-text="..." --action-text="..."
```

Flag names to remember (bitten by these): `--central-conflict` not
`--conflict`; `--narrative-text` not `--narrative`; `--is-main-plot`
not `--main`.

## Ordering discipline — the "why"

- **World before people**: places are where characters do things. If
  you create characters first, their backstories will reference places
  that don't exist yet and you'll end up retrofitting.
- **People before plots**: plots reference characters as protagonist
  and antagonist. The plot is empty without them.
- **Plots before arcs**: an arc is a character's transformation across
  a plot. The plot's beats are the arc's turning points.
- **Arcs before scenes**: scenes land beats, beats land arcs, arcs
  land plots. A scene written before you know what arc it's advancing
  is a scene that will get cut.
- **Scenes before beats**: beats live inside scenes. This one is
  mechanical.
- **Beats before prose**: beats are the scene's job. Prose is the
  scene's *execution* of that job. Write the prose last so the prose
  actually executes the beat instead of drifting into a different
  one.

## Rules

- Never use placeholder prose like "Lorem ipsum" or "TBD". If you're
  writing, write real prose. If the user is writing, leave narrative
  fields unset and tell them what scene needs filling.
- Capture every id the CLI returns. `tee`/jq into a scratch file is
  fine; just don't lose them.
- For novellas and novels, do not try to write every scene in one
  session. Scaffold everything through step 9 (beats) in one pass,
  then fill in prose a scene at a time over multiple sessions.
- If you're tempted to skip world-building "because there's only one
  location", you're wrong. Create at least three places: the main
  setting, the place the protagonist is from, and somewhere off-stage
  the antagonist operates from. The graph tells you things later.

## After creation

End with `se-explore-story <id>` or
`./story-cli story structure <id> --json` so the user can see the
finished shape. Then tell them to open
`http://localhost:3002/stories/<jpaId>` in the editor.

## Related skills

- `se-explore-story` — verify after creation
- `se-play-story` — read it back when the prose lands
- `se-progress-report` — check pacing guidance as you go
