---
name: se-explore-story
description: Deep dive into a single story — structure, characters, places, plots, arcs, and progress. Use when the user picks a story and says "tell me about X", "summarise", "what's in this story", or "give me the full picture".
user_invocable: true
---

# Explore a story

Produce a compact dashboard for one story.

## Input

The user supplies a story id, a title, or "the one I'm working on".
- If they give a title, resolve it via `./story-cli search --json "<title>" | jq '.results[] | select(.type=="story")'`, or run `se-list-stories` and pick by title match.
- Never guess. If you can't disambiguate, ask which story they mean.

## Steps (run in parallel where possible; each CLI call is ~5s of JVM boot)

1. **Basic info + statistics**:
   ```bash
   ./story-cli story get <id> --json
   ./story-cli story statistics <id> --json
   ```

2. **Structure tree** (plots, arcs, chapters, scenes):
   ```bash
   ./story-cli story structure <id> --json
   ```

3. **Characters**:
   ```bash
   ./story-cli character list --story <id> --json \
     | jq -r '.[] | "\(.narrativeRole // "—"):  \(.name)  \(.physicalDescription[:60] // "")"'
   ```

4. **Places**:
   ```bash
   ./story-cli place list --story <id> --json
   ```

5. **Plots + arcs**:
   ```bash
   ./story-cli plot list --story <id> --json
   ./story-cli arc list --story <id> --json
   ```

6. **Progress**:
   ```bash
   ./story-cli progress <id> --json
   ```

## Output

Present as a sectioned dashboard:

```
# <Title> — <Genre>

Premise: <premise>
Synopsis: <first 200 chars of synopsis or description>

## Progress
  Phase: <currentPhase>     Complete: <completionPercentage>%
  Words: <currentWordCount> / <targetWordCount or —>
  Open plot threads: <count>     Arcs needing attention: <count>

## Cast
  PROTAGONIST:  <name> — <one-line description>
  ANTAGONIST:   <name> — <one-line description>
  MENTOR:       <name> — <one-line description>
  ...

## Places (<count>)
  <name>  (<type>)  — <one-line description>

## Plot threads (<count>)
  [MAIN_PLOT]  <title>  (<status>, <completion>%)
  [SUBPLOT]    <title>  (<status>)

## Character arcs (<count>)
  <arcType>  <character name>: <title>  (<status>)

## Scenes (<count>, <total words> words)
  #1  <title>   (<status>, <wordCount>w)
  #2  <title>   ...
```

Keep it under ~40 lines. If a section is empty, show `(none)` rather
than omitting it — absence is information too.

## Related skills

- `se-character-trace` — zoom into one character
- `se-check-plot` — zoom into one plot thread
- `se-play-story` — read scene prose
- `se-progress-report` — just the progress block
