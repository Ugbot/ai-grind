---
name: se-play-story
description: Read a story's scenes in order — narrative, dialogue, and action text. Use when the user asks "read me this story", "play through X", "walk me through the scenes", or wants to consume the prose end-to-end.
user_invocable: true
---

# Play a story scene by scene

Present a story's prose as a paced read-through. This is the Claude-
Code analogue of opening the `/stories/{id}/vn` tab in the UI.

## Input

A story id or title. If only a title, resolve via
`./story-cli search --json "<title>" | jq '.results[] | select(.type=="story")'`.

## Steps

1. **List scenes in order**:
   ```bash
   ./story-cli scene list --story <id> --json \
     | jq -r 'sort_by(.globalOrder // 9999)[] | "\(.globalOrder // "?")  \(.sceneId)  \(.title)  (\(.wordCount)w)"'
   ```

2. **Ask the user** whether they want:
   - the whole story back to back
   - one scene at a time, waiting for "next" between each
   - just summaries (skip prose)

3. **For each scene**, fetch its detail and render:
   ```bash
   ./story-cli scene get <sceneId> --json \
     | jq -r '
         "\n=== #\(.globalOrder // "?")  \(.title) ===",
         (if .summary then "\n[\(.summary)]" else empty end),
         (if .narrativeText then "\n\(.narrativeText)" else empty end),
         (if .dialogueText then "\n\n— Dialogue —\n\(.dialogueText)" else empty end),
         (if .actionText then "\n\n— Action —\n\(.actionText)" else empty end)
       '
   ```

4. **If a scene has no prose**, say so (`"(no prose for this scene yet)"`)
   and offer to skip or generate. Don't pretend empty scenes have content.

5. **Between scenes** (if paced), show a compact transition:
   ```
   — end of scene 1 of 3 — say "next" to continue, or "skip to N"
   ```

## When the story has branches

`./story-cli story get <id> --json | jq .parentStoryId`. If non-null,
this is a branch; mention that and offer to show the parent story.
`./story-cli --json story structure <parentStoryId>` lists siblings.

## Related skills

- `se-explore-story` — skeleton view before diving in
- `se-character-trace` — follow one character across the scenes
