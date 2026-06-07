---
name: se-list-stories
description: List every story in the Story Engine library with title, genre, word count, and id. Use when the user asks "what stories do I have", "show me my stories", "list stories", or needs a story id to feed into another skill.
user_invocable: true
---

# List stories

Surface every story in the library as a short table.

## Steps

1. If the backend isn't healthy, invoke `start-engine` first.

2. Fetch the list:
   ```bash
   ./story-cli story list --json \
     | jq -r '.[] | "\(.storyId[:8])  \(.title)  (\(.genre // "—"))  scenes=\(.sceneCount // 0)  words=\(.wordCount // 0)"'
   ```

3. If the user asked to filter, append flags to the CLI call:
   - by genre: `--genre="Literary Fiction"`
   - by status: `--status=DRAFT` (or `PUBLISHED`, `ARCHIVED`)

4. Present as a table. Include the short id so the user can copy it.
   When they pick one, remember the full id — you'll need it for
   follow-on skills.

## Example output

```
6994408a  The Last Lighthouse            (Science Fiction)  scenes=2   words=19
aecdba5a  The Cartographer's Daughter    (Literary Fiction) scenes=3   words=631
dfcdafa4  The Honest Clock               (Fantasy)          scenes=3   words=676
e792d327  Dracula                        (Gothic Horror)    scenes=8   words=0
```

## Related skills

- `se-explore-story` — dive into one of these
- `se-progress-report` — progress dashboard for one
- `se-play-story` — read a story scene-by-scene
