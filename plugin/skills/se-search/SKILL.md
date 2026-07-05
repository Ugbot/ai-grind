---
name: se-search
description: Full-text search across every story's characters, scenes, places, and items. Use when the user asks "find", "where is", "search for", "look up", or names a character/place/scene without an id.
user_invocable: true
---

# Search the story world

Disambiguate a name or phrase into concrete entities.

## Input

A query string. Optionally scope to one story.

## Steps

1. **Search**:
   ```bash
   ./story-cli search --json "<query>"                       # whole library
   ./story-cli search --json --story <storyId> "<query>"    # one story
   ./story-cli search --json --limit 20 "<query>"           # more hits
   ```

2. **Group results by type** (story / character / scene / place / item)
   and render each with score + story context:
   ```bash
   ./story-cli search --json "<query>" | jq -r '
     .results
     | group_by(.type)[]
     | "\n\(.[0].type | ascii_upcase):",
       (sort_by(-.score)[] | "  \(.title)  (\(.metadata.storyTitle // "—"))  score=\(.score)  id=\(.id[:8])")
   '
   ```

3. **If the user wanted one specific thing** (e.g. "find the character
   Calla"), filter the result set before rendering:
   ```bash
   ./story-cli search --json "Calla" | jq '.results[] | select(.type=="character")'
   ```

4. **Follow up**. Once an entity is identified, chain into the right
   skill:
   - character hit → `se-character-trace`
   - story hit → `se-explore-story`
   - scene hit → `./story-cli scene get <sceneId> --json` and render prose
   - place hit → `./story-cli place get <placeId> --json`

## Notes

- Search is substring-matching on title + description today (no vector
  search). Single-word queries work best. Quoting a multi-word query
  matches on either token.
- Drafts are included; no auth yet, so "your library" is "every story".
- If the user is looking for content *inside* a scene's prose (not in
  title or summary), the search service won't currently index the
  `narrativeText` body — note the limitation and offer to grep the
  scene's full `narrativeText` field via `./story-cli scene get` + `jq`.

## Related skills

- `se-character-trace` — after finding a character
- `se-explore-story` — after finding a story
