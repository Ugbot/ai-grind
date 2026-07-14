---
name: se-character-trace
description: Trace a character through a story — their scenes, arcs, relationships, and where they appear. Use when the user asks "tell me about X", "what does character Y do", "show me Jonathan's arc", "where does Mina appear".
user_invocable: true
---

# Trace a character

Build a short biography + map of where the character shows up.

## Input

A character name or id. Optionally scoped to a story; otherwise ask
the user to pick when the name is ambiguous.

## Steps

1. **Resolve the character id** if the user gave a name:
   ```bash
   ./story-cli search --json --limit 20 "<name>" \
     | jq '.results[] | select(.type=="character")'
   ```
   If multiple hits, show them with the parent story title and ask
   which one.

2. **Profile**:
   ```bash
   ./story-cli character get <id> --json | jq .
   ```
   Surface: `name`, `narrativeRole`, `physicalDescription`, `personality`,
   `backstory`, `motivations`, `strengths`, `weaknesses`,
   `isMainCharacter`.

3. **Arcs** for this character (list story arcs, filter by characterId):
   ```bash
   ./story-cli arc list --story <storyId> --json \
     | jq --arg cid "<id>" '.[] | select(.character.id == $cid or .character.characterId == $cid)'
   ```
   If empty, say "no recorded arcs".

4. **Scenes** where the character appears. Today the cleanest path is
   to list scenes and filter by the present-characters list:
   ```bash
   ./story-cli scene list --story <storyId> --json \
     | jq --arg cid "<id>" '
         .[] | select(
           any(.presentCharacters[]?; .id == $cid or .characterId == $cid)
           or (.viewpointCharacter.id == $cid)
         ) | {order:.globalOrder, title, wordCount, viewpoint: (.viewpointCharacter.id == $cid)}
       '
   ```

5. **Relationships** (if your story has any recorded):
   ```bash
   ./story-cli --json story structure <storyId> \
     | jq '.relationships[]? | select(.sourceCharacterId == "<id>" or .targetCharacterId == "<id>")'
   ```

## Output

```
<Name>  (<narrativeRole>)   — <Story>
  <physicalDescription>
  Personality: <personality>
  Backstory:   <backstory>
  Motivations: <motivations>

## Arcs (<n>)
  <arcType>  <title>  — <status>, <completion>%
    start → turning point → resolution

## Appearances (<n> scenes)
  #<order>  <scene title>   (VP, <wordCount>w)
  ...

## Relationships (<n>)
  <relationshipType>  <other character>  — <note>
```

## Related skills

- `se-check-plot` — if the character is a plot protagonist
- `se-play-story` — to read the scenes they appear in in order
- `se-explore-story` — whole-story dashboard
