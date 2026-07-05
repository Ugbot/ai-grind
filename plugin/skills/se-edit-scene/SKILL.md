---
name: se-edit-scene
description: Revise an existing scene's prose or metadata. Use when the user asks "rewrite scene X", "fix the dialogue in Y", "update the summary for Z", or wants to replace narrativeText / dialogueText / actionText on an existing scene.
user_invocable: true
---

# Edit an existing scene

Replace fields on a scene that already exists. Pair with `se-scene-context`
— always recall context before writing new prose.

## Input

- `storyId` (JPA id or domain id — either works)
- `sceneId` — either form accepted; the CLI resolves it via
  `Scene.findByAnyId`
- The kind of edit: which field(s) to update

## Steps

1. **Always recall first**. Invoke `se-scene-context` for the target
   scene. Even for a "just change one word" edit, the recall prevents
   tonal drift. For a rewrite, the recall is non-negotiable.

2. **Write the new prose into a temp file** so you can feed it back
   through the CLI without heredoc escaping headaches:

   ```bash
   cat > /tmp/scene-edit.txt <<'EOF'
   <paste the new prose here>
   EOF
   ```

3. **Apply the edit**. The CLI's `scene update` accepts any subset of
   fields — untouched fields are preserved.

   ```bash
   ./story-cli scene update <sceneId> \
     --narrative-text="$(cat /tmp/scene-edit.txt)"
   ```

   Other updatable fields:
   - `--title="..."`
   - `--summary="..."`
   - `--dialogue-text="..."`
   - `--action-text="..."`
   - `--place=<placeId>`          (move the scene)
   - `--viewpoint=<characterId>`  (change POV)
   - `--order=<n>`                (reorder)
   - `--status=<SceneStatus>`     (OUTLINE / DRAFT / REVISION / FINAL)
   - `--scene-type=<SceneType>`
   - `--emotional-tone=<EmotionalTone>`

4. **Word counts refresh automatically.** Scene has a
   `@PrePersist/@PreUpdate` hook that recounts the three prose fields
   on every save, so you don't need to call `/update-word-count`
   afterwards for the scene itself. The story-level rollup still needs
   the explicit refresh (see below).

5. **Refresh the story total if you want the dashboard to show
   accurate overall word counts**:
   ```bash
   curl -s -X POST http://localhost:9876/api/stories/<storyId>/update-word-count
   ```
   Scene-level counts are always fresh; story-level aggregation isn't
   automatic because it'd thrash the story row every time a scene is
   updated.

6. **Verify**:
   ```bash
   ./story-cli scene get <sceneId> --json \
     | jq '{title, wordCount, narrativeLen: (.narrativeText|length), updatedAt}'
   ```

## When the user wants a partial edit

"Fix the typo in scene 2" — read the current narrative, patch the
specific lines, write back the whole field. Don't invent diff syntax;
`--narrative-text` is a full replacement. If the user wants true
line-level edits, read the text, produce a revised version, and write
the whole thing back.

## When the user wants a tonal rewrite

"Make scene 3 more Gideon-like." Steps:

1. `se-scene-context` for scene 3.
2. Pull the story's `description` + `synopsis` for the voice contract.
3. Read the existing narrative.
4. Rewrite the prose preserving every beat in order. Do not invent
   new beats or drop existing ones. If a beat can't survive the tonal
   change, pause and ask the user.
5. `./story-cli scene update` with the new `--narrative-text`.

## Related skills

- `se-scene-context` — MANDATORY preamble before any prose edit
- `se-write-story` — upstream scaffolding skill
- `se-play-story` — read the story back after edits to catch drift
