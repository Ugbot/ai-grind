---
name: se-enhance-prose
description: Analyse prose style (metrics + closest-register match) and optionally rewrite toward a target style or enhancement axis. Use when the user asks "is this scene Gideon-enough", "tighten this passage", "punch up the rhythm", "what register is this in", "rewrite scene X in Hemingway".
user_invocable: true
---

# Analyse and enhance prose

The service does two things:

- **Analyse** — pure Java, no LLM call. Returns sentence-length stats,
  adverb density, passive-voice markers, dialogue share, subordination
  ratio, style-fit scores against 6 named registers (Hemingway, Gothic,
  Tolkien, Minimalist, McCarthy, Muir), plus actionable suggestions.
- **Enhance** — LLM-backed rewrite through the configured AI provider.
  The prompt includes the diagnostic, the target register's voice
  brief, and the chosen enhancement axis. Returns the new prose plus
  short notes on what changed.

## When to use each

| User asks | Run |
|---|---|
| "is this scene Gideon-enough?" | analyse with `--target=MUIR` |
| "what style is this in?" | analyse without `--target`; read `closestStyle` |
| "what's wrong with the pacing here?" | analyse; look at the sentence-length stdev + suggestions |
| "tighten this passage" | enhance with `--type=TIGHTEN` |
| "rewrite scene 4 in Hemingway" | enhance `--scene=<id> --type=STYLE_MATCH --target=HEMINGWAY --write` |
| "add more atmosphere to scene 2" | enhance `--type=ATMOSPHERE --target=<matching style>` |

## Analyse

```bash
# A scene's combined prose, compared to a target
./story-cli prose analyse --scene=<sceneId> --target=MUIR

# An ad-hoc passage from a file
./story-cli prose analyse --file=/tmp/draft.txt --target=HEMINGWAY

# Inline (quote it)
./story-cli prose analyse --target=GOTHIC "He was at the desk. The lamp was still on."
```

Output shape (human mode):
- word / sentence counts and per-sentence stats
- adverb / filler / passive / dialogue ratios
- closest register match
- style-fit table with bar chart per register
- suggestion bullets (adverb-heavy, monotonous rhythm, target-mismatch, …)

For programmatic use, add `--json` before the subcommand:
`./story-cli --json prose analyse --scene=<id>`.

## Enhance

```bash
# In-memory rewrite: don't touch the scene
./story-cli prose enhance --scene=<id> --type=RHYTHM --target=MUIR

# Rewrite AND persist onto scene.narrativeText
./story-cli prose enhance --scene=<id> --type=TIGHTEN --write

# Rewrite an arbitrary passage
./story-cli prose enhance --file=/tmp/draft.txt --target=MCCARTHY --type=STYLE_MATCH
```

### EnhancementType

- `WORD_CHOICE` — replace flat words with sharper register-appropriate ones
- `RHYTHM` — adjust sentence-length variance toward the target
- `PACING` — punch up action, slow down reflection
- `ATMOSPHERE` — foreground sensory detail
- `CHARACTER_VOICE` — make POV more distinctive / consistent
- `TIGHTEN` — strip filler, adverbs, passive constructions
- `EXPAND` — develop a sparse beat
- `STYLE_MATCH` — holistic push toward the target register

### ProseStyle

`NEUTRAL`, `HEMINGWAY`, `GOTHIC`, `TOLKIEN`, `MINIMALIST`, `MCCARTHY`,
`MUIR`, `CUSTOM`.

## Recommended workflow for a scene edit

1. `se-scene-context` — get the structural context.
2. `./story-cli prose analyse --scene=<id> --target=<story's style>` —
   diagnose.
3. Decide which suggestion matters most; pick an `EnhancementType`.
4. Run `prose enhance --scene=<id>` without `--write` first; review.
5. If acceptable, re-run with `--write`, OR use `se-edit-scene` to
   hand-craft the rewrite guided by the analysis.

## Warnings to surface

- If `adverb density > 4%`: the prose is adverb-heavy regardless of
  register.
- If `sentence stdev < 3` with more than 4 sentences: runs of same-length
  sentences; break one up.
- If the closest register differs from the story-wide target
  (e.g., story aims MUIR but scene reads MCCARTHY), surface that to the
  user — it may be intentional, or it may be drift.
- If `fillerRatio > 1.5%`: very/really/just/actually density is high.

## REST equivalents

- `POST /api/prose/analyse`                         `{prose, targetStyle}`
- `POST /api/prose/enhance`                         `{prose, type, targetStyle}`
- `GET  /api/prose/scenes/{sceneId}/analyse?targetStyle=MUIR`
- `POST /api/prose/scenes/{sceneId}/enhance`        `{type, targetStyle}` (persists)

## Related skills

- `se-analyse-story` — narrative-shape analysis (different level: tension / pacing / arcs)
- `se-scene-context` — MANDATORY context recall before any rewrite
- `se-edit-scene` — hand-written replacement path when you don't trust the LLM rewrite
