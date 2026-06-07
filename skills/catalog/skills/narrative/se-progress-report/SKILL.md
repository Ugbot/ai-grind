---
name: se-progress-report
description: Show a story's progress dashboard — phase, completion %, open plot threads, arcs needing attention, word counts. Use when the user asks "how's the story going", "where am I in X", "what's the status".
user_invocable: true
---

# Story progress report

Compact dashboard for "am I on track?" questions.

## Input

A story id or title.

## Steps

1. **Pull progress**:
   ```bash
   ./story-cli progress <id> --json
   ```

2. **Render a short block**. Not all fields are always populated —
   fall back gracefully.

   Important fields from the response:
   - `currentPhase` (OPENING / INCITING_INCIDENT / RISING_ACTION / MIDPOINT / PRE_CLIMAX / CLIMAX / FALLING_ACTION / RESOLUTION)
   - `completionPercentage` (0–100)
   - `phaseGuidance` (advice for this phase)
   - `paceRecommendation`
   - `currentWordCount`, `targetWordCount`, `wordCountProgress`
   - `openPlotThreads[]`, `completedPlotThreads[]`, `plotThreadsToClose[]`
   - `activeArcCount`, `arcsNeedingAttention[]`
   - `criticalBeatsRemaining`
   - `shouldOpenNewPlots`, `shouldClosePlots`

3. **Output format** (keep under 20 lines):

```
<Title>
  Phase: <currentPhase>   (<completionPercentage>% complete)
  Words: <currentWordCount> / <targetWordCount or "no target">
  Scenes: <scenesWritten or ?> / <totalScenes>

  Guidance: <phaseGuidance>
  Pacing:   <paceRecommendation>

  Plot threads:
    Open (<n>):      <title>, <title>, ...
    Close these (<n>): <title>, ...
    Completed (<n>): <title>, ...

  Character arcs:
    Active: <n>
    Needs attention: <title>, ...

  Critical beats remaining: <n>
```

4. **If the user is mid-writing**, highlight:
   - `shouldOpenNewPlots=true` → "this is a good moment to seed a new
     thread"
   - `shouldClosePlots=true` + `plotThreadsToClose` not empty →
     "consider resolving: …"
   - `arcsNeedingAttention` not empty → "these arcs are lagging: …"

## Related skills

- `se-explore-story` — full dashboard (this block + cast + plots + arcs)
- `se-check-plot` — drill into a specific thread that's flagged
