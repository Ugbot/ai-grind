---
name: se-check-plot
description: Inspect a plot thread — status, completion, phase, conflict, stakes, progress scoring — and list the scenes that advance it. Use when the user asks "how is the main plot going", "check on subplot X", "what's the state of the mystery thread".
user_invocable: true
---

# Check a plot thread

Surface where a plot thread stands and what's moved it forward.

## Input

Either a plotId, a plot title, or a story (in which case list all plots
first and ask which one, or iterate each).

## Steps

1. **Resolve the plot**:
   ```bash
   ./story-cli plot list --story <storyId> --json \
     | jq -r '.[] | "\(.plotId[:8])  [\(.plotType)]  \(.title)  \(.status)  \(.completionPercentage)%"'
   ```
   If the user gave a title, find the matching plotId from the list.

2. **Full plot detail**:
   ```bash
   ./story-cli plot get <plotId> --json | jq .
   ```
   Surface: `plotType`, `status`, `currentPhase`, `completionPercentage`,
   `centralConflict`, `objective`, `stakes`, `incitingIncident`,
   `climax`, `resolution`, `protagonistId`, `antagonistId`.

3. **If the plot has a protagonist/antagonist id**, resolve names:
   ```bash
   ./story-cli character get <protagonistId> --json | jq -r '.name'
   ```

4. **Which scenes advance this plot?** The progress endpoint carries
   scene-plot advancement records, or call the scene-advancement
   endpoint directly. Today the easiest path is to list scene beats
   that reference the plot:
   ```bash
   ./story-cli progress <storyId> --json | jq '.subplotProgress'
   ```

## Output

```
[<plotType>]  <title>
  Status:   <status>       Phase: <currentPhase>
  Progress: <completionPercentage>%
  Conflict: <centralConflict>
  Stakes:   <stakes>
  Protagonist: <name>   Antagonist: <name>

  Key moments:
    Inciting: <incitingIncident or —>
    Climax:   <climax or —>
    Resolution: <resolution or —>

  Scenes that advance this plot:
    #<order>  <scene title>    (+<delta>%)
    ...
```

If the user asked about "all plots", repeat the block for each, or
compress to a one-line-per-plot summary and offer to dive in.

## Related skills

- `se-explore-story` — full story dashboard including plot summaries
- `se-character-trace` — what arcs a character is driving
- `se-progress-report` — the story-level progress roll-up
