---
name: write-story
description: Write a complete story using the Story Engine MCP tools. Creates story structure (characters, places, plots, arcs, chapters, scenes, beats) and generates prose.
user_invocable: true
---

# Write Story via Story Engine MCP

You are a creative writing assistant using the Story Engine MCP server to build stories with full narrative structure.

## Prerequisites

The Story Engine backend must be running (`./start.sh backend` or `./start.sh`).
The MCP STDIO server must be configured in `.mcp.json`.

## Workflow

When the user asks to write a story, follow this process:

### Phase 1: Concept
1. Ask the user for a **genre**, **premise/logline**, and **tone** (or suggest options)
2. Optionally use `generate(type=premise, genre=...)` to brainstorm premises
3. Create the story: `story(action=create, title=..., genre=..., synopsis=...)`
4. Save the returned story ID -- you'll need it for everything

### Phase 2: World Building
1. Create **places** with hierarchy:
   - Top-level location (city, kingdom, ship)
   - Sub-locations (districts, rooms, floors) using `parentPlaceId`
   - Use `place(action=create, storyId=..., name=..., description=..., type=..., parentPlaceId=...)`
2. Create **items** if relevant:
   - `item(action=create, storyId=..., name=..., description=..., type=..., locationId=...)`

### Phase 3: Characters
1. Create the **protagonist** first:
   - `character(action=create, storyId=..., name=..., role=PROTAGONIST, physicalDescription=..., personality=..., backstory=..., motivations=..., isMainCharacter=true)`
2. Create the **antagonist** and supporting cast
3. Optionally use `generate(type=character, ...)` for AI-generated character concepts

### Phase 4: Plot Structure
1. Create the **main plot**:
   - `plot(action=create, storyId=..., title=..., plotType=MAIN_PLOT, centralConflict=..., stakes=..., isMainPlot=true, protagonistId=..., antagonistId=...)`
2. Create **subplots** (romance, mystery, personal growth, etc.)
3. Create **character arcs**:
   - `arc(action=create, storyId=..., characterId=..., title=..., arcType=HERO_JOURNEY, startingState=..., desiredEnding=..., internalConflict=..., growthTheme=...)`

### Phase 5: Chapter & Scene Outline
1. Create **chapters**: `chapter(action=create, storyId=..., title=..., chapterNumber=...)`
2. Create **scenes** within each chapter:
   - `scene(action=create, storyId=..., title=..., summary=..., placeId=..., characterIds=..., viewpointCharacterId=..., order=...)`
   - `chapter(action=addScene, id=<chapterId>, sceneId=<sceneId>)`
3. Define **beats** for each scene:
   - `progress(action=createBeat, sceneId=..., storyId=..., beatType=HOOK, importance=SIGNIFICANT, title=..., description=...)`
   - Common beat flow: HOOK -> INCITING_INCIDENT -> ESCALATION -> MIDPOINT -> CRISIS -> CLIMAX -> RESOLUTION

### Phase 6: Write Prose
For each scene:
1. Get writing guidance: `progress(action=guidance, sceneId=..., storyId=...)`
2. Generate or write the prose
3. Update the scene: `scene(action=update, id=..., narrativeText=..., dialogueText=...)`
4. Optionally generate dialogue: `generate(type=dialogue, characters=..., premise=..., tone=..., objective=...)`

### Phase 7: Review & Polish
1. Check progress: `progress(action=progress, storyId=...)`
2. View structure: `story(action=structure, id=...)`
3. Get statistics: `story(action=statistics, id=...)`
4. Analyze content: `analyze(action=analyze, content=..., analysisTypes=pacing,character,plot)`
5. Improve weak sections: `analyze(action=improve, content=..., improvementType=enhance, focusAreas=dialogue,emotion)`

## Tips

- **Always save IDs** returned from create operations -- you need them for linking
- **Use viewpointCharacterId** on scenes to establish POV
- **Link scenes to plots/arcs** with `progress(action=linkPlot, ...)` and `progress(action=linkArc, ...)` to track how the story advances
- **Beat types matter**: they drive the prose guidance system. Use HOOK for chapter openers, CLIMAX for turning points, RESOLUTION for endings
- **Check progress regularly** to get AI-guided recommendations on pacing, open threads, and arcs needing attention
- For **short stories**, you may skip chapters and just use scenes directly
- For **novels**, create a full chapter outline first, then fill in scenes
