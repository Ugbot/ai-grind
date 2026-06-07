---
name: story-status
description: Check the status of stories in the Story Engine -- list stories, view structure, progress, and statistics.
user_invocable: true
---

# Story Status Check

Use the Story Engine MCP tools to report on existing stories.

## Steps

1. **List all stories**: `story(action=list)`
2. For each story the user is interested in:
   - **Structure overview**: `story(action=structure, id=<storyId>)` -- shows plots, arcs, chapters, scenes
   - **Statistics**: `story(action=statistics, id=<storyId>)` -- word count, character count, scene count
   - **Progress**: `progress(action=progress, storyId=<storyId>)` -- completion %, phase, AI guidance
   - **Relationship web**: `relationships(action=web, storyId=<storyId>)` -- character connections
   - **Place hierarchy**: `graph(action=placeHierarchy, storyId=<storyId>)` -- location tree

## Output Format

Present results as a concise dashboard:
- Story title, genre, status
- Completion percentage and current phase
- Chapter/scene/word counts
- Any AI guidance (pacing, threads to close, arcs needing attention)
