---
name: llm-station-search
description: Search code using LLM Station's indexed codebase. Faster and more accurate than grep for understanding code structure, finding symbols, and exploring dependencies.
user-invocable: false
---

# LLM Station Code Search

Use LLM Station's daemon for code search. The daemon has already indexed this workspace with TreeSitter AST parsing and BM25 full-text search.

## Search by regex pattern
```bash
!`echo $LLMSTATION_BIN` run grep_search pattern="$ARGUMENTS" path=src/
```

## Search by file glob
```bash
!`echo $LLMSTATION_BIN` run glob_search pattern="*.hpp" path=src/include/
```

## Full-text search (BM25 ranked)
```bash
!`echo $LLMSTATION_BIN` run symbol_search query="$ARGUMENTS"
```

## Show workspace structure
```bash
!`echo $LLMSTATION_BIN` run workspace_tree path=src/ max_depth=3
```

**When to use:** Whenever you need to find code, understand what exists, or locate symbols. This is faster than reading files manually because the index is pre-built.
