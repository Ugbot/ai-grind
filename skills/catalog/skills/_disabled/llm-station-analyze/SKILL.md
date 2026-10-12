---
name: llm-station-analyze
description: Analyze code structure using LLM Station's call graph, predicate search, and task decomposition. Use when understanding function relationships, impact of changes, or planning implementation order.
user-invocable: false
---

# LLM Station Code Analysis

Use LLM Station's code intelligence to understand code structure without reading every file.

## Explore call graph — what does a function call?
```bash
!`echo $LLMSTATION_BIN` run call_graph function="$ARGUMENTS" action=callees
```

## Explore call graph — what calls a function?
```bash
!`echo $LLMSTATION_BIN` run call_graph function="$ARGUMENTS" action=callers
```

## Find functions by structural criteria
```bash
!`echo $LLMSTATION_BIN` run predicate_search return_type="bool" file_pattern="paimon"
!`echo $LLMSTATION_BIN` run predicate_search calls="FileSystem" scope_pattern="Paimon"
```
Find functions matching patterns: return type, parameter types, what they call, who calls them, file location, scope/namespace.

## Decompose a function into implementation tasks
```bash
!`echo $LLMSTATION_BIN` run task_decompose function="TargetFunction"
```
Shows dependency-ordered tasks needed to implement a function. Uses GOAP planning on the call graph.

**When to use:** Before making changes, understand the impact. Use call_graph to see what else will be affected. Use predicate_search to find similar patterns in the codebase to follow.

*Note: the first call for a workspace auto-starts the daemon if not already running (a few seconds); subsequent calls are fast.*
