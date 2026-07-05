---
name: llm-station-debug
description: Debug and profile code using LLM Station's devtools integration. Run Valgrind memcheck, perf profiling, or check available debugging tools.
user-invocable: false
---

# LLM Station Debugging & Profiling

Use LLM Station's integrated devtools for debugging and performance analysis.

## Check available debugging tools
```bash
!`echo $LLMSTATION_BIN` run devtools_check
```

## Run Valgrind memcheck
```bash
!`echo $LLMSTATION_BIN` run devtools_run suite=valgrind tool=memcheck binary=$ARGUMENTS
```

## Run perf stat
```bash
!`echo $LLMSTATION_BIN` run devtools_run suite=perf tool=stat binary=$ARGUMENTS
```

## Analyze a previous run
```bash
!`echo $LLMSTATION_BIN` run devtools_analyze run_id=$ARGUMENTS
```

## List previous runs
```bash
!`echo $LLMSTATION_BIN` run devtools_list
```

## Search across all debugging results
```bash
!`echo $LLMSTATION_BIN` run devtools_search query="$ARGUMENTS"
```

**When to use:** After code compiles but has runtime issues. Use memcheck for memory errors, perf for performance hotspots.
