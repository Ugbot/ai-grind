Search the llm-station codebase using the running daemon's indexed tools.

The search query is: $ARGUMENTS

Run ALL of these in parallel and report the combined results:

1. Symbol search (functions, classes, variables by name):
```bash
timeout 20 llm-station --run symbol_search "query=$ARGUMENTS" --workspace /Users/bengamble/llm-station 2>&1 | head -60
```

2. Code search (semantic + BM25 ranked):
```bash
timeout 20 llm-station --run code_search "query=$ARGUMENTS" --workspace /Users/bengamble/llm-station 2>&1 | head -40
```

3. Grep search (exact pattern in source files):
```bash
timeout 20 llm-station --run grep_search "pattern=$ARGUMENTS" "context_lines=2" --workspace /Users/bengamble/llm-station 2>&1 | head -60
```

Synthesize the results: deduplicate, highlight the most relevant hits, and tell me what files and functions are most relevant to the query.

Note: Always quote each `key=value` param as a single shell argument to prevent glob expansion.
The daemon workspace is `/Users/bengamble/llm-station`.
