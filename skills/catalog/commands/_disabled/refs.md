Find all references to a symbol across the llm-station codebase.

The symbol to look up is: $ARGUMENTS

Run:
```bash
timeout 30 llm-station --run find_references "symbol=$ARGUMENTS" "context_lines=2" 2>&1
```

Then also check the call graph for function callers:
```bash
timeout 30 llm-station --run call_hierarchy "function=$ARGUMENTS" "direction=callers" 2>&1
```

Report:
- Where the symbol is defined
- All call sites with file:line locations
- Which files would need to change if this symbol were renamed or removed

Note: Quote each `key=value` param as a single shell argument.
