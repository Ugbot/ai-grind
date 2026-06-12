Show the call graph for a function in the llm-station codebase.

The function to inspect is: $ARGUMENTS

Run both directions:

1. What does this function call (callees):
```bash
timeout 30 llm-station --run call_graph "function=$ARGUMENTS" "direction=callees" "depth=2" 2>&1
```

2. What calls this function (callers):
```bash
timeout 30 llm-station --run call_graph "function=$ARGUMENTS" "direction=callers" "depth=2" 2>&1
```

3. Immediate callers/callees with file locations:
```bash
timeout 30 llm-station --run call_hierarchy "function=$ARGUMENTS" "direction=both" 2>&1
```

Report a clear picture of: what this function depends on, and what depends on it. Identify any surprising dependencies or high-fan-in/fan-out hotspots.

Note: Quote each `key=value` param as a single shell argument.
