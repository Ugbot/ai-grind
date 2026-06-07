Grep the llm-station codebase for a pattern using the daemon.

The pattern to search for: $ARGUMENTS

Run:
```bash
timeout 20 llm-station --run grep_search "pattern=$ARGUMENTS" "context_lines=2" --workspace /Users/bengamble/llm-station 2>&1
```

To restrict to specific file types, the pattern can be passed as: `pattern glob=*.cpp` style — parse accordingly from $ARGUMENTS.

If the user specified a glob (e.g. `ned-grep somePattern *.cpp`), split and run:
```bash
timeout 20 llm-station --run grep_search "pattern=<PATTERN>" "glob=<GLOB>" "context_lines=2" --workspace /Users/bengamble/llm-station 2>&1
```

Note: Always quote each `key=value` as a single shell argument to prevent shell glob expansion.
Daemon workspace: `/Users/bengamble/llm-station`
