Run an autonomous coding task using the LLM Station agent.

The task to run is: $ARGUMENTS

Steps:
1. First verify the daemon is running:
   ```
   /Users/bengamble/llm-station/build/llm-station status --workspace /Users/bengamble/llm-station 2>&1
   ```
   If not running, tell the user to run `/ned-start` first.

2. Submit the task to the autonomous agent via the `autonomous_task` tool:
   ```
   /Users/bengamble/llm-station/build/llm-station --run autonomous_task task="$ARGUMENTS" max_turns=30 --workspace /Users/bengamble/llm-station 2>&1
   ```

3. Stream and report the full output including tool calls made, files changed, and the final result.

The agent has access to all tools: file read/write/edit, grep, symbol search, call graph analysis, git, refactoring, and more. It operates on workspace `/Users/bengamble/llm-station`.
