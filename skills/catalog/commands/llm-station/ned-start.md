Start the LLM Station daemon for this workspace.

Run the following steps:

1. Check if the daemon is already running:
   ```
   /Users/bengamble/llm-station/build-ned/llm-station-mcp status --workspace /Users/bengamble/llm-station 2>&1
   ```

2. If NOT running, start it in the background:
   ```
   ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY /Users/bengamble/llm-station/build-ned/llm-station-mcp start --workspace /Users/bengamble/llm-station --verbose 2>&1 &
   ```
   Then wait 3 seconds and verify it started:
   ```
   sleep 3 && /Users/bengamble/llm-station/build-ned/llm-station-mcp status --workspace /Users/bengamble/llm-station 2>&1
   ```

3. Report back: is the daemon running, what tools are registered, and what model/provider is active. Use:
   ```
   /Users/bengamble/llm-station/build-ned/llm-station-mcp --list-tools --workspace /Users/bengamble/llm-station 2>&1 | head -40
   ```

The daemon binary is at `/Users/bengamble/llm-station/build-ned/llm-station-mcp`.
The workspace is `/Users/bengamble/llm-station`.
It needs `ANTHROPIC_API_KEY` in the environment for Claude providers to work.
