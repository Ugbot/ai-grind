Start the LLM Station daemon for this workspace.

Run the following steps:

1. Check if the daemon is already running:
   ```
   llm-station status 2>&1
   ```

2. If NOT running, start it in the background:
   ```
   ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY llm-station start --verbose 2>&1 &
   ```
   Then wait 3 seconds and verify it started:
   ```
   sleep 3 && llm-station status 2>&1
   ```

3. Report back: is the daemon running, what tools are registered, and what model/provider is active. Use:
   ```
   llm-station --list-tools 2>&1 | head -40
   ```

The daemon binary is at `llm-station`.
The workspace is `the current workspace`.
It needs `ANTHROPIC_API_KEY` in the environment for Claude providers to work.
