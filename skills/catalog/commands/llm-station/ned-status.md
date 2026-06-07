Check the status of the LLM Station daemon and report a full summary.

Run these commands and report on each:

1. Daemon process status:
   ```
   /Users/bengamble/llm-station/build-ned/llm-station-mcp status --workspace /Users/bengamble/llm-station 2>&1
   ```

2. If running, list available tools:
   ```
   /Users/bengamble/llm-station/build-ned/llm-station-mcp --list-tools --workspace /Users/bengamble/llm-station 2>&1
   ```

3. Check configured LLM providers/models:
   ```
   /Users/bengamble/llm-station/build-ned/llm-station-mcp --run manage_models operation=list --workspace /Users/bengamble/llm-station 2>&1
   ```

4. Check recent log tail:
   ```
   /Users/bengamble/llm-station/build-ned/llm-station-mcp log --workspace /Users/bengamble/llm-station 2>&1 | tail -20
   ```

Report: running/stopped, PID, tool count, active model, any errors.
Workspace: `/Users/bengamble/llm-station`
Binary: `/Users/bengamble/llm-station/build-ned/llm-station-mcp`
