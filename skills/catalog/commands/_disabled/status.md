Check the status of the LLM Station daemon and report a full summary.

Run these commands and report on each:

1. Daemon process status:
   ```
   llm-station status 2>&1
   ```

2. If running, list available tools:
   ```
   llm-station --list-tools 2>&1
   ```

3. Check configured LLM providers/models:
   ```
   llm-station --run manage_models operation=list 2>&1
   ```

4. Check recent log tail:
   ```
   llm-station log 2>&1 | tail -20
   ```

Report: running/stopped, PID, tool count, active model, any errors.
Workspace: `the current workspace`
Binary: `llm-station`
