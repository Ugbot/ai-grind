# Third-party attributions

Vendored/harvested code and skills that originate outside this repo, with their
upstream source and license. Keep this current when harvesting new skills.

## Vendored code

### GOAP planner core — `src/devtools_mcp/goap/`
- **Upstream:** agentix/GOAP (regressive-A* GOAP planner), via a copy in
  `C:\code\aibywire\work_runner\goap`.
- **License:** MIT.
- **Changes:** relative-import fix (absolute `devtools_mcp.goap.*` imports),
  removal of an unused import and a dead loop counter, trailing-whitespace
  cleanup. No behavioural changes to the planning algorithm.

## Harvested skills

_(Populated in Phase C when the external skill repos are harvested. Each entry:
skill name, upstream repo, license, commit.)_
