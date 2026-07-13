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

46 skills harvested (2026-07-13) from four upstream collections into `catalog/`
via `sources.toml`. Clones live under `C:/code/vendor-skills/`. We **vendor as we
go**: each skill is copied verbatim, kept under its own category, and credited
below with a link to the source. Redundant items (a second TDD, competing
skill-routers), and repo-owner-specific / creative / Office-document /
proprietary-licensed skills were deliberately left behind. With thanks to the
authors — please see the upstream repos for the canonical versions.

### addyosmani/agent-skills — Addy Osmani — MIT
Source: https://github.com/addyosmani/agent-skills
`interview-me`, `idea-refine`, `spec-driven-development`, `planning-and-task-breakdown`,
`context-engineering`, `source-driven-development`, `incremental-implementation`,
`api-and-interface-design`, `frontend-ui-engineering`, `code-review-and-quality`,
`code-simplification`, `security-and-hardening`, `performance-optimization`,
`browser-testing-with-devtools`, `documentation-and-adrs`, `observability-and-instrumentation`,
`git-workflow-and-versioning`, `ci-cd-and-automation`, `deprecation-and-migration`,
`shipping-and-launch`.

### obra/superpowers — Jesse Vincent (obra) — MIT
Source: https://github.com/obra/superpowers
`brainstorming`, `writing-plans`, `executing-plans`, `subagent-driven-development`,
`dispatching-parallel-agents`, `systematic-debugging`, `verification-before-completion`,
`requesting-code-review`, `receiving-code-review`, `using-git-worktrees`,
`finishing-a-development-branch`, `writing-skills`.

### mattpocock/skills — Matt Pocock — MIT
Source: https://github.com/mattpocock/skills
`grill-with-docs`, `grilling`, `domain-modeling`, `codebase-design`, `to-spec`,
`to-tickets`, `wayfinder`, `research`, `resolving-merge-conflicts`, `handoff`,
`improve-codebase-architecture`.

### anthropics/skills — Anthropic — Apache-2.0
Source: https://github.com/anthropics/skills
`mcp-builder`, `skill-creator`, `webapp-testing`. (The Office-document skills —
docx/pdf/pptx/xlsx — are source-available/proprietary and were NOT taken.)

Each item's `origin` path is also recorded per-skill in `MANIFEST.json`.
