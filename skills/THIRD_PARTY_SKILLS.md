# Third-party attributions

Vendored and harvested code and skills that originate outside this repo, with
their upstream source and license. Keep this current when harvesting.

## Vendored code

### GOAP planner core, `src/devtools_mcp/goap/`

Upstream: agentix/GOAP, a regressive-A* GOAP planner, via a copy in
`C:\code\aibywire\work_runner\goap`. License: MIT.

Changes we made: absolute `devtools_mcp.goap.*` imports in place of relative
ones, removal of an unused import and a dead loop counter, trailing-whitespace
cleanup. The planning algorithm behaves exactly as upstream.

## Harvested skills

79 skills and 3 agents live in `catalog/`, harvested from five upstream
collections through `sources.toml`. First pass 2026-07-13, extended 2026-08-19
with cursor/plugins, trimmed the same day by the dedup pass recorded below.
Clones live under `C:/code/vendor-skills/`.

We vendor as we go. Each skill is copied verbatim, kept under its own category,
and credited below with a link to its source. We left behind
repo-owner-specific, creative, Office-document, and proprietary-licensed skills,
plus anything that duplicated a skill already here. Thanks to the authors, and
please see the upstream repos for the canonical versions.

### addyosmani/agent-skills, Addy Osmani, MIT

Source: https://github.com/addyosmani/agent-skills

`interview-me`, `idea-refine`, `spec-driven-development`, `context-engineering`,
`source-driven-development`, `incremental-implementation`,
`api-and-interface-design`, `frontend-ui-engineering`, `code-simplification`,
`security-and-hardening`, `performance-optimization`, `documentation-and-adrs`,
`observability-and-instrumentation`, `git-workflow-and-versioning`,
`ci-cd-and-automation`, `deprecation-and-migration`, `shipping-and-launch`.

### obra/superpowers, Jesse Vincent (obra), MIT

Source: https://github.com/obra/superpowers

`brainstorming`, `writing-plans`, `executing-plans`, `systematic-debugging`,
`verification-before-completion`, `receiving-code-review`, `using-git-worktrees`,
`finishing-a-development-branch`.

### mattpocock/skills, Matt Pocock, MIT

Source: https://github.com/mattpocock/skills

`domain-modeling`, `codebase-design`, `to-spec`, `to-tickets`, `wayfinder`,
`research`, `resolving-merge-conflicts`, `handoff`,
`improve-codebase-architecture`.

### anthropics/skills, Anthropic, Apache-2.0

Source: https://github.com/anthropics/skills

`mcp-builder`, `skill-creator`, `webapp-testing`. The Office-document skills
(docx, pdf, pptx, xlsx) are source-available or proprietary, so we did not take
them.

### cursor/plugins, MIT

Source: https://github.com/cursor/plugins. Clone:
`C:/code/vendor-skills/cursor-plugins`. Harvested 2026-08-19: 42 skills and 3
agents, all MIT. Copyright by plugin: `pstack` (c) Lauren Tan (poteto);
`cursor-team-kit`, `thermos`, `cli-for-agent` (c) Cursor.

Upstream keeps one `LICENSE` per plugin rather than per skill, so the four MIT
texts are vendored verbatim in
[`vendor-licenses/cursor-plugins/`](vendor-licenses/cursor-plugins/). That
directory sits outside `catalog/`, which `harvest.py` wipes.

What we took, by category:

- `writing/`: `unslop`, `technical-writing` (pstack), `deslop` (cursor-team-kit).
- `understanding/`: `how`, `why`, `teach`, `recall` (pstack).
- `planning/`: `architect`, `arena`, `figure-it-out` (pstack).
- `review/`: `interrogate`, `blast-radius`, `no-comments` (pstack), `verify-this`
  (cursor-team-kit), `thermo-nuclear-review`,
  `thermo-nuclear-code-quality-review` (thermos).
- `meta/`: `reflect`, `show-me-your-work`, `automate-me`,
  `create-verification-skill`, `maintain-verification-skill` (pstack).
- `build/`: `cli-for-agents` (cli-for-agent), `typescript-best-practices`
  (pstack), `control-cli` (cursor-team-kit).
- `ship/`: `loop-on-ci`, `make-pr-easy-to-review`, `what-did-i-get-done`
  (cursor-team-kit).
- `principles/`: 15 of pstack's 21 `principle-*` skills, namely
  `boundary-discipline`, `build-the-lever`, `encode-lessons-in-structure`,
  `experience-first`, `foundational-thinking`, `laziness-protocol`,
  `make-operations-idempotent`, `migrate-callers-then-delete-legacy-apis`,
  `minimize-reader-load`, `never-block-on-the-human`,
  `outcome-oriented-execution`, `redesign-from-first-principles`,
  `separate-before-serializing-shared-state`, `subtract-before-you-add`,
  `type-system-discipline`.
- `agents/review/`: `comment-sicko` (pstack, driven by `no-comments`),
  `thermo-nuclear-review-subagent`,
  `thermo-nuclear-code-quality-review-subagent` (thermos).

#### Deliberately skipped

Six duplicate principles: `fix-root-causes` (we have `systematic-debugging`),
`prove-it-works` (`verification-before-completion`), `guard-the-context-window`
(`context-engineering`), `model-the-domain` (`domain-modeling`),
`sequence-verifiable-units` (`incremental-implementation`),
`exhaust-the-design-space` (`arena` covers the mechanic).

Cursor-product skills: `cursor-sdk`, `create-plugin-scaffold`,
`review-plugin-submission`, `pr-review-canvas`, `docs-canvas` for Cursor Canvas,
`orchestrate` for the Cursor SDK cloud-agent tree, `setup-pstack`, and
`workflow-from-chats`, which reads Cursor's chat storage.

Overlaps we already cover: `fix-merge-conflicts` (`resolving-merge-conflicts`),
`swarm` (the Agent and Workflow tools), `tdd`, `review-and-ship`,
`check-compiler-errors`, `new-branch-and-pr`, `run-smoke-tests`, `fix-ci` (a
subset of `loop-on-ci`), `get-pr-comments`, `weekly-review` (near-duplicate of
`what-did-i-get-done`), `control-ui` (`webapp-testing`), the `thermos` launcher
(spawn the two subagents directly), `ralph-loop` (Claude Code has `/loop`),
`continual-learning` (a hook plus Cursor memory), `check-agent-compatibility`,
the `teaching` plugin, and pstack's `bro`.

Environment-bound: `poteto-mode`, an umbrella mode skill that pulls in the whole
pstack playbook tree, and the Benny Slack automations (`setup-benny`,
`triage-issue-reports`, `reproduce-and-fix-issues`).

#### Known rough edges, kept verbatim on purpose

Several pstack skills read model names from `~/.cursor/rules/pstack-models.mdc`
and fall back to hardcoded model IDs. The fallbacks work. That Cursor rule file
just will not exist here.

`figure-it-out` points at the `poteto-mode` skill's Principles section and
`automate-me` at a `create-skill` skill. We took neither, so read those as
`principles/` and `skill-creator`. They are prose pointers, so nothing breaks.

Upstream skills use Cursor's `disable-model-invocation: true`, and Claude Code
honors it, which makes those skills user-invoked only:
`/devtools-mcp:architect`, `/devtools-mcp:principle-laziness-protocol`, and so
on. The model cannot load them through the Skill tool. That covers `architect`,
`arena`, `figure-it-out`, `interrogate`, `blast-radius`, `no-comments`, both
`thermo-nuclear-*`, `reflect`, `show-me-your-work`, `automate-me`, both
`*-verification-skill`, `teach`, `recall`, `technical-writing`, and all 15
`principle-*`. Upstream relies on the `poteto-mode` umbrella to pull the
principles in ambiently and we did not take it. To make them ambient here, strip
the flag upstream rather than in `catalog/`, because `harvest.py` re-copies.

`comment-sicko`'s agent name contains a space, so `no-comments` spawns it as
`subagent_type: "Comment Sicko"`.

### rudybear/renderdoc-skill — Alexey Medvedev — MIT
Source: https://github.com/rudybear/renderdoc-skill
`renderdoc-gpu-debug`. Vendored at `C:/code/vendor-skills/renderdoc-skill`.

Wraps [`rdc-cli`](https://github.com/BANANASJIM/rdc-cli) (66 commands over
RenderDoc's **Python** API): pixel history, shader debugging, mesh output,
render-target export, frame comparison.

**Complements — does not replace — the authored `renderdoc-frame-analysis`
skill.** They use different backends, and that difference decides which to reach
for:

| | backend | needs |
|---|---|---|
| `renderdoc-frame-analysis` (authored) | devtools-mcp `renderdoc` suite → `renderdoccmd.exe` / `qrenderdoc.exe` | RenderDoc install only |
| `renderdoc-gpu-debug` (vendored) | `rdc-cli` → `renderdoc.pyd` Python module | RenderDoc **built from source** with Python bindings |

**RUNTIME PREREQUISITE IS UNMET ON THIS MACHINE (checked 2026-08-01).**
`rdc-cli` 0.6.3 installs fine, but `rdc doctor` reports three failures:

```
[FAIL] renderdoc-module    not found in search paths
[FAIL] replay-support      renderdoc module unavailable
[FAIL] win-python-version  renderdoc.pyd not found
```

Cause: the stock Windows RenderDoc installer (1.45, `C:\Program Files\RenderDoc`)
ships `renderdoc.dll` and an embedded **Python 3.6** (`python36.dll`), but no
`renderdoc.pyd`. That module is not on PyPI and must be built from source, and
the system Python here is 3.12 — so the bindings would have to be rebuilt
against it.

The remedy is `rdc setup-renderdoc` (a from-source RenderDoc build). Its
prerequisites ARE present — VS Build Tools 17.14.36127.28 and `renderdoccmd`
1.45 — so this is a long build, not a missing dependency. It has deliberately
NOT been run: it is a multi-GB, long-running third-party build, and that is the
owner's call.

Everything else passes: `renderdoccmd`, the RenderDoc install, and the Vulkan
layer registered at `C:\Program Files\RenderDoc\renderdoc.json`.

Note also that `rdc.exe` installs to
`C:\Users\Capta\AppData\Roaming\Python\Python312\Scripts`, which is **not on
PATH** — that must be added before the skill's commands resolve.

Until then GPU frame work is not blocked: `devtools_check` reports all five
devtools-mcp `renderdoc` tools available (capture / analyze / counters /
resources / thumb), and that remains the working path.

## Retired 2026-08-19 (dedup pass)

Ten harvested skills left the library after an audit of all 122 loadable skills.
Each was a duplicate of something we kept, or depended on something this setup
does not have. Nothing else replaced them.

| Retired | Kept instead |
|---|---|
| `writing-skills` (superpowers) | `skill-creator`, which is bigger and ships an eval harness |
| `code-review-and-quality` (agent-skills) | the harness `/code-review` skill, plus `thermo-nuclear-code-quality-review` |
| `requesting-code-review` (superpowers) | the harness `/code-review` skill and `interrogate`. `receiving-code-review` stays, since responding to feedback is a different job |
| `planning-and-task-breakdown` (agent-skills) | `writing-plans` and `tracker-breakdown` |
| `subagent-driven-development` (superpowers) | the Agent and Workflow tools |
| `dispatching-parallel-agents` (superpowers) | the Agent and Workflow tools |
| `browser-testing-with-devtools` (agent-skills) | `webapp-testing`. It needed the `chrome-devtools` MCP server, which is not configured here |
| `grilling` (mattpocock) | `interview-me`, which runs the same one-question-at-a-time protocol and lists "grill me" as a trigger |
| `grill-with-docs` (mattpocock) | nothing. Its whole body was one line: run `/grilling` using `/domain-modeling` |
| `perfview-etw-trace` (local, MarbleDB) | `etw-profiling`, the generic devtools-mcp version. The retired one hardcoded MarbleDB's `tools/perf_trace.py` |

The same pass moved the four retired `llm-station-*` skills from their old
`code-intel/` and `debug/` folders into `_disabled/`. `sources.toml` had already
marked them `_disabled`, but the router builds its index by walking the tree, so
the stale folders kept them advertised as loadable.

Each item's `origin` path is recorded per-skill in `MANIFEST.json`.

## Design influences (not vendored)

Ideas we adopted and re-authored in our own stack. No source files copied, so
these are design influences rather than vendored code.

### Understand-Anything, and our native code property graph

Upstream: [Egonex-AI/Understand-Anything](https://github.com/Egonex-AI/Understand-Anything),
Yuxiang Lin / Infinite Universe, Inc., MIT.

What we took: the node and edge taxonomy for a unified code knowledge graph
(function, class, file, module nodes; calls, imports, contains, inherits edges),
and the click-to-focus graph view.

How it became ours: re-authored in C++ over our MarbleDB engine, as
`MarblePropertyGraphStore` and `PropertyGraphBuilder` in `llm-station` with
`graph_build`, `graph_query`, and `graph_export`, plus our own extensions
(`profiled_by`, `implements_task`, planner metadata) and a server-rendered SVG
view in the devtools-mcp dashboard under `codegraph/`. We did not vendor their
TypeScript and Zod schema, React-Flow dashboard, or scan scripts.
