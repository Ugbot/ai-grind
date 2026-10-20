# Reuse Harvester library adoption

GRIND-11. The Gestalt2 harvest supplied a reusable curation workflow, a small
artifact contract caller and qualification lessons. These now have canonical
authored sources in ai-grind and generated plugin mirrors. The
[machine-readable review](reuse-harvest-integration-20261020.json) records each
reviewed source, disposition, identity and validation boundary.

The adopted skill is
[`reuse-harvesting`](../../skills/authored/skills/devtools/reuse-harvesting/SKILL.md).
It bundles the standalone artifact caller, a qualification reference and native
Codex TOML role. The authored
[Claude agent](../../skills/authored/agents/integration/reuse-harvester.md)
uses that skill. The TOML remains a native role asset for explicit configuration;
Markdown sync does not convert or register Codex agents. This integration
installs no scheduler and writes no global mirror.

## Adoption decisions

| Source capability | Decision | Result |
|---|---|---|
| Dedicated curator role/workflow | Adopt | Generic skill, Claude agent and native Codex role; pending discovery resumes before cutoff discovery |
| Standalone artifact check | Adopt with fixes | Imports safely, reports errors, requires specific failure types, checks byte preservation, replacement and cleanup; 14 contracts |
| Qualification notes | Adopt | General source/binary attribution and discriminating negative controls; historical native results stay provenance |
| Artifact hashing/publication primitives | Duplicate | Reuse existing `evidence/artifacts.py`; implementation and hash unchanged |
| Harvest index, ledger, discovery and validation records | Duplicate as historical evidence | Preserve original project ownership and link source identities; 48 discovery identities do not become 48 reviewed assets |
| Launcher and scheduler installer | Needs validation for library extraction | Retain one project runner and existing schedule; defects reported to the owning agent, no copied scheduler |
| Live-tree mutation/baseline runners described by the harvest | Reject for direct concurrent reuse | Ownership, isolation, restoration and expected failures require a separate design and coordinated native validation |

The original caller accepted any `ValueError` or `OSError` as success for every
negative case. That can hide the wrong failure, such as a permissions error in
an overwrite check. The adopted caller requires the expected error class and
verifies the old record survives. It adds explicit replacement, JSON size and
temporary cleanup checks and is safe to import without invoking argparse.

The library previously ignored authored agents. A failing synthetic check
reproduced that behavior. `skills/sync.py` now flattens
`authored/agents/<category>/<name>.md`, requires matching metadata, rejects
case-insensitive collisions with catalog/other authored output names and respects sidelined categories.
Shared target semantics stay per-item overwrite; unrelated entries survive.
`--target agents` remains skills-only. Catalog is untouched.

Review also found uncapped runner logs, incomplete POSIX descendant cleanup
after parent exit, and scheduler platform checks occurring after global role
mutation. Those findings were handed to the Gestalt2 owner for local fixes and
isolated process checks. The owner saved nine passing check categories,
including actual timeout/cancellation and an exited parent with a TERM-ignoring
descendant, under `docs/harvest/launcher-hardening-2026-10-20/` in Gestalt2.
The result and source manifest were reviewed as owner-produced evidence rather
than rerun by this library integration. Windows cleanup covers the immediate
process only; POSIX descendants that escape their session are outside its
contract. Successful invocation cutoff and pending discovery are
distinct; preserving the ledger cursor avoids dropping older deferred entries.
No evidence of actual cursor loss was established during this review.

## Validation and limits

The new sync/caller checks and existing artifact suite pass: 33 focused tests.
Ruff and Black pass for the changed Python files. Tests run with `--noconftest`
to exclude the repository's unrelated autouse DBOS executor fixture. The first
baseline invoked that fixture in its disposable test directory; subsequent
targeted runs exclude it. No persistent server or native build was started.

The standalone uv caller passes from `/private/tmp` with a script path containing
spaces. Missing checkout exits 1 and an omitted argument exits 2. Its JSON binds
the checked helper SHA256. Python is 3.12.13 on macOS 26.6.2 arm64; Linux and
Windows are unverified. Symlinks and hard links are required; missing privileges
fail explicitly. Helper bindings detect ordinary churn, without proving build
provenance or excluding changes that occur and revert between observations.

The Markdown and TOML metadata parse and plugin assets match authored source.
No new live agent invocation, behavioral pressure evaluation, cross-platform
qualification, native test, latency, race injection or durability drill is
claimed. Parent-owned launcher evidence has its own declared boundary in the
review JSON. Existing global roles and scheduling remain machine configuration.

Plugin regeneration reports 110 skills (80 harvested plus 30 authored), one
command and five agents. Review the exact staged source/mirror files before
landing; forensic scratch, existing untracked data and catalog are preserved.
