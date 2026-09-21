# Station progression implementation

ai-grind remains the lightweight entry point. Station gradually takes ownership of
shared internals; full Station adds indexing, inference and richer interfaces.
The remote collaboration platform remains optional for both.

## Delivery ledger

- Upstream reconciled in isolated worktrees; original dirty checkouts preserved.
- Reliability: project-scoped links, codegraph registration, cached correlation,
  paged peer exchange, immutable recipe snapshots and explicit-input cache keys.
- Validation: isolate credentials and enforce lint, formatting and type checks.
- Native slice: model-free stdio MCP Station core and opt-in Node CPU adapter.
- Migration: a read-only tracker preflight reports importer coverage and omitted
  records. It always blocks ownership transfer; backup and reverse migration are
  not implemented. Existing stores retain their owners.

## Native contract

The separately built `station-core` executable owns Node CPU execution and
canonical stack extraction. ai-grind discovers protocol version 1 over stdio MCP,
checks `node.cpu` support, imports the resulting artifacts and retains a local
copy for offline analysis. Failures never rerun the workload in another engine.
Native run IDs, timestamps, engine identity and source frame locations survive
restart. Comparison rejects mixed legacy and native frame formats.

The core has no account, model, indexer or GUI dependency. It bounds protocol
input and artifacts, supports cancellation and deadlines, and terminates its
owned process tree. Atomic rename gives complete-run visibility; this preview
does not promise power-loss durability. Recipe recovery uses immutable resolved
execution snapshots; external commands still require idempotency for retries.
Recipe caching includes explicit environment and working-directory overrides;
use force when external files or ambient environment have changed.

## Verification

Verified on 2026-09-21: 905 Python tests passed, one skipped with the native
binary enabled; both native CTest targets passed (ten MCP integration cases
plus the expansion-bound check). Lint, formatting, types, prose and plugin
mirror checks passed. Dashboard tests require permission to bind local sockets.

Run from an ai-grind checkout with its development dependencies installed:

```sh
STATION_CORE_TEST_BINARY=/absolute/path/to/station-core uv run pytest -q
uv run ruff check src tests
uv run black --check src tests
uv run mypy src/devtools_mcp
uv run python scripts/unslop_check.py
uv run python skills/sync.py --target plugin
git diff --exit-code plugin/
```

The optional native tests execute real Node workloads, persist/reopen their
results, analyze and compare captures, and check failure paths do not execute
twice. Station's `core/devtools` CTest also checks import validation,
flame generation, cancellation, EOF, idle shutdown, process descendants and
expanded artifact bounds. These native checks passed on macOS ARM64; the added
Linux and Windows CI jobs still need execution.

The remote graph contract was checked against the actual local platform router
and database service with an isolated in-memory database and test identity:

```sh
PYTHONPATH=src:../llm-station-remote/src \
  ../llm-station-remote/.venv/bin/python scripts/check_station_platform_contract.py
```

This verifies create/update, project separation and the organisation guard. It
does not exercise deployed authentication, billing or network infrastructure.

One local startup observation measured 1.320 seconds to initialize the Python
entrypoint (39 tools, 69,430 schema bytes), and 0.005 seconds for the native core
(8 tools, 2,612 schema bytes). These expose different feature sets and are not a
like-for-like performance comparison. The macOS Release core is 640,320 bytes.

## Deferred rollout gates

Replacement of remaining profiler/debugger/recipe/tracker/skill implementations
requires their own conformance results. Tracker import currently omits replica,
platform-link and collaboration state; it must not become an automatic ownership
transfer. Cross-platform binaries and Venus graph-quality remeasurement are
required before a general release. Existing users retain the Python default.

Next, execute the three-platform CI matrix and publish versioned core artifacts.
Then complete tracker backup, full-state import, exclusive ownership and rollback
tests before enabling transfer. Expand native engines one workflow at a time,
with persisted-result and failure-path conformance before changing defaults.
Remeasure Venus against fixed repositories and explicit graph-quality criteria
before using it as evidence for broader indexing adoption.
