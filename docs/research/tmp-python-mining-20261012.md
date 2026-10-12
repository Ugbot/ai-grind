# Temporary Python mining and callable evidence library

Task: **GRIND-3**. Date: 2026-10-12. Canonical repository: `/Users/bengamble/ai-grind`.

The useful first library is `devtools_mcp.evidence`: stable file bindings,
read-only bounded artifact inventory, atomic JSON evidence publication, strict
schema-driven metric decoding, and strict JSONL decoding. It adds no dependency,
server registration, arbitrary-script registry, or benchmark acceptance policy.
Original scripts, data, binaries, logs, and failure artifacts remain intact.

## Actual inventory coverage

The accompanying `tmp-python-mining-20261012.json` records every accessible
candidate's path, size, timestamp, SHA256 and static summary, plus every traversal
error and exclusion. There are **2,362 Python files / 1,271 distinct hashes**.
Every candidate in the final pass was within the 2 MiB source-inspection cap,
parsed as AST, and retained the same size/timestamp during its individual read.
This is a timestamped observation of active directories, not a frozen filesystem.

| Canonical root | Entries visited | Directories visited | Pending at completion |
|---|---:|---:|---:|
| `/private/tmp` | 234,158 | 81,222 | 0 |
| `/private/var/tmp` | 106 | 14 | 0 |
| `/private/var/folders/bb/xk0ymlz56y35thcpsrkph8mh0000gn/T` | 12,232 | 5,150 | 0 |

Traversal caps were 500,000 entries **per root**, depth 64, and 2 MiB per Python
source. No entry, depth or source-size cap was hit. `/tmp` and `/var/tmp` aliases
were deduplicated. An immediate-child directory-name check under
`/Users/bengamble` found no `tmp`, `temp`, `scratch` or `.tmp` directory; no
unrelated home/data trees were recursively inspected.

**32 directories were inaccessible:** `/private/tmp/codex-daemon-501` and 31
protected application `TemporaryItems` directories under the macOS temp root.
Their exact paths and errno values appear in the JSON. Thus coverage is all
accessible candidates within the stated scope/exclusions, not all host files.

**1,070 exclusions** are recorded: symlinks and named dependency/vendor trees
(`.git`, `.venv`, `venv`, `node_modules`, `site-packages`, `__pycache__`,
`vendor`, `vendors`, `third_party`, `third-party`, `3rdparty`, `extern`), plus known
`scratchpad/dl/zstd` and `dl/lz4` source downloads. An initial discovery pass had
2,382 files; the refined pass excluded those downloaded dependencies and added
`/private/var/tmp`. The final counts above are the authoritative pass. One
inventory script written during mining is marked as generated mining tooling.

No discovered script was executed or imported. The inventory contains no source
bodies, string literals, credentials, environment dumps, or datasets. Static
summary metadata is deduplicated by SHA256. The live
`79-fixed-work-supervise-20261012.py` was observed only and remains provisional;
it was neither edited nor used as the library's authoritative implementation.

## Breadth and triage

These are **heuristic categories of unique hashes**, using filename/path and
bounded AST summaries. They are a triage index, not a claim that every line of
1,271 unique scripts received a semantic audit. All candidates received AST
inventory; the concrete origins below received focused mechanism inspection.

| Category | Unique scripts |
|---|---:|
| Misc developer / infrastructure tools | 571 |
| Asset / rendering / domain pipelines | 210 |
| Tests / oracle fixtures | 177 |
| One-off source / documentation edits | 103 |
| Process / resource orchestration | 91 |
| Artifact identity / evidence / retention | 83 |
| Validation / benchmark parsing / comparison | 35 |
| Generated mining tooling | 1 |

Static caution flags found machine-bound literals in 605 unique scripts,
destructive file operations in 111, and dynamic execution calls in 24. These
flags identify review candidates, not vulnerability verdicts. Literal values
were not copied into the inventory. Most useful non-storage patterns came from
Venus scratch/release snapshots, including its `venus_unreal_lib/fileio.py`,
`source_identity.py`, `reclaim_scan.py`, and graphics route utilities.

## Extracted functions and provenance

Aliases below shorten paths only. **S** = `/private/tmp/storage-goal-20261011`.
**V** = `/private/tmp/claude-501/-Users-bengamble-Venus/00cf88bf-eacc-4e62-b6f5-290653c4225a/scratchpad/idx1/src/tools`.
Exact origin paths, observed SHA256 values and function line numbers are in
`extraction_provenance` in the JSON artifact. Mechanisms were reimplemented as
small typed APIs; original scripts were not wholesale copied.

| API | Observed origins | Adaptation and rationale |
|---|---|---|
| `fingerprint_file`, `verify_binding` | S/40-archive-post-gc-control-db.py (`ident`, `sha`); S/verify_720_binding.py; V/venus_unreal_lib/fileio.py (`sha256_file`); V/venus_unreal_lib/source_identity.py (`file_sha256`) | Stream SHA256 under an explicit byte cap; compare pre/open/post file identities; reject nonregular/symlink leafs, oversized files and churn. Remove fixed roots, shell hash commands and success assertions on runtime conditions. Preserve local identity as well as content. |
| `inventory_artifacts` | V/venus_unreal_lib/reclaim_scan.py (`inventory`); S/40-build-live-namespace-evidence.py | Explicit entry/depth/logical-byte caps, exclusions, partial errors and `complete` flag. No deletion, unbounded `rglob` materialization, allocation/free-space claims, or domain-specific reclaim decisions. Iteration-level errors retain partial counters. |
| `write_json_record` | S/40-archive-post-gc-control-db.py (`syncjson`); V/unreal_ue_common.py (`atomic_json`); V/venus_unreal_lib/fileio.py | Same-directory temporary file, strict JSON, flush/fsync, atomic exclusive publication by default; explicit replace opt-in; POSIX directory fsync; own temporary cleanup; returned SHA256 binding. Remove hardcoded roots and record payloads. |
| `parse_metric_line` | S/40-consumer-flush-fix/after/scripts/goal_board.py (`parse_drir`) | Typed caller-declared fields and numeric bounds; reject duplicates, unknown/missing fields, invalid booleans/nonfinite numbers and wrong version token. Keep repo-specific completion/durability/scoring rules out. No default zero/pass for missing facts. |
| `decode_json_records` | S/sustained-src/sustained_gate.py (`decode_object`, `read_records`); V/run_graphics_route.py (`unique_object`, `finite_number`) | Bounded JSONL objects, duplicate-key rejection at every level, finite numeric decoding, explicit record count and nesting cap 64; blank/malformed rows fail. Keep record-type and workload schemas with callers. |

Existing ai-grind helpers were checked first. `store/provenance.py` supplies git
and host context but not content bindings; `station/links.py` and
`recipes/store.py` canonicalize their own domain payloads and retain their
existing contracts. `RunStore.store_artifact` and `export_bundle` remain the
storage/export seam. `build.exec.run_capture` and DBOS recipe execution remain
execution seams; generic supervision belongs to GRIND-5, not this package.

## Callable example

```python
from dataclasses import asdict
from pathlib import Path
from devtools_mcp.evidence import (
    MetricField, ScanLimits, fingerprint_file, inventory_artifacts,
    parse_metric_line, verify_binding, write_json_record,
)

source = fingerprint_file(Path("harness.py"), max_bytes=2 << 20)
metrics = parse_metric_line(
    "RESULT_V1 rows=12 seconds=0.25 durable=1",
    {"rows": MetricField("integer", minimum=0),
     "seconds": MetricField("number", minimum=0),
     "durable": MetricField("boolean")},
    prefix="RESULT_V1",
)
inventory = inventory_artifacts(
    Path("artifacts"), limits=ScanLimits(max_entries=1000, max_depth=8,
                                       max_total_bytes=64 << 20),
)
verify_binding(source)
receipt = write_json_record(Path("receipt.json"), {
    "source": asdict(source), "metrics": metrics,
    "inventory_complete": inventory.complete,
    "inventory_issues": [asdict(issue) for issue in inventory.issues],
})
verify_binding(receipt)
```

Expected input/limit failures use `ArtifactError` / `EvidenceParseError`
(`ValueError` subclasses); filesystem IO errors propagate as `OSError`.
Metric schema/configuration failures use `ValueError`. Missing data is rejected,
never interpreted as acceptance. Callers can publish complete failed-run records
with the same writer: it imposes no success-only policy.

The coordinated maintained MCP client (GRIND-4) reuses `write_json_record` for
result/receipt publication, providing a real consumer rather than leaving a
standalone library unconnected. Root owns final source/binary binding integration.

## Limits and practical guarantees

- These functions operate on caller-owned stable paths. Leaf no-follow and
  identity checks detect ordinary churn; they are **not** a security sandbox
  against adversarial ancestor-directory replacement.
- Inventory is read-only metadata observation, relative to explicit exclusions;
  `complete=True` does not make it an atomic tree snapshot. Logical byte totals
  are neither physical/APFS allocation nor reclaimed-space claims.
- File hashing defaults to 64 MiB and accepts an explicit cap up to 1 TiB.
  All reads are chunked and iteration counts bound the original-size scan.
- JSON records default to 1 MiB and allow explicit caps up to 32 MiB for the
  maintained client. The caller-owned Python object is serialized in memory
  before the encoded output cap; this is not an untrusted huge-object serializer.
- Atomic writes create no parents and preserve existing records by default.
  `replace=True` explicitly permits replacement. Serialization/prepublication
  failure retains the previous record. A directory-sync or postpublication
  verification error can leave the newly published record; the error is explicit.
  Successful POSIX writes sync file and parent; Windows lacks the POSIX directory
  durability guarantee. Files are published with the private tempfile mode.
- JSONL defaults: 1 MiB input, 10,000 rows, nesting depth 64. Metric lines:
  64 KiB default, 256 fields, decimal numbers and booleans exactly `0`/`1`.
  These decoders impose syntax/completeness, not benchmark equivalence or scoring.

## Deferred and rejected candidates

| Candidate / provenance | Decision and reason |
|---|---|
| Generic owned-process supervision: S/40-post-gc-memory-c-api-v2/supervise.py; S/parent40-ingest-geometry/supervise.py; active S/79-fixed-work-supervise-20261012.py | Deferred to GRIND-5 / the separate callable-seam audit. Requires process ownership, descendant cleanup, output/resource limits and durable failure semantics; no duplicate supervisor here. Active script remains provisional. |
| Bounded verified artifact export: S/40-archive-post-gc-control-db.py (`HashReader`, tar verification); existing `RunStore.export_bundle` | Candidate follow-up: fixed quotas, safe relative members, replayable manifest and content verification. Current library supplies bindings/inventory; archive/retirement policy is deliberately absent. |
| Current/peak RSS, APFS/free-floor sampling: S/40-post-gc-memory-c-api-v2/supervise.py; V/venus_unreal_lib/library_reclaim.py | Deferred: measurements need platform-specific contracts and honest logical/physical/peak distinctions. Raw snippets cannot become portable resource guarantees. |
| Histogram quantile interval comparison: S/sustained-src/sustained_gate.py (`histogram`, `quantile_bounds`, `conservative_latency_within_20pct`) | Candidate reusable math only; defer until bucket format and empty/tail/error semantics have an independent typed contract. Duration/workload/durability gates stay repository-specific. |
| Source/package containment and bounded reads: V/venus_unreal_lib/fileio.py (`contained`, `bounded_read`, `read_json_bytes`), V/venus_unreal_lib/source_identity.py | Bounded hashing extracted now. General containment/binary-package helpers deferred because some assume domain layouts and are not race-safe sandbox boundaries. |
| Strict external service proof parsing: /private/tmp/edp-strimzi-kafka-checks-abrupt-proof-final-guard.py and cold/abrupt variants | Domain/service identities and acceptance belong in that project. Generic JSON rejection mechanisms covered; no network or cluster actions performed. |
| Asset/render/image/GLB processing: V/venus_unreal_lib and associated scratch/release tools | Indexed for future domain plugins; not copied into generic devtools without consumers and dependency/runtime contracts. |
| One-off text rewriting: /private/tmp/g2perf79-edit-core.py; S/*patch.py and *update*.py | Rejected as callable library features: machine/file anchors and broad text replacement can corrupt unrelated sources. Retain them as provenance, not execution recipes. |
| File retirement/reclaim/archive apply: S/40-archive-post-gc-control-db.py retirement loop; V/venus_unreal_lib/reclaim.py (`apply`), library_reclaim.py | Destructive operations rejected from extraction. Useful verification mechanisms extracted independently. No original file was deleted. |
| Dynamic execution/embedded commands and broad process attachment | Rejected as general callable registry mechanisms. Scripts were not run, no arbitrary registry or elevated attach was added. |

## Verification

`tests/test_evidence_artifacts.py` and `tests/test_evidence_benchmark.py`:
**52 passed**, 2.38 seconds. Fixtures are synthetic files only. They cover normal
bindings, zero/exact/oversized files, same-size modification and replacement,
symlinks/nonregular paths, hash-time churn, traversal entry/depth/byte caps,
explicit IO and partial-iterator failures, atomic no-clobber/replace/cleanup and prepublication fsync failure preserving the previous record,
JSON byte caps, duplicate/missing/unknown metric fields, numeric bounds,
nonfinite JSON/metrics, nested duplicates, row caps and deterministic depth.

Scoped ruff and black checks pass for the package and its tests. Scoped mypy:
**Success: no issues found in 3 source files**. No dependencies were installed,
no native builds/benchmarks ran, and no live storage process was disturbed.

Required full-repository gates were run and failed outside this package:

- Ruff: existing `flamegraph/tree.py` UP037 and `tools/analysis_tools.py` SIM108;
  an in-progress GRIND-4 client UP035 was reported to its owner.
- Black: existing `flamegraph/tree.py`, `tracker/issues.py`,
  `tools/analysis_tools.py`, `tests/test_stack_filter.py`,
  `tests/test_stack_compare.py`; in-progress client formatting also flagged.
- Mypy: 24 errors in 12 unrelated files (GOAP, codegraph, debugger, Maven/Gradle,
  station tools and analysis tools); no new evidence-package errors.

The five existing ruff/black-related paths were unchanged relative to HEAD when
checked. In-progress client findings are not mislabeled as inherited failures.
Full raw gate logs are `/private/tmp/aigrind-mining-{ruff,black,mypy}-20261012.log`.
No unrelated gate failure was edited or ignored. No commits/pushes were made by
this extraction agent; root owns tracker, project-map integration and landing.


## Review correction: strict JSONL framing (2026-10-12)

Task review P2 found that `str.splitlines()` was an incorrect JSONL framing
mechanism: it split legal U+2028, U+2029 and U+0085 characters inside JSON strings,
and treated non-JSONL separators as record boundaries. Added causal regressions
first: **12 failed / 42 passed** before the correction.

`decode_json_records` now splits only on LF. The JSON decoder handles the CR
whitespace in standard CRLF framing. Empty input returns no records; exactly
one final LF remains optional; blank rows still fail. Added regressions for
Unicode characters inside strings, bare CR/control/Unicode separators between
objects, LF/CRLF with optional final terminators, and blank LF/CRLF rows.

Affected-only verification after the change:

- `.venv/bin/python -m pytest tests/test_evidence_benchmark.py -q --tb=short`:
  **54 passed in 2.84s**.
- `.venv/bin/ruff check src/devtools_mcp/evidence/benchmark.py tests/test_evidence_benchmark.py`:
  **All checks passed!**
- `.venv/bin/black --check src/devtools_mcp/evidence/benchmark.py tests/test_evidence_benchmark.py`:
  **2 files would be left unchanged.**
- `.venv/bin/mypy src/devtools_mcp/evidence/benchmark.py`:
  **Success: no issues found in 1 source file.**

This follow-up changed only the parser, its tests, and this report appendix.
Artifact/client implementation files and the inventory/provenance artifact were
unchanged; their tests were not rerun for this parser-only correction. Verification
used the existing Python 3.12 environment on macOS; no native builds, network calls
or new dependencies. Root must issue a new v3 binding for the changed source/test
and report files; this extraction agent performs no further edits after its final
freeze notice.
