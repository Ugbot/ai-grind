# Bind qualification evidence to actual inputs

Have the build/test owner choose explicit source, executable and fixture paths,
including dirty and untracked submodule inputs. A commit ID does not identify a
dirty build. Respect private data boundaries when saving fixture identities.

1. Bound file count and total bytes. Record `fingerprint_file(Path(...),
   max_bytes=...)` bindings from `devtools_mcp.evidence.artifacts`. Oversized,
   nonregular or observed-changing inputs fail qualification. Use an explicit
   larger executable budget where required.
2. Run the exact argv with a deadline and bounded log handling. Retain exit
   status, tool versions, flags/defines, complete test counts and JUnit output.
   Count cases from full logs when JUnit `system-out` is truncated.
3. Run `verify_binding` for each input afterwards. A failed binding invalidates
   attribution; preserve the failure. Stable before/after identities detect
   ordinary churn but do not prove build provenance or rule out mid-run changes
   that revert.
4. Publish bounded records with `write_json_record`; overwrite is refused by
   default. Surface incomplete inventories and IO/publication errors. Preserve
   raw failed evidence and inspect a possible published record before retrying.

## Keep negative controls discriminating

- Verify every acknowledged key/value using bounded ordered pages and an
  overflow sentinel. Counts and sums can hide payload corruption and key loss.
- Require dictionary `data` and list `rows` for negative probes too. Missing,
  null or false data must fail. Choose a probe key outside all attempted writes,
  including unacknowledged attempts.
- A failed negative probe invalidates an exact proof after all positive rows
  pass. Never weaken an oracle to obtain a passing extraction.
- A controlled defect must compile, then fail the intended assertion. Preserve
  original bytes, mutated hash, exact command, expected failure, restoration
  hash, restored rebuild and final green run. A crash or compile failure alone
  does not establish oracle discrimination.
- Mutating source and rebuilding require exclusive ownership and coordination.
  Keep live-tree scratch runners as provenance until a safe isolated design is
  implemented and exercised.

Local receipts, injected source faults, actual durability loss and distributed
quorum/failover are separate qualifications. Historical native results are
provenance rather than a fresh gate for a newly extracted tool. The bundled
contract caller validates helper behavior on its declared local platform only.

Adapted from Gestalt2's `docs/harvest/qualification/README.md` and its linked
2026-10-20 Iceberg qualification/mutation evidence. Source identities and review
decisions are in ai-grind's
`docs/research/reuse-harvest-integration-20261020.json`. No historical native
qualification has been rerun for this library adoption.
