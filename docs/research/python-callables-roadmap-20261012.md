# Reusable Python callables from development scratch work

Date: 2026-10-12. Tracker: GRIND-3, GRIND-4, GRIND-5.

The repeated helpers in development temporary directories belong in maintained
ai-grind modules when they encode a reusable operation. The original scripts
remain evidence of what actually ran; replacing a helper does not rewrite old
results or promote a failed experiment to a pass.

## Library boundary

- **GRIND-3 — evidence utilities:** bounded filesystem inventory, content binding
  and verification, schema-driven metric and JSONL parsing. Public Python
  functions live in `src/devtools_mcp/evidence/`. The [mining report](tmp-python-mining-20261012.md)
  records inspected roots, omissions, deduplication, source provenance and which
  candidates were extracted or deferred. Discovered scripts are read as data,
  never imported or executed during mining.
- **GRIND-4 — MCP client:** a maintained typed caller and `devtools-call` CLI use
  the installed MCP SDK. See [usage and failure semantics](../devtools-call.md).
  Explicit requests carry tool names and structured arguments. An interrupted
  call can have an unknown outcome; it must not be retried automatically.
- **GRIND-5 — process supervision:** planned integration through the existing
  recipe tool and result store, described below. This is not implemented by the
  evidence library or MCP client.

Workload fixtures, exact query/result oracles, build matrices and performance
acceptance thresholds stay in the repositories that own them. A generic library
must not decide whether a MarbleDB benchmark passes, change its resource caps,
or import campaign-specific paths and commands. Dependency/vendor scripts are
not silently incorporated as owned source.

## Process supervision integration plan (GRIND-5)

The existing path is `recipe` → `recipes.runner._exec_step_activity` →
`build.exec.run_capture`. Preserve this public tool instead of adding a parallel
MCP execution registry. `run_capture` currently truncates output after
`communicate()` and does not verify process-group emptiness; its output limit
is therefore not a bound on resident capture memory. Windows cleanup currently
targets the direct child. These limitations require explicit implementation
work before stronger supervision claims.

1. Add typed process specification, limits and result models. Carry exact argv,
   cwd, bounded environment overrides, explicitly owned artifact roots and
   content bindings. Validate positive finite limits and supported capabilities
   before launch. Environment values are not implicitly published as evidence.
2. Add a callable supervisor with bounded streamed output capture. Keep a launch
   receipt and independently record observation, signal and cleanup errors.
   Termination attempts and direct-child reaping must not depend on a successful
   process-list observation. Unknown group state is distinct from empty.
3. Sample optional group RSS, file bytes, entry count and filesystem free space.
   Report thresholds, sampling interval, observed peaks and possible overshoot;
   these checks are not OS hard quotas. Define file-byte accounting explicitly.
   Reuse evidence helpers where their semantics match rather than broadening
   them to silently tolerate changing retained artifacts.
4. Publish launch/progress/terminal evidence atomically through the existing
   RunStore artifact location and typed result catalog. A receipt-write failure,
   unknown cleanup, resource breach or nonzero child exit cannot count as a pass.
   Group ownership alone cannot contain a child that deliberately detaches.
5. Extend recipe steps with explicit argv and supervision data. Preserve legacy
   shell command compatibility, require exactly one command representation,
   include policy and bindings in canonical spec hashes, and persist nullable
   evidence references with a versioned migration. DBOS checkpoints carry bounded
   summaries and references rather than full logs.
6. Test synthetic observation failures and tiny owned child processes: launch
   failure, timeout/cancellation, output flood, descendant survival, TERM
   resistance, unknown group state, retained partial evidence and failed writes.
   Cover recipe round trips, migration, stop-on-failure and replay references.
   POSIX may ship first with an explicit capability; Windows needs an owned
   process-tree/Job Object implementation for equivalent guarantees.

Fresh benchmarks must force execution until recipe cache identity includes all
relevant per-run environment, executable and dataset bindings. A cached result
is not a new measurement. Reconcile owned processes before considering any
retry of an interrupted run.

The local audit is recorded in `aigrind-callable-seam.md` under the retained
`/tmp/storage-goal-20261011/` evidence directory. Its corrected supervisor input
and focused rereview are retained there as `79-corrected-supervisor-frozen.py`
and `79-supervision-rereview.md`. Both previously found cleanup/catalog-lifecycle
defects are reviewed as fixed in that frozen input; this does not validate a
future generic supervisor. G2PERF-79 final corrected native endpoint validation
still awaits integration and fresh resource admission. No performance gate is
advanced by this library extraction.
