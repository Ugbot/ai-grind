# Artifact contract caller

Run from any directory with an explicit, trusted ai-grind checkout:

```sh
uv run --no-project --offline --script /path/to/skill/scripts/check_artifacts.py --ai-grind /path/to/ai-grind
```

Requires uv and an available Python >=3.12 interpreter. PEP 723 declares no
third-party dependencies. The script loads only the checkout's stdlib artifact
module, executes 14 checks with disposable synthetic files, then verifies the
module binding. Operations are small and dependent, so execution is sequential.
Only use a trusted checkout; a supplied Python module is executable code.

Exit 0 prints JSON with checks, Python/platform, module path and SHA256. Exit 1
reports IO, import or contract failure on stderr; argparse uses exit 2 for absent
arguments. Temporary files are cleaned on success and failure.

The caller requires symlink and hard-link support; Windows symlinks can require
privileges. A filesystem failure is a failure, never a skipped passing check.
The underlying helpers detect ordinary file churn; they do not defend against
hostile ancestor replacement or prove a file never changed and reverted.
An IO error after atomic publication may leave a completed record. Inspect that
record before retrying; default publication refuses overwrite.

Locally executed on macOS arm64 with Python 3.12.13. Linux and Windows remain
unverified. No race-injection, durability-loss, native or latency claim follows.
Provenance and exact validation live in
`docs/research/reuse-harvest-integration-20261020.json` in ai-grind.
