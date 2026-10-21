# Helper and publication contract

## Secret-free helper

Run the bundled script with UV, from any working directory:

```sh
uv run --script /path/to/cloudflare-control/scripts/cloudflare_control.py s3-config --account ACCOUNT_ID --bucket BUCKET_NAME
uv run --script /path/to/cloudflare-control/scripts/cloudflare_control.py auth-status --project /path/to/project
```

Python3.12+, stdlib only; PEP723 declares no dependencies. `s3-config` performs
no remote I/O and writes no files. It validates a32-digit lowercase hexadecimal
account id, R2 bucket name and default/EU jurisdiction, then emits portable
endpoint/region/bucket/path-style configuration, standard AWS environment key
names and presence booleans. It never prints their values. Environment input
stays process-local. `remote_verified:false` is unconditional.

`auth-status` chooses cf unless the project uses a Wrangler file without a
`cloudflare.config.ts`. Explicit `--provider cf|wrangler` overrides that choice.
CLI paths are explicit `--cf`/`--wrangler` overrides or PATH discovery; there is
no machine-specific npm cache path, install fallback, credential file read,
token export or resource action. A `.cmd`/`.bat` launcher requires its actual
JavaScript entry via `--cf-js`/`--wrangler-js` and `--node`, avoiding shell
interpolation on Windows. These overrides remain official CLI entry points.

The only child command is cf `auth whoami` or Wrangler `whoami --json`. It may
use the official tool's own OAuth refresh. stdin is closed, child stdout/stderr
are drained concurrently with64KiB caps each, and timeout is finite (default20s,
maximum60s). Cancellation/failure terminates the child process group on POSIX;
Windows uses bounded taskkill tree cleanup then kills/reaps the direct child;
its descendant cleanup is best-effort if the parent exited before taskkill.
Output is allowlisted to auth validity and up to100 account ids/names; no raw
stderr, emails, scopes, arbitrary JSON or credential-store paths are returned.
Known secret environment values are redacted from returned account names.

Errors are JSON `{status:error, code:...}` with exit1. Unsupported/missing CLI,
invalid parameters, malformed/oversized status, timeout, auth failure and invalid
accounts have explicit codes. No retry occurs. Success is a bounded statement
about that CLI, not MCP auth or S3 permissions. macOS/Windows/Linux behavior
requires its own qualification; source portability is not an executed result.

## Selective publication

Defaults of `skills/harvest.py` and `skills/sync.py` remain unchanged. Full
harvest needs every legacy source, including unavailable Windows paths; default
sync wipes wholly owned mirrors. Use their explicit `--additive --select NAME`
mode for this slice, repeating `--select` for each exact requested asset.

Harvest only the19 registered official names under category `cloudflare`.
Sources use `~/.agents/skills/<name>` so each user resolves their own official
install. Authored `cloudflare-control` is selected by sync, not harvested.
Select `cloudflare-controller` only for plugin/local mirrors; the agents mirror
is skills-only. Selected mode refuses project/global targets.

Selected inputs and destination conflicts are preflighted and staged before
publication. Identical files are skipped, absent files are published without
replacement, and differing files or extra destination files refuse publication.
A compatible partial directory resumes by adding missing files. Unrelated
catalog, manifest entries and mirror directories are preserved. Metadata is
published atomically after selected outputs succeed. Publication needs local
filesystem hard-link support for atomic create; unsupported filesystems fail
explicitly. No deletion, pruning, OAuth action or global sync is implicit.

The root execution owner reviews and runs qualification and selected publication.
Until that gate runs, authored source and registrations are prepared assets,
not a claim that generated plugin or current sessions have loaded them.
