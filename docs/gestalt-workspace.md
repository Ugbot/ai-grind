# Portable Gestalt workspace reconstruction

`devtools_gestalt_workspace(action, bundle_dir, base_url, limits=None)` supports
`inspect`, `install`, and `verify` for the original synthetic `cme.research` v1
bundle: four notebook JSON documents and one dashboard template. Supply the
absolute bundle directory and an explicit `http://127.0.0.1:PORT` or
`http://[::1]:PORT` endpoint. It uses ordinary compute gateway actions; it does
not execute cells or restore a database or lost user edits. The safe no-follow file reader currently requires macOS/Linux POSIX
descriptors; unsupported platforms return `unsupported_platform` before HTTP,
and Windows file admission is not qualified. No authentication
is configured by this callable; use the existing local demo server configuration.

Validation precedes every HTTP mutation: stable bounded direct-child files inside the bundle (no symlink following),
unique names/keys, notebook schema and declared canonical body SHA-256, template
references, and the declared source-factory mapping digest. Source-factory paths
are provenance metadata: their files outside the bundle are never read or claimed
verified. Hash serialization is compact UTF-8 JSON with recursively sorted object
keys, retained array order, and no trailing newline. The current template declares
no hash; its resolved dashboard body hash is computed and read back.

All list pages must finish; pagination exhaustion, repeated cursors, malformed
responses and duplicate names fail explicitly. Full `get` bodies must match
canonically before reuse. Any edited same-name resource refuses installation
before any create. Install creates missing notebooks first, resolves their fresh
IDs into the dashboard, then gets every resource again. It never updates, deletes,
or rolls back successful assets. IDs stay decimal strings.

The receipt has `ok`, `outcome` (`complete`, `incomplete`, `error`, `uncertain`),
`action`, `namespace` (after validation), and `assets`. Each asset has `kind`,
`name`, `id`, `body_sha256`, `state`, `url`. URL routes use the supplied base URL;
when a separate frontend serves the UI, retain the route and use that frontend's
origin. Missing dashboard hashes before notebook IDs exist describe the template.
`created_unverified` retains a known create ACK when readback fails. A lost ACK,
malformed create reply, or create transport failure is `uncertain`; inspect or
resume installation to discover exact existing resources before creating again.
An absolute request watchdog shuts down the captured socket at its deadline,
including trickled headers and repeated informational responses, and is joined
before the connection closes. No HTTP request is automatically retried. A decoded gateway `ok:false` rejection is an error; other failed create replies
remain conservatively uncertain.

One process-local asynchronous lock serializes admission, including cancellation
until the bounded worker finishes. **A single install writer across MCP processes
is required.** Gestalt offers no atomic create-by-name: cross-process atomic
idempotency cannot be promised. Existing duplicates always refuse ambiguity.

Limits are strict positive integers: booleans, unknown keys, coercions and values
above ceilings are rejected, never clamped. Defaults / ceilings:

| Limit | Default | Ceiling |
|---|---:|---:|
| max_resources (per kind scanned) | 4096 | 16384 |
| max_pages (per kind) | 128 | 1024 |
| page_size | 128 | 512 |
| max_file_bytes / max_body_bytes | 8388608 | 33554432 |
| max_total_bytes (bundle files read) | 33554432 | 134217728 |
| max_response_bytes | 16777216 | 67108864 |
| timeout_ms (each HTTP request/read) | 10000 | 60000 |

No recursive directory scan, shell, redirect following, SQLite shadow catalog,
market computation, or arbitrary notebook execution is involved. The notebook
code is opaque saved content. Run trusted cells separately through ordinary
`cell.execute` or the UI after checking installation receipts.
