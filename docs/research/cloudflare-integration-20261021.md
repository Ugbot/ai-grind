# Cloudflare integration qualification

2026-10-21, macOS arm64, existing Python 3.12.13 UV environment.

## Delivered

The library now includes 19 official Cloudflare skills, the authored
cloudflare-control skill and a Cloudflare Controller agent. The committed plugin
has 130 skills and 6 Markdown agents. Native Codex TOML remains a bundled runner
asset; publication does not register that role or install a scheduler. Official
MCP/skills activation is separate from this repository publication.

The helper uses only the Python standard library, is invoked with UV, and uses
bounded async subprocess I/O. It emits secret-free S3 configuration or safe
official CLI status. It implements no OAuth, credential store or product API.
Live cf status returned authenticated=true. Offline configuration correctly
reported remote_verified=false; stored credentials are not inspected.

## Qualification and publication

Final focused execution: 63 passed in 1.28s. Ruff and Black checks pass; the scoped
new source/prose check has zero findings. Repository-wide prose check remains
red with 45 findings in existing content. No general repository-wide test pass
is claimed.

The first focused run found a real capped-output subprocess cleanup deadlock
(1 failed, 45 passed). The fix cancels/joins original readers before bounded
discard readers drain killed pipes during reaping. Final stdout/stderr/both
flood, timeout and cancellation checks pass, with unraisable cleanup warnings
treated as errors.

Publication review also corrected linked output roots/ancestors and linked
manifest preflight, deterministic metadata budget failures after mutation,
preparse byte limits, and portable per-user source origins. Focused synthetic
fixtures cover those refusals, conflict preflight, partial failure resume and
unchanged default routes.

Only selected additive modes ran: 19 catalog sources, 21 plugin assets, 21 local
assets and 20 skills-only .agents assets. Every one of 749 existing output files
and 125 prior manifest items remained unchanged. The manifest now has 144 items.
Repeating all four selected publications changed no output or manifest bytes.
The earlier local/.agents mirrors retained their preceding unrelated content;
this was not a full rebuild. No project/global sync ran.

The exact Apache2.0 license was saved under skills/vendor-licenses/cloudflare.
THIRD_PARTY_SKILLS.md records its upstream revision and digest. Official skill
folders remain verbatim. Git whitespace checks flag upstream whitespace in20
vendor files; authored changes pass their scoped whitespace check. Do not run
full harvest on this checkout while its
legacy Windows sources are absent.

## Evidence and next work

- cloudflare-integration-20261021.json records source bindings, actual mirror
  counts, preservation, idempotence and qualification results.
- cloudflare-integration-20261021.xml is the final focused pytest report.
- The actual native SST R2 transport check belongs to Gestalt2 commit 2254bc24,
  not to this helper's readiness claim. It passed four cases including real
  multipart geometry, conditional refusal/abort, full native values and cleanup.

Windows/Linux native subprocess behavior, native Codex role registration and
full remote Iceberg/catalog/proxy/cache acceptance remain separate work.
