# Official access and bounded R2 operations

## Setup and client activation

The canonical setup is [Cloudflare's agent prompt](https://developers.cloudflare.com/agent-setup/prompt.md).
Follow its section for the intended client, preserve unrelated configuration,
and check actual per-client installation results. For Codex the prompt installs
`cloudflare/skills` globally, adds `https://mcp.cloudflare.com/mcp`, then invokes
`codex mcp login cloudflare`. A new session is needed to expose new skills/tools.
An unsupported message for an unrelated client does not invalidate a successful
Codex installation. The optional beta cf CLI is installed only when requested.

Ai-grind harvests official skills verbatim through explicit `sources.toml`
entries. `cloudflare-control` supplies orchestration, not replacement product
skills. Upstream: [Cloudflare skills](https://github.com/cloudflare/skills),
Apache-2.0. Root official setup does not authenticate another machine; repeat
that machine's official flow and record its own readiness evidence.

## Authentication boundaries

| Surface | Official status and reauth | What it proves |
| --- | --- | --- |
| Cloudflare MCP | MCP's own identity/status tools; client-managed OAuth | That MCP connection's permitted API access |
| cf | `cf auth whoami`; `cf auth login` | cf's own credentials; require `authenticated` and `tokenValid` |
| Wrangler | `wrangler whoami --json`; `wrangler login` | Wrangler's own access, including official refresh |
| R2 S3 API | Scoped S3 access key/secret and optional session token, supplied through protected environment | Only the S3 operations actually verified |

Do not inspect or copy OS-keyring/config secrets. Do not use `auth token`
commands to emit credentials. Reauth belongs to the official tool; maintain
existing session permissions and resource ownership. OAuth scopes can be large;
return safe identity/account summaries instead of dumping raw status output.

The bundled helper supports an explicit cf or Wrangler status check. It omits
email, scopes, auth-store paths and unknown fields. Missing/failed authentication
is an error even if an official beta CLI exits zero with `authenticated:false`.
MCP and S3 readiness stay `not_checked` in a CLI-only receipt.

## Account and R2 workflow

1. Read the current official MCP/CLI schema and list accounts, bounded to100
   records per page and10 pages per task. Return the next cursor if more remain.
   Require an explicitly chosen account before resource operations; never select
   the first returned account silently.
2. Discover bucket list/info syntax with that installed tool. For cf0.15.0,
   actual `--help` and `cf schema` work while `cf cli search` is unavailable.
   Upstream docs can move ahead of beta releases. Wrangler4.135.0 has structured
   `whoami --json` and `r2 bucket info <name> --json`; bucket list is labelled
   text, and whoami rejects `--profile`. Do not invent unsupported JSON/profile
   flags or treat table parsing as authoritative structured API data.
3. Read bucket state before an authorized change. Prepare exact account, name,
   jurisdiction, location/storage parameters and expected action; keep unrelated
   buckets intact. Execute the requested operation once through the official
   tool, then read it back. A delete that prints `Aborted.` may exit zero: verify
   actual state rather than inferring success from an exit code.
4. Separate control-plane auth from S3 data-plane setup. Use Cloudflare's
   [R2 token guidance](https://developers.cloudflare.com/r2/api/tokens/) and
   [S3 setup](https://developers.cloudflare.com/r2/get-started/s3/). Never ask for
   secret values in chat or embed them in a receipt. Scope credentials to the
   required buckets/actions using the official provisioning flow.
5. The prepared S3 endpoint is `https://<account>.r2.cloudflarestorage.com`,
   region `auto`; EU jurisdiction uses `<account>.eu.r2.cloudflarestorage.com`.
   Bucket location such as WEUR is independent of jurisdiction. A prepared
   configuration or boolean credential-presence check does not establish
   successful HEAD, PUT, multipart, range, cleanup or application behavior.

Sources: [official MCP](https://github.com/cloudflare/mcp),
[cf agent guidance](https://developers.cloudflare.com/cf/agents/index.md),
[cf CLI skill](https://github.com/cloudflare/skills/tree/main/skills/cf-cli),
[Wrangler general commands](https://developers.cloudflare.com/workers/wrangler/commands/general/),
[Wrangler R2](https://developers.cloudflare.com/workers/wrangler/commands/r2/).
