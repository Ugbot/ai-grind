---
name: station-sync
description: >
  Sync the local devtools-mcp tracker with an llm-station-remote platform
  (the multi-tenant agent+human collaboration backend) via the station_link /
  station_sync / station_session MCP tools — including how to get the user
  authenticated through the browser. Use when asked to "connect to the
  platform", "sync tasks to the station/server", set up station.toml, push
  claims/skills/perf runs, coordinate sessions or handoffs across members, or
  whenever a station tool answers "Not authenticated". For peer-to-peer CRDT
  replica sync between machines use tracker_sync instead.
---

# Station sync (devtools-mcp ⇄ llm-station-remote)

The platform (FastAPI + Postgres, default `http://localhost:8000`) is a
remote backend for the local tracker. **Local SQLite stays the source of
authority**; `station_sync` pushes/pulls over REST. Offline is a normal
state — a failed run fails fast and the next run re-diffs; nothing queues.

## Authenticating the user (do this first, and whenever a tool says "Not authenticated")

The key resolution order is: env `LLM_STATION_API_KEY` → the browser-auth
credential store (`~/.devtools-mcp/station-auth.json`). When neither is set,
every station tool fails with instructions. **Relay them to the user; you
cannot authenticate for them.** The flow:

1. Make sure the dashboard is running — call the `devtools_dashboard` tool
   (or the shared service already serves it on `:8765`).
2. Tell the user to open **`http://127.0.0.1:8765/station/auth`** in their
   browser, check the platform URL shown, and click **Sign in with GitHub**
   or **Sign in with Google**.
3. The platform completes OAuth, mints an `lls_` API key, and redirects it
   back to the local page, which stores it. The page confirms "Connected —
   tell your agent to retry."
4. Retry the station tool. `station_link action='auth'` reprints the
   instructions and shows current auth state; `action='logout'` clears the
   stored credential.

Fallbacks: the same page has a paste-a-key form, and the env var always
wins (useful for CI/agent identities minted via the platform's
`/orgs/{org}/tokens` admin API).

## Setup (once per repo)

1. `station_link action='init'` — writes a commented
   `.devtools-mcp/station.toml` into the repo. Rules live in that file, per
   project: which domains sync (`tasks/sessions/collab/skills/perf`),
   `direction` (push/pull/both), conflict policy. Keys never go in the file
   (it is rejected if one is found).
2. Edit the TOML: set `[project].local` to the tracker project key (e.g.
   `GRIND`) and enable domains.
3. `station_link action='link'` — validates online (auth, org membership,
   project, repo) and caches the resolved ids. Sync refuses to run unlinked
   or after the TOML changed (re-link).

## Day-to-day

- `station_sync` — sync all enabled domains; `domain='tasks'` for one;
  `dry_run=True` to preview after rule changes.
- `station_sync` conflict model: tasks are row-level **local-wins** (the
  platform copy is overwritten; set `on_conflict = "remote_wins"` to flip).
  Local deletes become remote `status=cancelled`. Pulled platform tasks get
  **fresh local keys** — `GRIND-19` here and `GRIND-19` there are unrelated;
  the link table is the identity.
- `station_session` — live coordination: `start`/`update` a work session,
  `handoff` work to other members, `inbox`/`accept`/`decline` handoffs,
  `context` for the platform's onboarding packet.
- `station_link action='status'` — auth state, links, per-rule watermarks
  and errors. Rules auto-pause after 10 consecutive failures;
  `action='resume' domain='tasks'` un-pauses.
- Local file claims (tracker_files) become advisory platform checkouts;
  other members' checkouts appear in `tracker_files action='conflicts'`.

## Troubleshooting

- "Not authenticated" → the auth flow above.
- "Stored credential is for <other url>" → the user authenticated against a
  different platform; `station_link action='logout'` then re-auth.
- "not linked / config changed" → `station_link action='link'`.
- "rule auto-paused" → fix the cause (see last_error in status), then
  `station_link action='resume'`.
- Platform down → runs fail fast and cleanly; just retry later.
