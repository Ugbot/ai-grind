---
name: tracker-github-sync
description: >
  Bridge devtools-mcp tracker tasks to GitHub issues and git history: create a
  GitHub issue from a task (criteria checklist included), sync remote state and
  spot drift, close remote issues, and auto-link commits by putting task keys
  in commit messages. Use when a tracked task needs a public issue, when
  reconciling tracker vs GitHub state, or when wiring commits to tasks.
---

# GitHub sync and commit linking

## Auth

Set `GITHUB_TOKEN` (or `GH_TOKEN`) in the environment the MCP server runs in,
a fine-grained token with issue write access to the target repo, or a classic
token with `repo` scope. No token is stored by the tracker. (If you use the
`gh` CLI: `$env:GITHUB_TOKEN = gh auth token`.)

## Task → issue

```
tracker_issue(action="create", key="GRIND-7", repo="owner/name")
```

Creates the issue with:
- title = task title,
- body = task description + the acceptance criteria as a markdown checklist
  (`- [x]` met / `- [ ]` open, with test refs) + a `Tracked as GRIND-7` footer,
- labels = the task's tags,
and stores the ref (number, URL, state) on the task. One issue per
(task, provider); re-creating is rejected, so sync instead.

## Sync and drift

```
tracker_issue(action="sync", key="GRIND-7")
```

Pulls remote state, stamps `last_synced`, and reports **drift**: local task
done/cancelled while the remote issue is still open, or the remote closed
while the local task isn't. Resolve drift deliberately: close the remote
(`action="close"`) or update the local status; the tool never auto-changes
either side.

`provider="gitlab"` is reserved (the interface exists; calls return a clear
not-implemented error).

## Commits → tasks

Two ways to link commit hashes:

- **Convention + scan** (preferred): put the task key in the commit message
  (`git commit -m "GRIND-7: implement tag filter"`), then
  `tracker_commits(action="scan", repo="C:/path/to/repo")`. The scan reads
  `git log` (default last 500, `max_commits=` up to 5000), links every commit
  mentioning a known task key, and is idempotent, so re-scan freely. Keys whose
  project or task doesn't exist are counted and skipped, never errors.
- **Manual**: `tracker_commits(action="link", key="GRIND-7", repo="…",
  commit="<hash>", message="optional snippet")`.

Linked commits appear in `tracker_task(action="get", key=…)` and
`tracker_query(view="commits", project=…)`.

## End-of-feature checklist

1. `tracker_commits(action="scan", repo=".")` sweeps the branch's commits.
2. `tracker_criteria` records all green → `tracker_status(… status="done")`.
3. `tracker_issue(action="close", key=…)` or let `sync` flag the drift.
