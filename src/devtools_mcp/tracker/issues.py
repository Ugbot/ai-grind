"""External issue lifecycle: create-from-task, sync state, close.

The remote issue body is generated from the task: description, the acceptance
criteria as a checklist, and a back-reference footer. One external ref per
(task, provider) — the external_refs primary key.
"""

from __future__ import annotations

from devtools_mcp.tracker import criteria as criteria_mod
from devtools_mcp.tracker import tags as tags_mod
from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso
from devtools_mcp.tracker.models import ExternalRef, Task
from devtools_mcp.tracker.providers import get_provider
from devtools_mcp.tracker.providers.base import ExternalIssue
from devtools_mcp.tracker.tasks import get_task

BODY_CRITERIA_MAX: int = 50


def build_issue_body(db: TrackerDB, task: Task) -> str:
    """Markdown issue body: description + criteria checklist + tracker footer."""
    assert task.id > 0, "task must be persisted"
    parts: list[str] = []
    if task.description:
        parts += [task.description, ""]
    criteria = criteria_mod.list_criteria(db.conn, task.id)
    if criteria:
        parts.append("## Acceptance criteria")
        for criterion in criteria[:BODY_CRITERIA_MAX]:
            mark = "x" if criterion.is_met else " "
            ref = f" (`{criterion.test_ref}`)" if criterion.test_ref else ""
            parts.append(f"- [{mark}] {criterion.text}{ref}")
        if len(criteria) > BODY_CRITERIA_MAX:
            parts.append(f"- ... {len(criteria) - BODY_CRITERIA_MAX} more")
        parts.append("")
    parts.append(f"---\n*Tracked as `{task.key}` in the devtools-mcp tracker.*")
    body = "\n".join(parts)
    assert task.key in body, "footer lost the task key"
    return body


def get_ref(db: TrackerDB, task_id: int, provider: str) -> ExternalRef | None:
    """The stored external ref for (task, provider), if any."""
    assert task_id > 0, f"bad task_id {task_id}"
    row = db.conn.execute(
        "SELECT * FROM external_refs WHERE task_id = ? AND provider = ?",
        (task_id, provider),
    ).fetchone()
    return ExternalRef.from_row(row) if row is not None else None


def create_issue_for_task(
    db: TrackerDB,
    task_key: str,
    provider_name: str,
    repo: str,
    token: str | None = None,
    client=None,
) -> ExternalIssue:
    """Create a remote issue from a task and store the external ref."""
    if not repo or not repo.strip():
        raise TrackerError("repo is required (e.g. 'owner/name')")
    task = get_task(db.conn, task_key)
    existing = get_ref(db, task.id, provider_name)
    if existing is not None:
        raise TrackerError(
            f"{task.key} already has a {provider_name} issue: {existing.url} " "(use action='sync' to refresh it)"
        )
    provider = get_provider(provider_name, token=token, client=client)
    labels = tags_mod.tags_for_task(db.conn, task.id)
    issue = provider.create_issue(repo.strip(), task.title, build_issue_body(db, task), labels)
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO external_refs (task_id, provider, ref_id, repo, url, state, "
            "last_synced) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (task.id, issue.provider, issue.ref_id, issue.repo, issue.url, issue.state, utc_now_iso()),
        )
    assert issue.ref_id, "provider returned issue without ref_id"
    return issue


def sync_issue(
    db: TrackerDB,
    task_key: str,
    provider_name: str,
    token: str | None = None,
    client=None,
) -> tuple[ExternalIssue, list[str]]:
    """Pull remote issue state; returns (issue, drift notes vs local status)."""
    task = get_task(db.conn, task_key)
    ref = get_ref(db, task.id, provider_name)
    if ref is None:
        raise TrackerError(f"{task.key} has no {provider_name} issue (use action='create')")
    provider = get_provider(provider_name, token=token, client=client)
    issue = provider.get_issue(ref.repo, ref.ref_id)
    with db.transaction() as conn:
        conn.execute(
            "UPDATE external_refs SET state = ?, url = ?, last_synced = ? " "WHERE task_id = ? AND provider = ?",
            (issue.state, issue.url, utc_now_iso(), task.id, provider_name),
        )
    drift: list[str] = []
    local_closed = task.status in ("done", "cancelled")
    remote_closed = issue.state == "closed"
    if local_closed and not remote_closed:
        drift.append(f"local {task.key} is {task.status} but remote issue is open")
    if remote_closed and not local_closed:
        drift.append(f"remote issue is closed but local {task.key} is {task.status}")
    assert len(drift) <= 1, "drift cannot point both ways"
    return issue, drift


def close_external_issue(
    db: TrackerDB,
    task_key: str,
    provider_name: str,
    token: str | None = None,
    client=None,
) -> ExternalIssue:
    """Close the remote issue linked to a task and record the new state."""
    task = get_task(db.conn, task_key)
    ref = get_ref(db, task.id, provider_name)
    if ref is None:
        raise TrackerError(f"{task.key} has no {provider_name} issue to close")
    provider = get_provider(provider_name, token=token, client=client)
    issue = provider.close_issue(ref.repo, ref.ref_id)
    with db.transaction() as conn:
        conn.execute(
            "UPDATE external_refs SET state = ?, last_synced = ? " "WHERE task_id = ? AND provider = ?",
            (issue.state, utc_now_iso(), task.id, provider_name),
        )
    assert issue.state == "closed", f"close returned state {issue.state!r}"
    return issue
