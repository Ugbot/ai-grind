"""External issue lifecycle: create-from-task, adopt-existing, sync state, close.

The remote issue body is generated from the task: description, the acceptance
criteria as a checklist, and a back-reference footer. One external ref per
(task, provider) — the external_refs primary key.

The footer carries a machine-readable marker as well as the human sentence:

    <!-- devtools-mcp:task=PROJ-123 uid=<32-hex> -->

**The uid is the identity; the key is only a label.** A merge that resolves a
concurrent PROJ-123 collision re-keys one of the tasks (see
`crdt._resolve_key_collision`), so a key recorded in an external system can go
stale or, worse, come to name a *different* task. The uid is never reassigned.
Resolution therefore prefers the uid and falls back to the key only for markers
written before uids were stamped.

The marker is what makes the link survive things the local DB cannot: a rebuilt
tracker, a fresh CRDT replica (`external_refs` is site-local, not synced), or an
issue a human opened by hand. `adopt` uses it to attach an existing remote issue.
"""

from __future__ import annotations

import re

from devtools_mcp.tracker import criteria as criteria_mod
from devtools_mcp.tracker import tags as tags_mod
from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso
from devtools_mcp.tracker.models import ExternalRef, Task
from devtools_mcp.tracker.providers import get_provider
from devtools_mcp.tracker.providers.base import ExternalIssue
from devtools_mcp.tracker.tasks import get_task, get_task_by_uid

BODY_CRITERIA_MAX: int = 50
MARKER_RE = re.compile(r"<!--\s*devtools-mcp:task=([A-Z][A-Z0-9]*-\d+)(?:\s+uid=([0-9a-f]{32}))?\s*-->")


def task_marker(task_key: str, uid: str | None = None) -> str:
    """The machine-readable back-reference embedded in a remote issue body.

    `uid` is the stable identity and should always be supplied for real tasks;
    it is optional only so the marker can be constructed in tests and for
    legacy key-only bodies.
    """
    assert task_key, "empty task key"
    suffix = f" uid={uid.strip().lower()}" if uid else ""
    marker = f"<!-- devtools-mcp:task={task_key.upper()}{suffix} -->"
    parsed_key, parsed_uid = parse_task_ref(marker)
    assert parsed_key == task_key.upper(), f"marker not parseable: {marker!r}"
    assert not uid or parsed_uid == uid.strip().lower(), f"marker dropped the uid: {marker!r}"
    return marker


def parse_task_ref(body: str) -> tuple[str | None, str | None]:
    """(key, uid) stamped in a remote issue body; either may be None."""
    match = MARKER_RE.search(body or "")
    if match is None:
        return None, None
    return match.group(1), match.group(2)


def parse_task_key(body: str) -> str | None:
    """The task key stamped in a remote issue body, or None if unmarked."""
    return parse_task_ref(body)[0]


def resolve_marked_task(db: TrackerDB, body: str) -> Task | None:
    """The task a remote issue body points at, preferring the stable uid.

    Returns None when the body carries no marker, or when the marker names a
    task this replica does not have.
    """
    key, uid = parse_task_ref(body)
    if uid:
        try:
            return get_task_by_uid(db.conn, uid)
        except TrackerError:
            return None  # uid wins, but this replica has not seen that task
    if key:
        try:
            return get_task(db.conn, key)
        except TrackerError:
            return None
    return None


FOOTER_RE = re.compile(
    r"\n*(?:^---\s*$\n)?^\*Tracked as `[A-Z][A-Z0-9]*-\d+` in the devtools-mcp tracker\.\*\s*$\n?",
    re.MULTILINE,
)


def build_issue_footer(task_key: str, uid: str | None = None) -> str:
    """Provenance footer: human sentence plus the parseable marker."""
    return f"---\n*Tracked as `{task_key}` in the devtools-mcp tracker.*\n{task_marker(task_key, uid)}"


def strip_issue_footer(body: str) -> str:
    """Remove any tracker footer/marker so a fresh one can replace it.

    Used when upgrading a legacy key-only marker in place — without this the
    replacement footer would stack on top of the old one.
    """
    return FOOTER_RE.sub("", MARKER_RE.sub("", body or "")).rstrip()


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
    parts.append(build_issue_footer(task.key, task.uid))
    body = "\n".join(parts)
    assert parse_task_ref(body) == (task.key, task.uid), "footer lost the task marker"
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


def adopt_issue(
    db: TrackerDB,
    task_key: str,
    provider_name: str,
    repo: str,
    ref_id: str,
    token: str | None = None,
    client=None,
) -> tuple[ExternalIssue, bool]:
    """Link an existing remote issue to a task; returns (issue, stamped).

    For issues opened by hand or by another replica. The remote issue is
    fetched to prove it exists, the external ref is stored, and the marker is
    appended to the remote body when absent so the link is recoverable from the
    issue alone. Refuses to hijack an issue already marked for a different task.
    """
    if not repo or not repo.strip():
        raise TrackerError("repo is required (e.g. 'owner/name')")
    if not str(ref_id).strip():
        raise TrackerError("ref_id is required (the remote issue number)")
    task = get_task(db.conn, task_key)
    existing = get_ref(db, task.id, provider_name)
    if existing is not None:
        raise TrackerError(
            f"{task.key} already has a {provider_name} issue: {existing.url} (use action='sync' to refresh it)"
        )
    provider = get_provider(provider_name, token=token, client=client)
    issue = provider.get_issue(repo.strip(), str(ref_id).strip())
    claimed_key, claimed_uid = parse_task_ref(issue.body)
    # Compare on uid when the marker has one: a key can be reassigned by a merge,
    # so key equality is neither necessary nor sufficient to prove same-task.
    if claimed_uid is not None and claimed_uid != task.uid:
        raise TrackerError(
            f"{provider_name} issue #{issue.ref_id} is already marked for uid {claimed_uid} "
            f"(`{claimed_key}`), not `{task.key}`"
        )
    if claimed_uid is None and claimed_key is not None and claimed_key != task.key:
        raise TrackerError(
            f"{provider_name} issue #{issue.ref_id} is already marked for `{claimed_key}`, not `{task.key}`"
        )
    stamped = False
    if claimed_uid is None:
        # Covers both unmarked bodies and legacy key-only markers, which get
        # upgraded in place to carry the stable uid.
        footer = build_issue_footer(task.key, task.uid)
        stripped = strip_issue_footer(issue.body)
        marked = f"{stripped}\n\n{footer}" if stripped else footer
        issue = provider.update_issue(issue.repo, issue.ref_id, body=marked)
        stamped = True
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO external_refs (task_id, provider, ref_id, repo, url, state, "
            "last_synced) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (task.id, issue.provider, issue.ref_id, issue.repo, issue.url, issue.state, utc_now_iso()),
        )
    assert get_ref(db, task.id, provider_name) is not None, "adopt did not store the ref"
    return issue, stamped


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
    notes: list[str] = []
    # Self-heal: an issue created before uids were stamped (or edited by a human
    # who dropped the footer) gets the canonical marker put back, so the link
    # stays recoverable from the issue alone.
    if parse_task_ref(issue.body)[1] != task.uid:
        footer = build_issue_footer(task.key, task.uid)
        stripped = strip_issue_footer(issue.body)
        issue = provider.update_issue(
            issue.repo, issue.ref_id, body=f"{stripped}\n\n{footer}" if stripped else footer
        )
        notes.append(f"stamped the canonical uid marker into {provider_name} #{issue.ref_id}")
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
    return issue, drift + notes


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
