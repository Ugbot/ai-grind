"""Tests for the issue providers: GitHub via httpx.MockTransport, registry, body."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from devtools_mcp.tracker import criteria, issues, tasks
from devtools_mcp.tracker.db import TrackerDB, TrackerError, open_tracker
from devtools_mcp.tracker.providers import get_provider
from devtools_mcp.tracker.providers.github import API_BASE, GitHubProvider


@pytest.fixture
def db(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "tracker.db")
    yield tracker
    tracker.close()


@pytest.fixture(autouse=True)
def fake_token(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_testtoken")


def _issue_payload(number: int = 7, state: str = "open", title: str = "t", body: str = "") -> dict:
    return {
        "number": number,
        "state": state,
        "title": title,
        "body": body,
        "html_url": f"https://github.com/o/r/issues/{number}",
    }


def _mock_client(handler) -> httpx.Client:
    return httpx.Client(base_url=API_BASE, transport=httpx.MockTransport(handler))


class TestGitHubProvider:
    def test_missing_token_is_error(self, monkeypatch):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.delenv("GH_TOKEN", raising=False)
        with pytest.raises(TrackerError, match="GITHUB_TOKEN"):
            GitHubProvider()

    def test_gh_token_fallback(self, monkeypatch):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.setenv("GH_TOKEN", "fallback")
        provider = GitHubProvider(client=_mock_client(lambda r: httpx.Response(200, json={})))
        assert provider._token == "fallback"

    def test_create_issue(self):
        seen: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["method"] = request.method
            seen["path"] = request.url.path
            seen["auth"] = request.headers["Authorization"]
            seen["body"] = json.loads(request.content)
            return httpx.Response(201, json=_issue_payload(42, title="my issue"))

        provider = GitHubProvider(client=_mock_client(handler))
        issue = provider.create_issue("o/r", "my issue", "body text", ["user-story"])
        assert seen["method"] == "POST"
        assert seen["path"] == "/repos/o/r/issues"
        assert seen["auth"] == "Bearer ghp_testtoken"
        assert seen["body"]["labels"] == ["user-story"]
        assert issue.ref_id == "42"
        assert issue.state == "open"
        assert issue.url.endswith("/issues/42")

    def test_get_update_close(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, json=_issue_payload(7, state="open"))
            assert request.method == "PATCH"
            payload = json.loads(request.content)
            return httpx.Response(200, json=_issue_payload(7, state=payload.get("state", "open")))

        provider = GitHubProvider(client=_mock_client(handler))
        assert provider.get_issue("o/r", "7").state == "open"
        assert provider.update_issue("o/r", "7", title="new").state == "open"
        assert provider.close_issue("o/r", "7").state == "closed"

    @pytest.mark.parametrize("status,fragment", [(401, "token"), (404, "not found"), (422, "validation")])
    def test_error_mapping(self, status, fragment):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status, json={"message": "nope"})

        provider = GitHubProvider(client=_mock_client(handler))
        with pytest.raises(TrackerError, match=fragment):
            provider.get_issue("o/r", "1")

    def test_bad_repo_rejected(self):
        provider = GitHubProvider(client=_mock_client(lambda r: httpx.Response(200, json={})))
        with pytest.raises(TrackerError, match="owner/name"):
            provider.create_issue("not a repo", "t", "b", [])

    def test_update_needs_a_field(self):
        provider = GitHubProvider(client=_mock_client(lambda r: httpx.Response(200, json={})))
        with pytest.raises(TrackerError, match="Nothing to update"):
            provider.update_issue("o/r", "1")


class TestProviderRegistry:
    def test_unknown_provider(self):
        with pytest.raises(TrackerError, match="Unknown provider"):
            get_provider("jira")

    def test_gitlab_stub(self):
        provider = get_provider("gitlab")
        with pytest.raises(NotImplementedError, match="github"):
            provider.create_issue("g/p", "t", "b", [])


class TestIssueLifecycle:
    def _seeded_task(self, db):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "Ship feature", description="The work.")
        met = criteria.add_criterion(db, task.id, "covered", test_ref="tests/t.py::a")
        criteria.record_result(db, met.id, "pass")
        criteria.add_criterion(db, task.id, "still open")
        return task

    def test_body_contains_checklist_and_footer(self, db):
        task = self._seeded_task(db)
        body = issues.build_issue_body(db, task)
        assert "The work." in body
        assert "- [x] covered (`tests/t.py::a`)" in body
        assert "- [ ] still open" in body
        assert task.key in body

    def test_create_stores_ref_and_rejects_duplicate(self, db):
        task = self._seeded_task(db)

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(201, json=_issue_payload(11))

        client = _mock_client(handler)
        issue = issues.create_issue_for_task(db, task.key, "github", "o/r", client=client)
        assert issue.ref_id == "11"
        ref = issues.get_ref(db, task.id, "github")
        assert ref is not None and ref.repo == "o/r" and ref.state == "open"
        with pytest.raises(TrackerError, match="already has"):
            issues.create_issue_for_task(db, task.key, "github", "o/r", client=client)

    def test_sync_reports_drift(self, db):
        task = self._seeded_task(db)
        state = {"value": "open"}
        remote: dict = {"body": ""}

        def handler(request: httpx.Request) -> httpx.Response:
            # Echo the created body back, as GitHub does — otherwise sync sees an
            # unmarked issue and self-heals, which is not what this test is about.
            if request.method in ("POST", "PATCH"):
                remote["body"] = json.loads(request.content).get("body", remote["body"])
            code = 201 if request.method == "POST" else 200
            return httpx.Response(code, json=_issue_payload(11, state=state["value"], body=remote["body"]))

        client = _mock_client(handler)
        issues.create_issue_for_task(db, task.key, "github", "o/r", client=client)
        _, no_drift = issues.sync_issue(db, task.key, "github", client=client)
        assert no_drift == []
        tasks.set_status(db, task.key, "done", override=True)
        _, drift = issues.sync_issue(db, task.key, "github", client=client)
        assert len(drift) == 1 and "remote issue is open" in drift[0]
        state["value"] = "closed"
        issue, _ = issues.sync_issue(db, task.key, "github", client=client)
        assert issue.state == "closed"
        assert issues.get_ref(db, task.id, "github").state == "closed"

    def test_close_external(self, db):
        task = self._seeded_task(db)

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return httpx.Response(201, json=_issue_payload(11))
            return httpx.Response(200, json=_issue_payload(11, state="closed"))

        client = _mock_client(handler)
        issues.create_issue_for_task(db, task.key, "github", "o/r", client=client)
        closed = issues.close_external_issue(db, task.key, "github", client=client)
        assert closed.state == "closed"

    def test_sync_without_ref_is_error(self, db):
        task = self._seeded_task(db)
        with pytest.raises(TrackerError, match="no github issue"):
            issues.sync_issue(db, task.key, "github")


UID_A = "a" * 32
UID_B = "b" * 32


class TestTaskMarker:
    def test_marker_roundtrips_key_and_uid(self):
        assert issues.parse_task_ref(issues.task_marker("GR-12", UID_A)) == ("GR-12", UID_A)

    def test_marker_is_case_normalised(self):
        assert issues.parse_task_ref(issues.task_marker("gr-12", UID_A.upper())) == ("GR-12", UID_A)

    def test_legacy_key_only_marker_still_parses(self):
        assert issues.parse_task_ref(issues.task_marker("GR-12")) == ("GR-12", None)

    def test_unmarked_body_parses_to_none(self):
        assert issues.parse_task_ref("just prose, tracked as GR-12 maybe") == (None, None)
        assert issues.parse_task_ref("") == (None, None)

    def test_marker_survives_surrounding_markdown(self):
        body = f"# Title\n\ntext\n\n{issues.task_marker('ABC-7', UID_A)}\n\nmore"
        assert issues.parse_task_ref(body) == ("ABC-7", UID_A)

    def test_strip_footer_removes_prose_and_marker(self):
        body = "Real content.\n\n" + issues.build_issue_footer("GR-12", UID_A)
        assert issues.strip_issue_footer(body) == "Real content."

    def test_strip_footer_is_idempotent_on_unmarked_body(self):
        assert issues.strip_issue_footer("Real content.") == "Real content."


class TestUidIsTheStableIdentity:
    """The key can be reassigned by a merge; the uid cannot. Resolution must
    follow the uid, which is the whole point of stamping it."""

    def _task(self, db):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "Ship feature")
        assert task.uid, "task created without a uid"
        return task

    def test_resolves_by_uid(self, db):
        task = self._task(db)
        body = issues.build_issue_footer(task.key, task.uid)
        assert issues.resolve_marked_task(db, body).id == task.id

    def test_resolution_follows_the_task_after_a_rekey(self, db):
        task = self._task(db)
        body = issues.build_issue_footer(task.key, task.uid)
        # Simulate crdt._resolve_key_collision handing this task a new key.
        with db.transaction() as conn:
            conn.execute("UPDATE tasks SET key = 'GR-999' WHERE uid = ?", (task.uid,))
        resolved = issues.resolve_marked_task(db, body)
        assert resolved is not None and resolved.id == task.id
        assert resolved.key == "GR-999", "should resolve to the task under its new key"

    def test_stale_key_does_not_resolve_to_a_different_task(self, db):
        """The failure a key-only marker allows: another task takes the key."""
        task = self._task(db)
        other, _ = tasks.create_task(db, "GR", "Unrelated")
        body = issues.build_issue_footer(task.key, task.uid)
        with db.transaction() as conn:
            conn.execute("UPDATE tasks SET key = 'GR-777' WHERE uid = ?", (task.uid,))
            conn.execute("UPDATE tasks SET key = ? WHERE uid = ?", (task.key, other.uid))
        resolved = issues.resolve_marked_task(db, body)
        assert resolved is not None and resolved.id == task.id, "uid must win over the recycled key"
        assert resolved.id != other.id

    def test_legacy_key_only_marker_falls_back_to_key(self, db):
        task = self._task(db)
        assert issues.resolve_marked_task(db, issues.task_marker(task.key)).id == task.id

    def test_unknown_uid_resolves_to_none(self, db):
        self._task(db)
        assert issues.resolve_marked_task(db, issues.task_marker("GR-1", UID_B)) is None


class TestSyncSelfHeals:
    def _task(self, db):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "Ship feature")
        return task

    def test_sync_restamps_a_legacy_key_only_marker(self, db):
        task = self._task(db)
        remote = {"body": f"Content.\n\n{issues.build_issue_footer(task.key)}"}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return httpx.Response(201, json=_issue_payload(11, body=remote["body"]))
            if request.method == "PATCH":
                remote["body"] = json.loads(request.content)["body"]
            return httpx.Response(200, json=_issue_payload(11, body=remote["body"]))

        client = _mock_client(handler)
        issues.create_issue_for_task(db, task.key, "github", "o/r", client=client)
        remote["body"] = f"Content.\n\n{issues.build_issue_footer(task.key)}"  # legacy state
        _, notes = issues.sync_issue(db, task.key, "github", client=client)
        assert any("stamped the canonical uid marker" in n for n in notes)
        assert issues.parse_task_ref(remote["body"]) == (task.key, task.uid)
        assert "Content." in remote["body"]

    def test_sync_does_not_rewrite_an_already_correct_marker(self, db):
        task = self._task(db)
        good = f"Content.\n\n{issues.build_issue_footer(task.key, task.uid)}"

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return httpx.Response(201, json=_issue_payload(11, body=good))
            assert request.method == "GET", "correct marker must not trigger a PATCH"
            return httpx.Response(200, json=_issue_payload(11, body=good))

        client = _mock_client(handler)
        issues.create_issue_for_task(db, task.key, "github", "o/r", client=client)
        _, notes = issues.sync_issue(db, task.key, "github", client=client)
        assert notes == []


class TestAdopt:
    def _seeded_task(self, db):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "Ship feature", description="The work.")
        return task

    def test_body_carries_parseable_marker(self, db):
        task = self._seeded_task(db)
        assert issues.parse_task_key(issues.build_issue_body(db, task)) == task.key

    def test_adopt_stamps_unmarked_issue_and_stores_ref(self, db):
        task = self._seeded_task(db)
        patched: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "PATCH":
                patched["body"] = json.loads(request.content)["body"]
                return httpx.Response(200, json=_issue_payload(9, body=patched["body"]))
            return httpx.Response(200, json=_issue_payload(9, body="hand-written issue"))

        issue, stamped = issues.adopt_issue(db, task.key, "github", "o/r", "9", client=_mock_client(handler))
        assert stamped is True
        assert issues.parse_task_key(patched["body"]) == task.key
        assert "hand-written issue" in patched["body"], "adopt must preserve the original body"
        ref = issues.get_ref(db, task.id, "github")
        assert ref is not None and ref.ref_id == "9" and ref.repo == "o/r"
        assert issue.ref_id == "9"

    def test_adopt_is_idempotent_on_already_marked_issue(self, db):
        task = self._seeded_task(db)

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET", "already-marked issue must not be PATCHed"
            return httpx.Response(200, json=_issue_payload(9, body=issues.task_marker(task.key, task.uid)))

        _, stamped = issues.adopt_issue(db, task.key, "github", "o/r", "9", client=_mock_client(handler))
        assert stamped is False
        assert issues.get_ref(db, task.id, "github") is not None

    def test_adopt_refuses_issue_claimed_by_another_uid(self, db):
        task = self._seeded_task(db)

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=_issue_payload(9, body=issues.task_marker("GR-999", "f" * 32)))

        with pytest.raises(TrackerError, match="already marked for uid"):
            issues.adopt_issue(db, task.key, "github", "o/r", "9", client=_mock_client(handler))
        assert issues.get_ref(db, task.id, "github") is None, "rejected adopt must not store a ref"

    def test_adopt_accepts_issue_marked_with_our_uid_under_a_stale_key(self, db):
        """After a re-key the marker's key is stale but the uid still matches."""
        task = self._seeded_task(db)

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET", "uid already matches; no PATCH needed"
            return httpx.Response(200, json=_issue_payload(9, body=issues.task_marker("GR-404", task.uid)))

        _, stamped = issues.adopt_issue(db, task.key, "github", "o/r", "9", client=_mock_client(handler))
        assert stamped is False
        assert issues.get_ref(db, task.id, "github") is not None

    def test_adopt_upgrades_legacy_key_only_marker_to_carry_uid(self, db):
        task = self._seeded_task(db)
        patched: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "PATCH":
                patched["body"] = json.loads(request.content)["body"]
                return httpx.Response(200, json=_issue_payload(9, body=patched["body"]))
            legacy = f"Original text.\n\n{issues.build_issue_footer(task.key)}"
            return httpx.Response(200, json=_issue_payload(9, body=legacy))

        _, stamped = issues.adopt_issue(db, task.key, "github", "o/r", "9", client=_mock_client(handler))
        assert stamped is True
        assert issues.parse_task_ref(patched["body"]) == (task.key, task.uid)
        assert "Original text." in patched["body"]
        assert patched["body"].count("in the devtools-mcp tracker") == 1, "legacy footer must be replaced, not stacked"

    def test_adopt_rejects_duplicate_link(self, db):
        task = self._seeded_task(db)
        handler = lambda r: httpx.Response(  # noqa: E731
            201 if r.method == "POST" else 200, json=_issue_payload(11, body=issues.task_marker(task.key, task.uid))
        )
        client = _mock_client(handler)
        issues.create_issue_for_task(db, task.key, "github", "o/r", client=client)
        with pytest.raises(TrackerError, match="already has"):
            issues.adopt_issue(db, task.key, "github", "o/r", "11", client=client)

    def test_adopt_needs_repo_and_ref_id(self, db):
        task = self._seeded_task(db)
        with pytest.raises(TrackerError, match="repo is required"):
            issues.adopt_issue(db, task.key, "github", "", "9")
        with pytest.raises(TrackerError, match="ref_id is required"):
            issues.adopt_issue(db, task.key, "github", "o/r", "  ")

    def test_adopted_issue_then_syncs(self, db):
        task = self._seeded_task(db)
        marked = issues.task_marker(task.key, task.uid)

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=_issue_payload(9, state="closed", body=marked))

        client = _mock_client(handler)
        issues.adopt_issue(db, task.key, "github", "o/r", "9", client=client)
        issue, drift = issues.sync_issue(db, task.key, "github", client=client)
        assert issue.state == "closed"
        assert len(drift) == 1 and "remote issue is closed" in drift[0]
