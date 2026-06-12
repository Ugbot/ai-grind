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


def _issue_payload(number: int = 7, state: str = "open", title: str = "t") -> dict:
    return {
        "number": number,
        "state": state,
        "title": title,
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

        def handler(request: httpx.Request) -> httpx.Response:
            code = 201 if request.method == "POST" else 200
            return httpx.Response(code, json=_issue_payload(11, state=state["value"]))

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
