"""GitHub issue provider over the REST v3 API.

Auth: GITHUB_TOKEN, then GH_TOKEN, from the environment — or an explicit token.
An injectable httpx.Client (e.g. with MockTransport) makes this testable
without the network.
"""

from __future__ import annotations

import contextlib
import os
import re

import httpx

from devtools_mcp.tracker.db import TrackerError
from devtools_mcp.tracker.providers.base import ExternalIssue, IssueProvider

API_BASE: str = "https://api.github.com"
REPO_RE = re.compile(r"^[\w.-]+/[\w.-]+$")
TIMEOUT_SECONDS: float = 15.0

_STATUS_HINTS: dict[int, str] = {
    401: "bad or expired token (check GITHUB_TOKEN)",
    403: "forbidden — token lacks 'repo'/'issues' scope or rate limit hit",
    404: "repo or issue not found (or token cannot see it)",
    410: "issues are disabled on this repository",
    422: "validation failed — check title/labels",
}


def resolve_token(explicit: str | None = None) -> str:
    """Token from argument, GITHUB_TOKEN, or GH_TOKEN; TrackerError if absent."""
    token = explicit or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    token = token.strip()
    if not token:
        raise TrackerError("No GitHub token: set GITHUB_TOKEN (or GH_TOKEN) in the environment")
    assert "\n" not in token, "token contains a newline"
    return token


class GitHubProvider(IssueProvider):
    name = "github"

    def __init__(self, token: str | None = None, client: httpx.Client | None = None) -> None:
        self._token = resolve_token(token)
        self._client = client or httpx.Client(base_url=API_BASE, timeout=TIMEOUT_SECONDS)
        assert self._client is not None, "no http client"

    def _headers(self) -> dict[str, str]:
        assert self._token, "provider constructed without token"
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _request(self, method: str, path: str, json_body: dict | None = None) -> dict:
        """One API call with explicit error mapping; returns the parsed body."""
        assert path.startswith("/repos/"), f"unexpected API path {path!r}"
        try:
            response = self._client.request(method, path, json=json_body, headers=self._headers())
        except httpx.HTTPError as exc:
            raise TrackerError(f"GitHub request failed: {exc}") from exc
        if response.status_code >= 400:
            hint = _STATUS_HINTS.get(response.status_code, "unexpected error")
            detail = ""
            with contextlib.suppress(ValueError):
                detail = response.json().get("message", "")
            raise TrackerError(
                f"GitHub {method} {path} -> {response.status_code}: {hint}" + (f" ({detail})" if detail else "")
            )
        body = response.json()
        assert isinstance(body, dict), f"expected JSON object, got {type(body)}"
        return body

    @staticmethod
    def _check_repo(repo: str) -> str:
        repo = repo.strip()
        if not REPO_RE.match(repo):
            raise TrackerError(f"Bad repo {repo!r}: expected 'owner/name'")
        return repo

    def _to_issue(self, repo: str, body: dict) -> ExternalIssue:
        assert "number" in body, f"issue payload missing number: {sorted(body)[:10]}"
        return ExternalIssue(
            provider=self.name,
            ref_id=str(body["number"]),
            repo=repo,
            url=body.get("html_url", ""),
            state=body.get("state", "unknown"),
            title=body.get("title", ""),
        )

    def create_issue(self, repo: str, title: str, body: str, labels: list[str]) -> ExternalIssue:
        repo = self._check_repo(repo)
        if not title.strip():
            raise TrackerError("Issue title must not be empty")
        payload: dict = {"title": title, "body": body}
        if labels:
            payload["labels"] = labels[:20]  # GitHub caps labels; stay bounded
        data = self._request("POST", f"/repos/{repo}/issues", payload)
        return self._to_issue(repo, data)

    def get_issue(self, repo: str, ref_id: str) -> ExternalIssue:
        repo = self._check_repo(repo)
        assert ref_id, "empty issue ref_id"
        data = self._request("GET", f"/repos/{repo}/issues/{ref_id}")
        return self._to_issue(repo, data)

    def update_issue(
        self,
        repo: str,
        ref_id: str,
        title: str | None = None,
        body: str | None = None,
        state: str | None = None,
    ) -> ExternalIssue:
        repo = self._check_repo(repo)
        if title is None and body is None and state is None:
            raise TrackerError("Nothing to update: pass title, body, or state")
        if state is not None and state not in ("open", "closed"):
            raise TrackerError(f"Bad state {state!r}: 'open' or 'closed'")
        payload = {
            key: value for key, value in (("title", title), ("body", body), ("state", state)) if value is not None
        }
        data = self._request("PATCH", f"/repos/{repo}/issues/{ref_id}", payload)
        return self._to_issue(repo, data)

    def close_issue(self, repo: str, ref_id: str) -> ExternalIssue:
        return self.update_issue(repo, ref_id, state="closed")
