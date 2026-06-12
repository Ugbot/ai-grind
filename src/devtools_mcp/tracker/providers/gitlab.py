"""GitLab issue provider — skeleton only.

The provider interface is in place so the tracker's external_refs and tool
surface stay provider-agnostic; only the HTTP layer is unimplemented.
"""

from __future__ import annotations

from devtools_mcp.tracker.providers.base import ExternalIssue, IssueProvider

_MESSAGE = "GitLab provider not yet implemented; use provider='github'"


class GitLabProvider(IssueProvider):
    name = "gitlab"

    def create_issue(self, repo: str, title: str, body: str, labels: list[str]) -> ExternalIssue:
        raise NotImplementedError(_MESSAGE)

    def get_issue(self, repo: str, ref_id: str) -> ExternalIssue:
        raise NotImplementedError(_MESSAGE)

    def update_issue(
        self,
        repo: str,
        ref_id: str,
        title: str | None = None,
        body: str | None = None,
        state: str | None = None,
    ) -> ExternalIssue:
        raise NotImplementedError(_MESSAGE)

    def close_issue(self, repo: str, ref_id: str) -> ExternalIssue:
        raise NotImplementedError(_MESSAGE)
