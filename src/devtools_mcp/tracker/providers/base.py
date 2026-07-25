"""Issue-provider abstraction: one interface, many trackers (GitHub, GitLab...).

Providers are synchronous and stateless apart from an injected HTTP client;
the MCP tool layer runs them in a worker thread if needed (calls are short).
All provider failures surface as TrackerError with an actionable message.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from pydantic import BaseModel


class ExternalIssue(BaseModel):
    """Normalized view of a remote issue, whatever the provider."""

    provider: str
    ref_id: str
    repo: str
    url: str
    state: str
    title: str
    body: str = ""


class IssueProvider(ABC):
    """Minimal contract every issue provider implements."""

    name: ClassVar[str] = ""

    @abstractmethod
    def create_issue(self, repo: str, title: str, body: str, labels: list[str]) -> ExternalIssue:
        """Create a remote issue. `repo` is provider-native (e.g. 'owner/name')."""

    @abstractmethod
    def get_issue(self, repo: str, ref_id: str) -> ExternalIssue:
        """Fetch the current remote state of an issue."""

    @abstractmethod
    def update_issue(
        self,
        repo: str,
        ref_id: str,
        title: str | None = None,
        body: str | None = None,
        state: str | None = None,
    ) -> ExternalIssue:
        """Patch title/body/state on a remote issue."""

    @abstractmethod
    def close_issue(self, repo: str, ref_id: str) -> ExternalIssue:
        """Close a remote issue."""
