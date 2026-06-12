"""Provider registry: look up an issue provider by name."""

from __future__ import annotations

from devtools_mcp.tracker.db import TrackerError
from devtools_mcp.tracker.providers.base import ExternalIssue, IssueProvider

PROVIDER_NAMES: tuple[str, ...] = ("github", "gitlab")


def get_provider(name: str, token: str | None = None, client=None) -> IssueProvider:
    """Construct a provider by name; TrackerError for unknown names."""
    assert isinstance(name, str), f"provider name must be str, got {type(name)}"
    normalized = name.strip().lower()
    if normalized == "github":
        from devtools_mcp.tracker.providers.github import GitHubProvider

        return GitHubProvider(token=token, client=client)
    if normalized == "gitlab":
        from devtools_mcp.tracker.providers.gitlab import GitLabProvider

        return GitLabProvider()
    raise TrackerError(f"Unknown provider {name!r}; known: {', '.join(PROVIDER_NAMES)}")


__all__ = ["ExternalIssue", "IssueProvider", "get_provider", "PROVIDER_NAMES"]
