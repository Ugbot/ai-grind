"""SQLite-backed progress tracker (mini-JIRA) for devtools-mcp.

Persistent, hierarchical task tracking: projects, tasks/subtasks (punch-card
breakdown), acceptance criteria linked to tests, commit links, auto-tag rules,
and external issue refs (GitHub etc.). All MCP-facing tools live in
devtools_mcp.tools.tracker_tools; this package is the domain layer.
"""

from devtools_mcp.tracker.db import TrackerDB, TrackerError, open_tracker

__all__ = ["TrackerDB", "TrackerError", "open_tracker"]
