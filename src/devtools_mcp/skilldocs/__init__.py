"""Live skills: SKILL.md files that are really CRDT documents.

Each live skill is a pycrdt text document persisted as an update log in
SQLite and *materialized* to a real `<skills-dir>/<name>/SKILL.md` on every
change, the file Claude Code loads is always the current merged state.
Machines sync docs peer-to-peer over the dashboard's `/api/skilldoc/` API;
concurrent edits from different agents/machines merge at character level
instead of last-writer-wins clobbering.
"""

from devtools_mcp.skilldocs.store import SkillDocError, SkillDocStore

__all__ = ["SkillDocError", "SkillDocStore"]
