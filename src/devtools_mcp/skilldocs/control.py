"""Local control state for dynamic skills: the active power mode, per-skill
overrides, and a disabled set. Small last-writer-wins key/value table in the
same skilldocs.db, so flipping a flag is one row write.

Machine-local in v1 (a `DEVTOOLS_MCP_SKILL_MODE` env var overrides everything);
cross-machine sync of this state is a marked follow-on that would reuse the
tracker's HLC+LWW pattern. The skill documents themselves already sync, a peer
just renders the synced content at its own effective mode.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from devtools_mcp.skilldocs.variants import DEFAULT_MODE

MODE_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
NAME_RE = re.compile(r"^[a-z][a-z0-9-]{1,63}$")
OVERRIDES_MAX: int = 500  # per-skill mode overrides
DISABLED_MAX: int = 500

_MODE_KEY = "mode"
_OVERRIDES_KEY = "overrides"
_DISABLED_KEY = "disabled"


class SkillControlError(Exception):
    """Expected/reportable condition (bad input), not a bug."""


def _utc_now_iso() -> str:
    stamp = datetime.now(UTC).isoformat()
    assert stamp.endswith("+00:00"), f"expected UTC timestamp, got {stamp!r}"
    return stamp


def _validate_mode(mode: str) -> str:
    mode = (mode or "").strip().lower()
    if not MODE_RE.match(mode):
        raise SkillControlError(f"Bad mode {mode!r}: kebab-case, 1-32 chars, [a-z0-9-]")
    return mode


def _validate_name(name: str) -> str:
    name = (name or "").strip().lower()
    if not NAME_RE.match(name):
        raise SkillControlError(f"Bad skill name {name!r}: kebab-case, 2-64 chars")
    return name


class SkillControl:
    """SQLite-backed control flags for local dynamic-skill rendering."""

    def __init__(self, root: Path | None = None, conn: sqlite3.Connection | None = None) -> None:
        if conn is not None:
            # Shared connection (from a SkillDocStore), already migrated; never
            # re-create tables and never own/close it.
            self.conn = conn
            self._owns_conn = False
        else:
            # Standalone: open the SAME skilldocs DB through the shared helper so
            # pragmas + migrations match the store exactly (skill_control lives
            # in migration v1, so no ad-hoc CREATE TABLE here).
            from devtools_mcp.skilldocs.store import connect, resolve_db_path

            self.conn = connect(resolve_db_path(root))
            self._owns_conn = True
        assert self.conn is not None, "control store failed to open"

    def close(self) -> None:
        """Close only a connection this instance owns; a shared conn is left open."""
        if self._owns_conn and self.conn is not None:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]

    # -- raw key/value (LWW) ---------------------------------------------------

    def _get(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM skill_control WHERE key = ?", (key,)).fetchone()
        return row["value"] if row is not None else None

    def _set(self, key: str, value: str) -> None:
        assert isinstance(value, str), "control value must be str"
        self.conn.execute(
            "INSERT INTO skill_control (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            (key, value, _utc_now_iso()),
        )

    def _get_json(self, key: str, fallback: object) -> object:
        raw = self._get(key)
        if raw is None:
            return fallback
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return fallback

    # -- global mode -----------------------------------------------------------

    def global_mode(self) -> str:
        """The stored global mode, or the default; env override is applied per
        skill in effective_mode (not stored here)."""
        stored = self._get(_MODE_KEY)
        return _validate_mode(stored) if stored else DEFAULT_MODE

    def set_mode(self, mode: str) -> str:
        mode = _validate_mode(mode)
        self._set(_MODE_KEY, mode)
        return mode

    def overrides(self) -> dict[str, str]:
        data = self._get_json(_OVERRIDES_KEY, {})
        if not isinstance(data, dict):
            return {}
        out = {str(k): str(v) for k, v in list(data.items())[:OVERRIDES_MAX]}
        assert len(out) <= OVERRIDES_MAX, "overrides exceeded bound"
        return out

    def set_override(self, name: str, mode: str) -> None:
        name = _validate_name(name)
        mode = _validate_mode(mode)
        current = self.overrides()
        if name not in current and len(current) >= OVERRIDES_MAX:
            raise SkillControlError(f"too many overrides (max {OVERRIDES_MAX})")
        current[name] = mode
        self._set(_OVERRIDES_KEY, json.dumps(current, sort_keys=True))

    def clear_override(self, name: str) -> None:
        name = _validate_name(name)
        current = self.overrides()
        if current.pop(name, None) is not None:
            self._set(_OVERRIDES_KEY, json.dumps(current, sort_keys=True))

    def disabled(self) -> set[str]:
        data = self._get_json(_DISABLED_KEY, [])
        if not isinstance(data, list):
            return set()
        out = {str(x) for x in data[:DISABLED_MAX]}
        assert len(out) <= DISABLED_MAX, "disabled set exceeded bound"
        return out

    def set_disabled(self, name: str, disabled: bool) -> None:
        name = _validate_name(name)
        current = self.disabled()
        if disabled:
            if name not in current and len(current) >= DISABLED_MAX:
                raise SkillControlError(f"too many disabled skills (max {DISABLED_MAX})")
            current.add(name)
        else:
            current.discard(name)
        self._set(_DISABLED_KEY, json.dumps(sorted(current)))

    def is_disabled(self, name: str) -> bool:
        return _validate_name(name) in self.disabled()

    # -- resolution ------------------------------------------------------------

    def effective_mode(self, name: str) -> str:
        """Mode for one skill: env override > per-skill override > global > default."""
        env = os.environ.get("DEVTOOLS_MCP_SKILL_MODE", "").strip().lower()
        if env and MODE_RE.match(env):
            return env
        name = _validate_name(name)
        override = self.overrides().get(name)
        if override:
            return _validate_mode(override)
        return self.global_mode()
