"""Local control state: power mode, per-skill overrides, disabled set."""

from __future__ import annotations

import pytest

from devtools_mcp.skilldocs.control import SkillControl, SkillControlError


def _control() -> SkillControl:
    return SkillControl()  # data dir is isolated by the autouse conftest fixture


def test_default_mode_is_high():
    c = _control()
    try:
        assert c.global_mode() == "high"
        assert c.effective_mode("some-skill") == "high"
    finally:
        c.close()


def test_set_and_persist_global_mode():
    c = _control()
    try:
        c.set_mode("low")
    finally:
        c.close()
    c2 = _control()
    try:
        assert c2.global_mode() == "low"
    finally:
        c2.close()


def test_resolution_order_env_over_override_over_global(monkeypatch):
    c = _control()
    try:
        c.set_mode("low")
        c.set_override("my-skill", "high")
        assert c.effective_mode("my-skill") == "high"  # override beats global
        assert c.effective_mode("other-skill") == "low"  # falls back to global
        monkeypatch.setenv("DEVTOOLS_MCP_SKILL_MODE", "low")
        assert c.effective_mode("my-skill") == "low"  # env beats override
    finally:
        c.close()


def test_clear_override():
    c = _control()
    try:
        c.set_override("sk", "low")
        assert c.overrides() == {"sk": "low"}
        c.clear_override("sk")
        assert c.overrides() == {}
    finally:
        c.close()


def test_disabled_set():
    c = _control()
    try:
        assert not c.is_disabled("sk")
        c.set_disabled("sk", True)
        assert c.is_disabled("sk") and c.disabled() == {"sk"}
        c.set_disabled("sk", False)
        assert not c.is_disabled("sk")
    finally:
        c.close()


def test_bad_mode_and_name_rejected():
    c = _control()
    try:
        with pytest.raises(SkillControlError):
            c.set_mode("Loud!")
        with pytest.raises(SkillControlError):
            c.set_override("Bad Name", "low")
    finally:
        c.close()


def test_shared_connection_not_closed():
    import sqlite3

    from devtools_mcp.skilldocs.schema import apply_migrations

    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)  # a borrowed conn is expected to be already migrated
    c = SkillControl(conn=conn)
    c.set_mode("low")
    c.close()  # must NOT close a borrowed connection
    assert conn.execute("SELECT 1").fetchone() == (1,)
    conn.close()
