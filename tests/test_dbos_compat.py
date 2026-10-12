"""SQLite compatibility must degrade recipes without suppressing other tests."""

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from devtools_mcp.recipes import dbos_app


@pytest.mark.parametrize("version", [(3, 38, 0), (3, 46, 0)])
def test_supported_sqlite_is_accepted(monkeypatch, version):
    monkeypatch.setattr(sqlite3, "sqlite_version_info", version)
    dbos_app.check_sqlite_for_dbos()


def test_old_sqlite_fails_before_dbos_construction(monkeypatch):
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 37, 2))
    monkeypatch.setattr(sqlite3, "sqlite_version", "3.37.2")

    def forbidden():
        raise AssertionError("DBOS must not be constructed")

    monkeypatch.setattr(dbos_app, "get_dbos", forbidden)
    with pytest.raises(RuntimeError, match=r"SQLite >= 3.38.0.*3.37.2") as error:
        dbos_app.launch_dbos()
    assert "alone does not replace" in str(error.value)


def test_server_remains_available_with_old_sqlite(monkeypatch, capsys):
    from devtools_mcp import server

    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 37, 2))
    server._launch_dbos()  # Reports degradation, does not kill the server.
    assert "DBOS failed to launch" in capsys.readouterr().err


def test_old_sqlite_skips_only_explicit_dbos_tests(tmp_path):
    root = Path(__file__).resolve().parents[1]
    (tmp_path / "conftest.py").write_text((root / "tests/conftest.py").read_text())
    (tmp_path / "test_probe.py").write_text(
        "def test_unrelated(): assert True\n"
        "def test_recipe(dbos_runtime): raise AssertionError('must skip before execution')\n"
    )
    program = (
        "import sqlite3, pytest; "
        "sqlite3.sqlite_version_info=(3,37,2); sqlite3.sqlite_version='3.37.2'; "
        "raise SystemExit(pytest.main(['-q', '-rs', '.']))"
    )
    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=tmp_path,
        env=dict(os.environ, PYTHONPATH=str(root / "src"), PYTEST_ADDOPTS=""),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 passed, 1 skipped" in result.stdout
    assert "SQLite >= 3.38.0" in result.stdout
