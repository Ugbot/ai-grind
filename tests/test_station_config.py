"""Station config: precedence, validation, key-leak rejection, hashing."""

from __future__ import annotations

from pathlib import Path

import pytest

from devtools_mcp.station.config import (
    CONFIG_TEMPLATE,
    StationConfig,
    load_station_config,
    validate_for_link,
)
from devtools_mcp.tracker.db import TrackerError

MINIMAL = """
[station]
url = "http://localhost:8000"
org = "acme"

[project]
local = "GRIND"

[domains.tasks]
enabled = true
direction = "both"
"""


def _write_repo_config(root: Path, body: str = MINIMAL) -> Path:
    dest = root / ".devtools-mcp" / "station.toml"
    dest.parent.mkdir(parents=True)
    dest.write_text(body, encoding="utf-8")
    return dest


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in ("DEVTOOLS_MCP_STATION_CONFIG", "LLM_STATION_REMOTE_URL", "LLM_STATION_ORG_ID"):
        monkeypatch.delenv(var, raising=False)


class TestLoading:
    def test_none_when_absent(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path / "nohome")
        assert load_station_config(tmp_path) is None

    def test_repo_file_found_by_walk_up(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path / "nohome")
        _write_repo_config(tmp_path)
        nested = tmp_path / "src" / "deep" / "deeper"
        nested.mkdir(parents=True)
        cfg = load_station_config(nested)
        assert cfg is not None
        assert cfg.project.local == "GRIND"
        assert cfg.rule("tasks").enabled
        assert cfg.rule("tasks").direction == "both"

    def test_env_path_wins(self, tmp_path, monkeypatch):
        explicit = tmp_path / "elsewhere.toml"
        explicit.write_text(MINIMAL.replace("GRIND", "OTHER"), encoding="utf-8")
        _write_repo_config(tmp_path)
        monkeypatch.setenv("DEVTOOLS_MCP_STATION_CONFIG", str(explicit))
        cfg = load_station_config(tmp_path)
        assert cfg is not None and cfg.project.local == "OTHER"

    def test_env_url_org_override(self, tmp_path, monkeypatch):
        _write_repo_config(tmp_path)
        monkeypatch.setenv("LLM_STATION_REMOTE_URL", "https://station.example")
        monkeypatch.setenv("LLM_STATION_ORG_ID", "override-org")
        cfg = load_station_config(tmp_path)
        assert cfg is not None
        assert cfg.station.url == "https://station.example"
        assert cfg.station.org == "override-org"

    def test_template_parses(self, tmp_path):
        _write_repo_config(tmp_path, CONFIG_TEMPLATE)
        cfg = load_station_config(tmp_path)
        assert cfg is not None
        assert cfg.rule("tasks").enabled  # template enables tasks by default


class TestValidation:
    def test_key_leak_rejected(self, tmp_path):
        bad = MINIMAL + '\n[domains.perf]\nenabled = true\nsuites = ["lls_secretkey"]\n'
        _write_repo_config(tmp_path, bad)
        with pytest.raises(TrackerError, match="API key"):
            load_station_config(tmp_path)

    def test_unknown_domain_rejected(self, tmp_path):
        _write_repo_config(tmp_path, MINIMAL + "\n[domains.bogus]\nenabled = true\n")
        with pytest.raises(TrackerError, match="bogus"):
            load_station_config(tmp_path)

    def test_typo_key_rejected(self, tmp_path):
        _write_repo_config(tmp_path, MINIMAL + "\n[domains.perf]\nenbaled = true\n")
        with pytest.raises(TrackerError, match="Invalid station config"):
            load_station_config(tmp_path)

    def test_bad_toml_rejected(self, tmp_path):
        _write_repo_config(tmp_path, "[station\nurl=")
        with pytest.raises(TrackerError, match="Bad TOML"):
            load_station_config(tmp_path)

    def test_validate_for_link_requires_url_project_domain(self):
        cfg = StationConfig()
        with pytest.raises(TrackerError, match="url"):
            validate_for_link(cfg)
        cfg.station.url = "http://x"
        with pytest.raises(TrackerError, match="local"):
            validate_for_link(cfg)
        cfg.project.local = "GRIND"
        with pytest.raises(TrackerError, match="domains"):
            validate_for_link(cfg)

    def test_api_key_env_only(self, monkeypatch):
        cfg = StationConfig()
        monkeypatch.delenv("LLM_STATION_API_KEY", raising=False)
        with pytest.raises(TrackerError, match="LLM_STATION_API_KEY"):
            cfg.api_key()
        monkeypatch.setenv("LLM_STATION_API_KEY", "lls_abc")
        assert cfg.api_key() == "lls_abc"


class TestHash:
    def test_hash_stable_and_source_independent(self, tmp_path):
        a = _write_repo_config(tmp_path / "a", MINIMAL)
        b = _write_repo_config(tmp_path / "b", MINIMAL)
        cfg_a = load_station_config(a.parent.parent)
        cfg_b = load_station_config(b.parent.parent)
        assert cfg_a is not None and cfg_b is not None
        assert cfg_a.source_path != cfg_b.source_path
        assert cfg_a.config_hash() == cfg_b.config_hash()

    def test_hash_changes_with_content(self, tmp_path):
        _write_repo_config(tmp_path, MINIMAL)
        cfg = load_station_config(tmp_path)
        assert cfg is not None
        before = cfg.config_hash()
        cfg.domains["tasks"].direction = "push"
        assert cfg.config_hash() != before
