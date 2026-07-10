"""Per-project station sync rules: TOML config, precedence, validation.

Pure module — no HTTP, no DB. Three layers, highest wins:

1. Env vars: LLM_STATION_REMOTE_URL / LLM_STATION_ORG_ID override url/org;
   DEVTOOLS_MCP_STATION_CONFIG points at an explicit config file. The API
   key is env-only (var named by [station].api_key_env, default
   LLM_STATION_API_KEY) — a config file containing anything shaped like an
   lls_ key is rejected outright.
2. Per-repo `.devtools-mcp/station.toml`, found by a bounded walk up from
   the starting directory. Committed rules, never credentials.
3. Global `~/.devtools-mcp/station.toml` user defaults.

A repo file wins wholesale over the global file (no deep merge) so a repo's
rules are always read as written.
"""

from __future__ import annotations

import hashlib
import json
import os
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from devtools_mcp.tracker.db import TrackerError

ENV_CONFIG_PATH: str = "DEVTOOLS_MCP_STATION_CONFIG"
ENV_URL: str = "LLM_STATION_REMOTE_URL"
ENV_ORG: str = "LLM_STATION_ORG_ID"
DEFAULT_API_KEY_ENV: str = "LLM_STATION_API_KEY"
CONFIG_FILENAME: str = "station.toml"
CONFIG_DIRNAME: str = ".devtools-mcp"
WALK_UP_MAX: int = 10
DOMAINS: tuple[str, ...] = ("tasks", "sessions", "collab", "skills", "perf")
LEAK_SCAN_MAX_VALUES: int = 500

Direction = Literal["push", "pull", "both"]


class DomainRule(BaseModel):
    """One domain's sync rule. Unknown keys are config typos — fail loudly."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    direction: Direction = "push"
    on_conflict: Literal["local_wins", "remote_wins"] = "local_wins"
    ttl_minutes: int = 30  # collab only: pushed checkout lease length
    suites: list[str] = []  # perf only: empty = all suites
    kinds: list[str] = []  # tasks only: empty = all kinds


class StationSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = ""
    org: str = ""
    api_key_env: str = DEFAULT_API_KEY_ENV


class ProjectSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    local: str = ""  # tracker project key, e.g. GRIND
    remote: str = ""  # platform project id or key; "" = match/create by local key
    repo: str = "auto"  # platform repo id, or "auto" = resolve via git origin


class StationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    station: StationSection = StationSection()
    project: ProjectSection = ProjectSection()
    domains: dict[str, DomainRule] = {}
    source_path: str = ""  # where this config was loaded from (not hashed)

    def config_hash(self) -> str:
        """Stable content hash — source_path excluded so moving a file is a no-op."""
        payload = self.model_dump(exclude={"source_path"})
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        assert len(digest) == 64, "sha256 hexdigest must be 64 chars"
        return digest

    def rule(self, domain: str) -> DomainRule:
        assert domain in DOMAINS, f"unknown domain {domain!r}"
        return self.domains.get(domain, DomainRule())

    def enabled_domains(self) -> tuple[str, ...]:
        enabled = tuple(d for d in DOMAINS if self.rule(d).enabled)
        assert len(enabled) <= len(DOMAINS), "enabled domains over bound"
        return enabled

    def api_key(self) -> str:
        """The lls_ key: env var first, browser-auth credential store second.

        Raises TrackerError whose message contains the exact instructions an
        LLM should relay to the user (dashboard /station/auth flow).
        """
        env_name = self.station.api_key_env or DEFAULT_API_KEY_ENV
        key = os.environ.get(env_name, "").strip()
        if key:
            return key
        from devtools_mcp.station import credentials

        stored = credentials.load_credentials()
        if stored is not None:
            stored_url = str(stored.get("url", "")).rstrip("/")
            if self.station.url and stored_url and stored_url != self.station.url.rstrip("/"):
                raise TrackerError(
                    f"Stored credential is for {stored_url}, but this project targets "
                    f"{self.station.url}. " + credentials.auth_instructions(self.station.url)
                )
            return str(stored["api_key"])
        raise TrackerError(credentials.auth_instructions(self.station.url))


def _scan_for_key_leak(node: object, depth: int = 0) -> None:
    """Reject any config value that looks like an lls_ API key."""
    assert depth <= 8, "config nesting too deep for leak scan"
    if isinstance(node, str) and node.startswith("lls_"):
        raise TrackerError(
            "station.toml contains what looks like an API key (lls_...). "
            "Remove it — keys are env-only (see [station].api_key_env)."
        )
    if isinstance(node, dict):
        assert len(node) <= LEAK_SCAN_MAX_VALUES, "config dict over leak-scan bound"
        for value in node.values():
            _scan_for_key_leak(value, depth + 1)
    if isinstance(node, list):
        assert len(node) <= LEAK_SCAN_MAX_VALUES, "config list over leak-scan bound"
        for value in node:
            _scan_for_key_leak(value, depth + 1)


def _find_repo_config(start: Path) -> Path | None:
    """Bounded walk up from `start` looking for .devtools-mcp/station.toml."""
    assert start.is_absolute(), f"walk needs absolute path, got {start}"
    current = start
    for _ in range(WALK_UP_MAX):  # bounded ancestor walk
        candidate = current / CONFIG_DIRNAME / CONFIG_FILENAME
        if candidate.is_file():
            return candidate
        if current.parent == current:
            return None
        current = current.parent
    return None


def _global_config_path() -> Path:
    return Path.home() / CONFIG_DIRNAME / CONFIG_FILENAME


def _parse_config_file(path: Path) -> StationConfig:
    """Parse + validate one TOML file (offline validation only)."""
    assert path.is_file(), f"config file vanished: {path}"
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise TrackerError(f"Bad TOML in {path}: {exc}") from exc
    _scan_for_key_leak(raw)
    try:
        cfg = StationConfig(**raw, source_path=str(path))
    except ValidationError as exc:
        raise TrackerError(f"Invalid station config {path}: {exc}") from exc
    unknown = set(cfg.domains) - set(DOMAINS)
    if unknown:
        raise TrackerError(f"Unknown domains in {path}: {', '.join(sorted(unknown))} (valid: {', '.join(DOMAINS)})")
    return cfg


def load_station_config(start_dir: Path | None = None) -> StationConfig | None:
    """Resolve the effective config: env path > repo file > global file.

    Returns None when no config exists anywhere. Env overrides for url/org
    are applied to whichever file won.
    """
    explicit = os.environ.get(ENV_CONFIG_PATH, "").strip()
    path: Path | None
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise TrackerError(f"{ENV_CONFIG_PATH} points at a missing file: {explicit}")
    else:
        start = (start_dir or Path.cwd()).resolve()
        path = _find_repo_config(start)
        if path is None:
            fallback = _global_config_path()
            path = fallback if fallback.is_file() else None
    if path is None:
        return None
    cfg = _parse_config_file(path)
    env_url = os.environ.get(ENV_URL, "").strip()
    env_org = os.environ.get(ENV_ORG, "").strip()
    if env_url:
        cfg.station.url = env_url
    if env_org:
        cfg.station.org = env_org
    assert cfg.source_path, "loaded config must record its source path"
    return cfg


def validate_for_link(cfg: StationConfig) -> None:
    """Offline checks that must hold before station_link goes online."""
    assert cfg is not None, "validate_for_link on missing config"
    if not cfg.station.url.startswith(("http://", "https://")):
        raise TrackerError(f"[station].url must be http(s), got {cfg.station.url!r} (or set {ENV_URL})")
    # org may stay empty: API keys are org-scoped, so link adopts /auth/me's org.
    if not cfg.project.local.strip():
        raise TrackerError("[project].local (tracker project key) is required")
    if not cfg.enabled_domains():
        raise TrackerError("No domains enabled — enable at least one [domains.*] section")


CONFIG_TEMPLATE: str = """\
# devtools-mcp station sync — per-project rules.
# Secrets NEVER live here: the API key comes from the env var named by
# api_key_env. Env overrides: LLM_STATION_REMOTE_URL, LLM_STATION_ORG_ID.

[station]
url = "http://localhost:8000"
org = ""                            # platform org id
api_key_env = "LLM_STATION_API_KEY" # NAME of the env var holding lls_...

[project]
local = ""      # tracker project key, e.g. "GRIND"
remote = ""     # platform project id/key; "" = match or create by local key
repo = "auto"   # platform repo id, or "auto" = resolve from git origin url

[domains.tasks]
enabled = true
direction = "both"          # push | pull | both
on_conflict = "local_wins"  # local_wins | remote_wins

[domains.sessions]
enabled = false
direction = "push"

[domains.collab]
enabled = false
direction = "push"
ttl_minutes = 30

[domains.skills]
enabled = false
direction = "push"

[domains.perf]
enabled = false
direction = "push"
suites = []                 # empty = all suites
"""
