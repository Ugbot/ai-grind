"""Browser-auth surface: credential store, key resolution, viz auth routes."""

from __future__ import annotations

import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

import devtools_mcp.server  # noqa: F401  (registers backends)
from devtools_mcp.station import credentials
from devtools_mcp.station.config import StationConfig, StationSection
from devtools_mcp.tracker.db import ENV_DB_PATH, TrackerError, open_tracker
from devtools_mcp.viz.server import VizServer
from devtools_mcp.workspace import AppContext


@pytest.fixture(autouse=True)
def isolated_store(tmp_path: Path, monkeypatch):
    monkeypatch.setenv(credentials.ENV_AUTH_PATH, str(tmp_path / "station-auth.json"))
    monkeypatch.delenv("LLM_STATION_API_KEY", raising=False)


class TestCredentialStore:
    def test_round_trip(self):
        assert credentials.load_credentials() is None
        path = credentials.save_credentials("http://platform:8000/", "lls_abc", "org-1", "Ben")
        assert path.is_file()
        stored = credentials.load_credentials()
        assert stored is not None
        assert stored["api_key"] == "lls_abc"
        assert stored["url"] == "http://platform:8000"  # trailing slash normalized
        assert credentials.clear_credentials() is True
        assert credentials.load_credentials() is None

    def test_bad_key_rejected(self):
        with pytest.raises(TrackerError, match="lls_"):
            credentials.save_credentials("http://platform:8000", "ghp_wrongkind", "org-1")

    def test_malformed_file_reads_as_none(self):
        path = credentials.credentials_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not json", encoding="utf-8")
        assert credentials.load_credentials() is None


class TestKeyResolution:
    def test_env_wins_over_store(self, monkeypatch):
        credentials.save_credentials("http://platform:8000", "lls_stored", "org-1")
        monkeypatch.setenv("LLM_STATION_API_KEY", "lls_env")
        cfg = StationConfig(station=StationSection(url="http://platform:8000"))
        assert cfg.api_key() == "lls_env"

    def test_store_fallback(self):
        credentials.save_credentials("http://platform:8000", "lls_stored", "org-1")
        cfg = StationConfig(station=StationSection(url="http://platform:8000"))
        assert cfg.api_key() == "lls_stored"

    def test_url_mismatch_is_actionable(self):
        credentials.save_credentials("http://other:9000", "lls_stored", "org-1")
        cfg = StationConfig(station=StationSection(url="http://platform:8000"))
        with pytest.raises(TrackerError, match="/station/auth"):
            cfg.api_key()

    def test_missing_key_message_teaches_the_flow(self):
        cfg = StationConfig(station=StationSection(url="http://platform:8000"))
        with pytest.raises(TrackerError) as exc:
            cfg.api_key()
        message = str(exc.value)
        assert "/station/auth" in message
        assert "LLM_STATION_API_KEY" in message
        assert "Tell the user" in message


@pytest.fixture
def served(tmp_path: Path, monkeypatch):
    monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "tracker.db"))
    open_tracker().close()  # migrate
    srv = VizServer(AppContext())
    url = srv.start(port=0)
    yield url
    srv.stop()


def _get(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _fresh_nonce(served: str) -> str:
    """Obtain a one-time callback nonce the way the real flow does, via the auth page."""
    _, page = _get(served + "/station/auth")
    match = re.search(r"nonce%3D([A-Za-z0-9_-]+)", page)
    assert match, "auth page did not embed a callback nonce"
    return match.group(1)


class TestVizAuthRoutes:
    def test_auth_page_renders_signin_links(self, served):
        status, body = _get(served + "/station/auth?url=http://platform:8000")
        assert status == 200
        assert "Sign in with GitHub" in body
        assert "local_callback=" in body
        assert "http://platform:8000/auth/github" in body

    def test_callback_stores_and_confirms(self, served):
        query = urllib.parse.urlencode(
            {
                "key": "lls_fromflow",
                "org": "org-1",
                "url": "http://platform:8000",
                "member": "Ben",
                "nonce": _fresh_nonce(served),
            }
        )
        status, body = _get(served + "/api/station/callback?" + query)
        assert status == 200
        assert "Connected" in body
        stored = credentials.load_credentials()
        assert stored is not None and stored["api_key"] == "lls_fromflow"
        assert stored["org_id"] == "org-1"

    def test_callback_rejects_garbage_key(self, served):
        query = urllib.parse.urlencode({"key": "oops", "url": "http://platform:8000", "nonce": _fresh_nonce(served)})
        status, body = _get(served + "/api/station/callback?" + query)
        assert status == 400
        assert credentials.load_credentials() is None

    def test_callback_without_nonce_is_rejected(self, served):
        # A forged callback (no valid nonce, e.g. an <img src> CSRF) must not store creds.
        query = urllib.parse.urlencode({"key": "lls_evil", "org": "attacker", "url": "http://evil"})
        status, _ = _get(served + "/api/station/callback?" + query)
        assert status == 403
        assert credentials.load_credentials() is None

    def test_status_endpoint_never_leaks_key(self, served):
        credentials.save_credentials("http://platform:8000", "lls_secret", "org-1")
        status, body = _get(served + "/api/station/status")
        assert status == 200
        assert "lls_secret" not in body
        assert '"authenticated": true' in body.lower()

    def test_token_paste_form(self, served):
        data = urllib.parse.urlencode({"url": "http://platform:8000", "key": "lls_pasted"}).encode()
        request = urllib.request.Request(
            served + "/api/station/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 200
        stored = credentials.load_credentials()
        assert stored is not None and stored["api_key"] == "lls_pasted"

    def test_cross_origin_post_is_refused(self, served):
        # A malicious page's cross-origin POST carries a foreign Origin → rejected.
        data = urllib.parse.urlencode({"url": "http://evil", "key": "lls_evil"}).encode()
        request = urllib.request.Request(
            served + "/api/station/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Origin": "http://evil.example"},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                code = response.status
        except urllib.error.HTTPError as exc:
            code = exc.code
        assert code == 403
        assert credentials.load_credentials() is None

    def test_rebinding_host_is_refused(self, served):
        # DNS-rebinding: attacker domain resolves to loopback, but the Host header
        # it sends is not in the allowlist → rejected before any handler runs.
        data = urllib.parse.urlencode({"url": "http://evil", "key": "lls_evil"}).encode()
        request = urllib.request.Request(
            served + "/api/station/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Host": "attacker.example"},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                code = response.status
        except urllib.error.HTTPError as exc:
            code = exc.code
        assert code == 403
