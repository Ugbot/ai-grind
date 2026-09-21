"""StationClient: the single HTTP seam to an llm-station-remote platform.

Synchronous httpx (the tracker layer is sync-with-injected-client; tool
functions wrap calls via anyio.to_thread). One short method per endpoint,
no pagination loops. Callers own their bounds. Every network failure
surfaces as StationError; offline is a normal, reported state.

Retry policy: connect errors retry for all methods (the request never
reached the server); read timeouts and 5xx retry only for GET, a replayed
non-idempotent POST could double-create (pending-intent links also
protect task creates, see station.links).
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from devtools_mcp.tracker.db import TrackerError

TIMEOUT_SECONDS: float = 30.0
RETRY_MAX: int = 2
BACKOFF_SECONDS: tuple[float, float] = (0.5, 2.0)
TASKS_PAGE_MAX: int = 1000  # platform list_tasks cap
CHECKOUTS_PAGE_MAX: int = 500
SKILLS_PAGE_MAX: int = 500
PERF_PAGE_MAX: int = 200


class StationError(TrackerError):
    """Platform HTTP failure (non-2xx or transport error)."""

    def __init__(self, status_code: int, detail: str) -> None:
        assert 0 <= status_code < 600, f"bad status code {status_code}"
        super().__init__(f"platform HTTP {status_code}: {detail}" if status_code else f"platform unreachable: {detail}")
        self.status_code = status_code
        self.detail = detail


class StationClient:
    """HTTP client for the llm-station-remote REST API (Bearer lls_ auth)."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        org_id: str,
        client: httpx.Client | None = None,
    ) -> None:
        assert base_url.startswith(("http://", "https://")), f"bad base url {base_url!r}"
        assert api_key, "api_key must be non-empty"
        assert org_id, "org_id must be non-empty"
        self.org_id = org_id
        self._http = client or httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=TIMEOUT_SECONDS,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> StationClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # -- transport -------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        assert method in ("GET", "POST", "PATCH", "PUT", "DELETE"), f"bad method {method}"
        assert path.startswith("/"), f"path must start with /: {path!r}"
        last_error = ""
        for attempt in range(RETRY_MAX + 1):  # bounded retries
            try:
                response = self._http.request(method, path, json=json_body, params=params)
            except httpx.ConnectError as exc:
                last_error = str(exc)
                if attempt < RETRY_MAX:
                    time.sleep(BACKOFF_SECONDS[attempt])
                    continue
                raise StationError(0, last_error) from exc
            except httpx.HTTPError as exc:  # timeouts, protocol errors
                last_error = str(exc)
                if method == "GET" and attempt < RETRY_MAX:
                    time.sleep(BACKOFF_SECONDS[attempt])
                    continue
                raise StationError(0, last_error) from exc
            if response.status_code >= 500 and method == "GET" and attempt < RETRY_MAX:
                time.sleep(BACKOFF_SECONDS[attempt])
                continue
            return self._raise_or_json(response)
        raise AssertionError("retry loop must return or raise")  # pragma: no cover

    @staticmethod
    def _raise_or_json(response: httpx.Response) -> Any:
        if response.is_success:
            if response.status_code == 204 or not response.content:
                return None
            return response.json()
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text
        raise StationError(response.status_code, str(detail)[:500])

    def _org(self, suffix: str) -> str:
        assert suffix.startswith("/"), f"suffix must start with /: {suffix!r}"
        return f"/orgs/{self.org_id}{suffix}"

    # -- auth / context ----------------------------------------------------

    def auth_me(self) -> dict[str, Any]:
        result = self._request("GET", "/auth/me")
        assert isinstance(result, dict), "auth/me must return an object"
        return result

    def devtools_context(self) -> dict[str, Any]:
        result = self._request("GET", "/devtools_context")
        assert isinstance(result, dict), "devtools_context must return an object"
        return result

    # -- projects ----------------------------------------------------------

    def projects_list(self) -> list[dict[str, Any]]:
        result = self._request("GET", self._org("/projects"))
        assert isinstance(result, list), "projects list must be an array"
        return result

    def project_create(self, key: str, name: str, description: str | None = None) -> dict[str, Any]:
        assert key.strip(), "project key must be non-empty"
        body: dict[str, Any] = {"key": key, "name": name}
        if description:
            body["description"] = description
        result = self._request("POST", self._org("/projects"), json_body=body)
        assert isinstance(result, dict), "project create must return an object"
        return result

    # -- tasks ---------------------------------------------------------------

    def tasks_list(self, project_id: str, limit: int = TASKS_PAGE_MAX) -> list[dict[str, Any]]:
        assert project_id, "project_id must be non-empty"
        assert 1 <= limit <= TASKS_PAGE_MAX, f"limit out of range: {limit}"
        result = self._request("GET", self._org(f"/projects/{project_id}/tasks"), params={"limit": limit})
        assert isinstance(result, list), "tasks list must be an array"
        return result

    def task_create(self, project_id: str, body: dict[str, Any]) -> dict[str, Any]:
        assert project_id, "project_id must be non-empty"
        assert body.get("title"), "task create needs a title"
        result = self._request("POST", self._org(f"/projects/{project_id}/tasks"), json_body=body)
        assert isinstance(result, dict), "task create must return an object"
        return result

    def task_update(self, project_id: str, task_id: str, body: dict[str, Any]) -> dict[str, Any]:
        assert project_id and task_id, "project_id and task_id must be non-empty"
        assert body, "task update needs at least one field"
        result = self._request("PATCH", self._org(f"/projects/{project_id}/tasks/{task_id}"), json_body=body)
        assert isinstance(result, dict), "task update must return an object"
        return result

    def criterion_create(self, project_id: str, task_id: str, text: str, test_ref: str | None) -> dict[str, Any]:
        assert project_id and task_id, "ids must be non-empty"
        body: dict[str, Any] = {"text": text}
        if test_ref:
            body["test_ref"] = test_ref
        result = self._request("POST", self._org(f"/projects/{project_id}/tasks/{task_id}/criteria"), json_body=body)
        assert isinstance(result, dict), "criterion create must return an object"
        return result

    def criterion_update(
        self, project_id: str, task_id: str, criterion_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        assert project_id and task_id and criterion_id, "ids must be non-empty"
        result = self._request(
            "PATCH",
            self._org(f"/projects/{project_id}/tasks/{task_id}/criteria/{criterion_id}"),
            json_body=body,
        )
        assert isinstance(result, dict), "criterion update must return an object"
        return result

    def dep_add(self, project_id: str, task_id: str, depends_on_id: str) -> dict[str, Any]:
        assert project_id and task_id and depends_on_id, "ids must be non-empty"
        result = self._request(
            "POST",
            self._org(f"/projects/{project_id}/tasks/{task_id}/deps"),
            json_body={"depends_on_id": depends_on_id},
        )
        assert isinstance(result, dict), "dep add must return an object"
        return result

    def dep_remove(self, project_id: str, task_id: str, depends_on_id: str) -> None:
        assert project_id and task_id and depends_on_id, "ids must be non-empty"
        self._request("DELETE", self._org(f"/projects/{project_id}/tasks/{task_id}/deps/{depends_on_id}"))

    def commit_link(self, project_id: str, task_id: str, body: dict[str, Any]) -> dict[str, Any]:
        assert project_id and task_id, "ids must be non-empty"
        assert body.get("commit_hash"), "commit link needs commit_hash"
        result = self._request("POST", self._org(f"/projects/{project_id}/tasks/{task_id}/commits"), json_body=body)
        assert isinstance(result, dict), "commit link must return an object"
        return result

    # -- coordination -------------------------------------------------------

    def session_start(self, body: dict[str, Any]) -> dict[str, Any]:
        result = self._request("POST", self._org("/sessions"), json_body=body)
        assert isinstance(result, dict), "session start must return an object"
        return result

    def session_update(self, session_id: str, body: dict[str, Any]) -> dict[str, Any]:
        assert session_id, "session_id must be non-empty"
        assert body, "session update needs at least one field"
        result = self._request("PATCH", self._org(f"/sessions/{session_id}"), json_body=body)
        assert isinstance(result, dict), "session update must return an object"
        return result

    def handoff_create(self, body: dict[str, Any]) -> dict[str, Any]:
        assert body.get("context") and body.get("next_steps"), "handoff needs context and next_steps"
        result = self._request("POST", self._org("/handoffs"), json_body=body)
        assert isinstance(result, dict), "handoff create must return an object"
        return result

    def handoffs_pending(self) -> list[dict[str, Any]]:
        result = self._request("GET", self._org("/handoffs/pending"))
        assert isinstance(result, list), "pending handoffs must be an array"
        return result

    def handoff_accept(self, handoff_id: str) -> dict[str, Any]:
        assert handoff_id, "handoff_id must be non-empty"
        result = self._request("POST", self._org(f"/handoffs/{handoff_id}/accept"))
        assert isinstance(result, dict), "handoff accept must return an object"
        return result

    def handoff_decline(self, handoff_id: str) -> dict[str, Any]:
        assert handoff_id, "handoff_id must be non-empty"
        result = self._request("POST", self._org(f"/handoffs/{handoff_id}/decline"))
        assert isinstance(result, dict), "handoff decline must return an object"
        return result

    # -- checkouts -----------------------------------------------------------

    def checkouts_acquire(self, body: dict[str, Any]) -> dict[str, Any]:
        assert body.get("repo_id") and body.get("paths"), "acquire needs repo_id and paths"
        result = self._request("POST", self._org("/checkouts"), json_body=body)
        assert isinstance(result, dict), "checkout acquire must return an object"
        return result

    def checkouts_list(self, active_only: bool = True, limit: int = CHECKOUTS_PAGE_MAX) -> list[dict[str, Any]]:
        assert 1 <= limit <= CHECKOUTS_PAGE_MAX, f"limit out of range: {limit}"
        result = self._request("GET", self._org("/checkouts"), params={"active_only": active_only, "limit": limit})
        assert isinstance(result, list), "checkouts list must be an array"
        return result

    def checkouts_release(self, checkout_ids: list[str]) -> None:
        assert checkout_ids, "release needs at least one checkout id"
        self._request("POST", self._org("/checkouts/release"), json_body={"checkout_ids": checkout_ids})

    def checkouts_heartbeat(self, checkout_ids: list[str], ttl_minutes: int) -> dict[str, Any]:
        assert checkout_ids, "heartbeat needs at least one checkout id"
        assert 1 <= ttl_minutes <= 480, f"ttl_minutes out of range: {ttl_minutes}"
        result = self._request(
            "POST",
            self._org("/checkouts/heartbeat"),
            json_body={"checkout_ids": checkout_ids, "ttl_minutes": ttl_minutes},
        )
        assert isinstance(result, dict), "heartbeat must return an object"
        return result

    # -- repos -----------------------------------------------------------------

    def repo_by_url(self, remote_url: str) -> dict[str, Any] | None:
        assert remote_url, "remote_url must be non-empty"
        try:
            result = self._request("GET", "/repos/by-url", params={"remote_url": remote_url})
        except StationError as exc:
            if exc.status_code == 404:
                return None
            raise
        assert isinstance(result, dict), "repo by-url must return an object"
        return result

    def repo_register(self, remote_url: str, name: str) -> dict[str, Any]:
        assert remote_url and name, "repo register needs url and name"
        result = self._request("POST", "/repos", json_body={"remote_url": remote_url, "name": name})
        assert isinstance(result, dict), "repo register must return an object"
        return result

    # -- skills ------------------------------------------------------------------

    def skills_list(self, limit: int = SKILLS_PAGE_MAX) -> list[dict[str, Any]]:
        assert 1 <= limit <= SKILLS_PAGE_MAX, f"limit out of range: {limit}"
        result = self._request("GET", "/skills", params={"limit": limit})
        assert isinstance(result, list), "skills list must be an array"
        return result

    def skill_upsert(self, body: dict[str, Any]) -> dict[str, Any]:
        assert body.get("name"), "skill upsert needs a name"
        result = self._request("POST", self._org("/skills"), json_body=body)
        assert isinstance(result, dict), "skill upsert must return an object"
        return result

    def code_graph_upload(self, body: dict[str, Any]) -> dict[str, Any]:
        """Upload a native knowledge-graph.json blob so an org shares one code
        graph. Upserts by (org, project). Requires the org's pro plan."""
        assert body.get("project"), "code graph upload needs a project"
        result = self._request("POST", self._org("/code-graphs"), json_body=body)
        assert isinstance(result, dict), "code graph upload must return an object"
        return result

    def plan(
        self,
        goal: dict[str, Any],
        world: dict[str, Any] | None = None,
        mode: str = "high",
        layered: bool = False,
    ) -> dict[str, Any]:
        """Ask the platform's canonical GOAP planner to sequence skills toward a
        goal. `layered=True` returns Kahn waves (requires the org's pro plan)."""
        assert isinstance(goal, dict) and goal, "goal must be a non-empty object"
        body = {"goal": goal, "world": world or {}, "mode": mode, "layered": layered}
        result = self._request("POST", self._org("/plan"), json_body=body)
        assert isinstance(result, dict), "plan must return an object"
        return result

    # -- perf runs ------------------------------------------------------------------

    def perf_upload(self, body: dict[str, Any]) -> dict[str, Any]:
        assert body.get("suite") and body.get("tool") and body.get("data"), "perf upload needs suite/tool/data"
        result = self._request("POST", self._org("/perf-runs"), json_body=body)
        assert isinstance(result, dict), "perf upload must return an object"
        return result

    def perf_list(self, limit: int = PERF_PAGE_MAX) -> list[dict[str, Any]]:
        assert 1 <= limit <= PERF_PAGE_MAX, f"limit out of range: {limit}"
        result = self._request("GET", self._org("/perf-runs"), params={"limit": limit})
        assert isinstance(result, list), "perf list must be an array"
        return result

    def perf_share(self, run_id: str) -> dict[str, Any]:
        assert run_id, "run_id must be non-empty"
        result = self._request("POST", self._org(f"/perf-runs/{run_id}/share"))
        assert isinstance(result, dict), "perf share must return an object"
        return result
