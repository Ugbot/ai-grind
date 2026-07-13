"""External/platform planner backend: delegates to a URL (or the platform) that
speaks the same `{goal, world, mode, layered} -> {steps, layers}` contract. Kept
severable — any failure degrades to a reportable PlanResult, never a crash."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from devtools_mcp.planning.planner import PLAN_STEPS_MAX, PlanResult

_TIMEOUT = 20  # seconds
_RESP_MAX = 1_000_000  # bytes


class RemotePlanner:
    """`target` is a base URL (POST <target>/plan) or the literal 'platform'."""

    def __init__(self, target: str) -> None:
        assert isinstance(target, str) and target, "target required"
        self.target = target.rstrip("/")

    def plan(self, goal: dict, world: dict, mode: str, layered: bool) -> PlanResult:
        if self.target == "platform":
            return self._platform(goal, world, mode, layered)
        return self._http(f"{self.target}/plan", goal, world, mode, layered)

    def _platform(self, goal: dict, world: dict, mode: str, layered: bool) -> PlanResult:
        try:
            from devtools_mcp.station.client import StationClient
        except ImportError:  # pragma: no cover
            return PlanResult(False, "platform", message="station client unavailable")
        try:
            client = StationClient()
            data = client.plan(goal=goal, world=world, mode=mode, layered=layered)
        except Exception as exc:  # noqa: BLE001 — degrade to a report, never crash the tool
            return PlanResult(
                False, "platform", message=f"platform planner unavailable ({exc}); set DEVTOOLS_MCP_PLANNER=local"
            )
        return self._from_payload(data, "platform")

    def _http(self, url: str, goal: dict, world: dict, mode: str, layered: bool) -> PlanResult:
        body = json.dumps({"goal": goal, "world": world, "mode": mode, "layered": layered}).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                data = json.loads(resp.read(_RESP_MAX).decode("utf-8", "replace"))
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            return PlanResult(False, "remote", message=f"external planner {url} unreachable ({exc})")
        return self._from_payload(data, "remote")

    @staticmethod
    def _from_payload(data: object, backend: str) -> PlanResult:
        if not isinstance(data, dict):
            return PlanResult(False, backend, message="planner returned a non-object response")
        steps = [str(s) for s in (data.get("steps") or [])][:PLAN_STEPS_MAX]
        raw_layers = data.get("layers")
        layers = None
        if isinstance(raw_layers, list):
            layers = [[str(s) for s in wave] for wave in raw_layers if isinstance(wave, list)]
        return PlanResult(
            ok=bool(data.get("ok", True)),
            backend=backend,
            steps=steps,
            layers=layers,
            message=str(data.get("message", "")),
        )
