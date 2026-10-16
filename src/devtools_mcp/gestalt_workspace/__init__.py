"""Bounded portable Gestalt example reconstruction, never notebook execution."""

from __future__ import annotations

import asyncio
from typing import Any

from .bundle import Bundle
from .common import WorkspaceError, canonical, decode, digest, endpoint, limits_config, require, resource_id
from .gateway import Gateway

__all__ = ["Bundle", "canonical", "digest", "limits_config", "reconstruct"]
_ADMISSION = asyncio.Lock()


def resolved(bundle: Bundle, identifiers: dict[str, str]) -> dict[str, Any]:
    text = canonical(bundle.dashboard["body"]).decode("utf-8")
    for key, identifier in identifiers.items():
        text = text.replace("{{" + key + "}}", identifier)
    return decode(text.encode("utf-8"))


def receipt(asset: dict[str, Any], base: str, state: str, identifier: str | None = None) -> dict[str, Any]:
    kind = asset["kind"]
    return {
        "kind": kind,
        "name": asset["name"],
        "id": identifier,
        "body_sha256": digest(asset["body"]),
        "state": state,
        "url": f"{base}/{kind}s/{identifier}" if identifier else None,
    }


def _run(action: str, directory: str, base_url: str, limits: dict[str, Any] | None) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "outcome": "error", "action": action, "assets": []}
    current: dict[str, Any] | None = None
    try:
        require(type(action) is str and action in {"inspect", "install", "verify"}, "unsupported action")
        config = limits_config(limits)
        host, port, base = endpoint(base_url)
        bundle = Bundle(directory, config)
        result["namespace"] = bundle.namespace
        gateway = Gateway(host, port, config)
        listings = {kind: gateway.listed(kind, bundle.prefix) for kind in ("notebook", "dashboard")}
        identifiers: dict[str, str] = {}
        plan: list[tuple[dict[str, Any], str | None]] = []
        for asset in bundle.notebooks:
            matches = listings["notebook"].get(asset["name"], [])
            require(len(matches) <= 1, "duplicate resource name; installation refused", "conflict")
            identifier = matches[0] if matches else None
            if identifier:
                require(
                    canonical(gateway.get("notebook", identifier, asset["name"])) == canonical(asset["body"]),
                    "edited notebook body; installation refused",
                    "conflict",
                )
                identifiers[asset["key"]] = identifier
            plan.append((asset, identifier))
            result["assets"].append(receipt(asset, base, "reused" if identifier else "missing", identifier))
        dashboard = dict(bundle.dashboard)
        matches = listings["dashboard"].get(dashboard["name"], [])
        require(len(matches) <= 1, "duplicate dashboard name; installation refused", "conflict")
        dashboard_id = matches[0] if matches else None
        if len(identifiers) == len(bundle.notebooks):
            dashboard["body"] = resolved(bundle, identifiers)
        if dashboard_id:
            existing = gateway.get("dashboard", dashboard_id, dashboard["name"])
            require(
                len(identifiers) == len(bundle.notebooks) and canonical(existing) == canonical(dashboard["body"]),
                "edited/unresolvable dashboard body; installation refused",
                "conflict",
            )
        result["assets"].append(receipt(dashboard, base, "reused" if dashboard_id else "missing", dashboard_id))
        if action != "install":
            complete = all(item["state"] == "reused" for item in result["assets"])
            result.update(ok=complete if action == "verify" else True, outcome="complete" if complete else "incomplete")
            return result
        plan.append((dashboard, dashboard_id))
        for index, (asset, identifier) in enumerate(plan):
            if asset["kind"] == "dashboard":
                asset["body"] = resolved(bundle, identifiers)
            current = receipt(asset, base, "reused" if identifier else "creating", identifier)
            result["assets"][index] = current
            if identifier is None:
                data = gateway.call(
                    f"{asset['kind']}.create", {"name": asset["name"], "body": canonical(asset["body"]).decode()}
                )
                try:
                    identifier = resource_id(data.get("id"))
                except WorkspaceError as exc:
                    exc.uncertain = True
                    raise
                current.update(id=identifier, state="created_unverified", url=f"{base}/{asset['kind']}s/{identifier}")
            actual = gateway.get(asset["kind"], identifier, asset["name"])
            require(canonical(actual) == canonical(asset["body"]), "canonical readback mismatch", "readback_mismatch")
            current["state"] = "created" if current["state"] == "created_unverified" else "reused"
            if asset["kind"] == "notebook":
                identifiers[asset["key"]] = identifier
        result.update(ok=True, outcome="complete")
    except WorkspaceError as exc:
        if exc.uncertain and current is not None:
            current["state"] = "uncertain"
        result.update(outcome="uncertain" if exc.uncertain else "error", error={"code": exc.code, "message": str(exc)})
    except (OSError, ValueError, TypeError, RecursionError):
        result["error"] = {"code": "invalid_bundle", "message": "bundle inaccessible or invalid"}
    return result


async def reconstruct(
    action: str, bundle_dir: str, base_url: str, limits: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Serialize the full operation; cancellation waits for its bounded worker before releasing admission."""
    async with _ADMISSION:
        task = asyncio.create_task(asyncio.to_thread(_run, action, bundle_dir, base_url, limits))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
            raise
