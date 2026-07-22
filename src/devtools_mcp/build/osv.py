"""OSV.dev advisory lookup over an already-parsed dependency list.

The JVM build tools have no built-in audit command. Instead of injecting an
audit plugin into the target build (breaks offline, breaks under Gradle
dependency verification), `query_osv` takes the Dependency rows a deps parse
already produced and asks https://osv.dev in one batched query. Needs network
at audit time; any failure is one clear line, never a flood.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence

from devtools_mcp.build.models import Dependency, Vulnerability

_API = "https://api.osv.dev/v1"
BATCH_LIMIT = 1000  # querybatch hard cap per request
DETAIL_CAP = 50  # bound: per-advisory detail fetches; rows past it stay id-only

# fetch(url, payload, timeout) -> parsed JSON. Injectable so tests never hit the network.
Fetch = Callable[[str, "bytes | None", float], object]


def _http_json(url: str, payload: bytes | None, timeout: float) -> object:
    """POST (payload given) or GET (payload None) a JSON endpoint."""
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "devtools-mcp"},
        method="POST" if payload is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed https host
        return json.loads(resp.read().decode("utf-8", "replace"))


def dedupe_packages(deps: Sequence[Dependency]) -> list[tuple[str, str]]:
    """Unique (name, version) query targets; Maven-style name = group:artifact."""
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for d in deps:
        version = d.resolved or d.version
        if d.omitted or not version or not d.artifact or d.artifact.startswith("project "):
            continue
        name = f"{d.group}:{d.artifact}" if d.group else d.artifact
        key = (name, version)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _bucket_score(score: float) -> str:
    if score >= 9:
        return "critical"
    if score >= 7:
        return "high"
    if score >= 4:
        return "moderate"
    return "low"


def bucket_severity(detail: dict) -> str:
    """OSV detail -> critical|high|moderate|low|unknown (GHSA database_specific first)."""
    raw = str((detail.get("database_specific") or {}).get("severity", "")).lower()
    if raw == "medium":
        return "moderate"
    if raw in ("critical", "high", "moderate", "low"):
        return raw
    for sev in detail.get("severity") or []:
        try:
            return _bucket_score(float(str(sev.get("score", ""))))
        except ValueError:
            continue  # CVSS vector strings land here; database_specific covers GHSA
    return "unknown"


def _fix_available(detail: dict) -> bool:
    for aff in detail.get("affected") or []:
        for rng in aff.get("ranges") or []:
            if any("fixed" in ev for ev in rng.get("events") or []):
                return True
    return False


def query_osv(
    deps: Sequence[Dependency],
    ecosystem: str = "Maven",
    timeout: float = 20.0,
    fetch: Fetch = _http_json,
) -> tuple[list[Vulnerability], list[str]]:
    """Map dependency rows onto OSV advisories.

    Returns (vulnerabilities, errors). One row per (package, advisory); the
    advisory id rides in `vulnerable_range` (cargo-audit precedent). Details
    (severity/title/fix) are fetched for at most DETAIL_CAP distinct advisories;
    rows past the cap keep the id + url with severity `unknown`.
    """
    packages = dedupe_packages(deps)
    if not packages:
        return [], []

    hits: list[tuple[str, str, str]] = []  # (name, version, advisory id)
    details: dict[str, dict] = {}
    try:
        for start in range(0, len(packages), BATCH_LIMIT):
            chunk = packages[start : start + BATCH_LIMIT]
            payload = json.dumps(
                {"queries": [{"package": {"ecosystem": ecosystem, "name": n}, "version": v} for n, v in chunk]}
            ).encode("utf-8")
            data = fetch(f"{_API}/querybatch", payload, timeout)
            results = data.get("results") or [] if isinstance(data, dict) else []
            for (name, version), res in zip(chunk, results):
                for v in (res or {}).get("vulns") or []:
                    vid = str(v.get("id", ""))
                    if vid:
                        hits.append((name, version, vid))
        ids = list(dict.fromkeys(vid for _, _, vid in hits))
        for vid in ids[:DETAIL_CAP]:
            detail = fetch(f"{_API}/vulns/{vid}", None, timeout)
            if isinstance(detail, dict):
                details[vid] = detail
    except (urllib.error.URLError, OSError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        return [], [f"OSV query failed: {exc}"]

    vulns: list[Vulnerability] = []
    for name, version, vid in hits:
        detail = details.get(vid)
        vulns.append(
            Vulnerability(
                name=name,
                severity=bucket_severity(detail) if detail else "unknown",
                version=version,
                vulnerable_range=vid,
                title=str(detail.get("summary", "")) if detail else "details not fetched (capped)",
                url=f"https://osv.dev/vulnerability/{vid}",
                fix_available=_fix_available(detail) if detail else False,
            )
        )
    return vulns, []
