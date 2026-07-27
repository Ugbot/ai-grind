"""Code-graph domain: push the native knowledge-graph.json export to the platform.

LLM Station builds the graph natively and `graph_export` writes it to a JSON file;
point `DEVTOOLS_MCP_CODEGRAPH_JSON` at that file and this syncer uploads it (hash-
diffed, one current graph per project). Org-scoped, premium (the platform gates it).
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from devtools_mcp.station import links

ENV_CODEGRAPH_JSON: str = "DEVTOOLS_MCP_CODEGRAPH_JSON"
BODY_MAX_BYTES: int = 32 * 1024 * 1024  # 32 MiB


def sync(db, client, cfg, project_row, state_row, dry_run) -> dict:  # noqa: ANN001 - domain-syncer contract
    """Push the local graph export if its hash differs from the last sync."""
    report: dict = {
        "domain": "codegraph",
        "pushed": 0,
        "pulled": 0,
        "conflicts": 0,
        "skipped": 0,
        "errors": 0,
        "notes": [],
    }
    assert project_row["org_id"], "unlinked project reached codegraph sync"

    path = os.environ.get(ENV_CODEGRAPH_JSON, "").strip()
    if not path or not Path(path).is_file():
        report["skipped"] += 1
        report["notes"].append(f"no graph export (set {ENV_CODEGRAPH_JSON} to a knowledge-graph.json)")
        return report

    raw = Path(path).read_text(encoding="utf-8", errors="replace")
    if len(raw.encode("utf-8")) > BODY_MAX_BYTES:
        report["errors"] += 1
        report["notes"].append("graph export exceeds size bound")
        return report
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        report["errors"] += 1
        report["notes"].append("graph export is not valid JSON")
        return report

    project = str(obj.get("project") or "default")
    nodes = len(obj.get("nodes") or [])
    edges = len(obj.get("edges") or [])
    sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    link = links.get_link(db.conn, "codegraph", project)
    if link is not None and link["synced_hash"] == sha:
        report["skipped"] += 1
        return report
    if not dry_run:
        remote = client.code_graph_upload({"project": project, "body": raw, "node_count": nodes, "edge_count": edges})
        links.insert_link(db, "codegraph", project, str(remote["id"]), project_row["org_id"], None, sha)
    report["pushed"] += 1
    return report
