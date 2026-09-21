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

    if cfg.rule("codegraph").direction != "push":
        report["errors"] += 1
        report["notes"].append("codegraph supports push direction only")
        return report

    path = os.environ.get(ENV_CODEGRAPH_JSON, "").strip()
    if not path or not Path(path).is_file():
        report["skipped"] += 1
        report["notes"].append(f"no graph export (set {ENV_CODEGRAPH_JSON} to a knowledge-graph.json)")
        return report

    with Path(path).open("rb") as source:
        data = source.read(BODY_MAX_BYTES + 1)
    if len(data) > BODY_MAX_BYTES:
        report["errors"] += 1
        report["notes"].append("graph export exceeds size bound")
        return report
    try:
        raw = data.decode("utf-8")
        obj = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        report["errors"] += 1
        report["notes"].append("graph export is not valid JSON")
        return report

    if not isinstance(obj, dict) or not isinstance(obj.get("nodes"), list) or not isinstance(obj.get("edges"), list):
        report["errors"] += 1
        report["notes"].append("graph export needs nodes and edges arrays")
        return report

    project = str(project_row["remote_project_id"])
    local_id = json.dumps([project_row["org_id"], project])
    nodes = len(obj.get("nodes") or [])
    edges = len(obj.get("edges") or [])
    sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    link = links.get_link(db.conn, "codegraph", local_id)
    if link is not None and link["synced_hash"] == sha:
        report["skipped"] += 1
        return report
    if not dry_run:
        remote = client.code_graph_upload({"project": project, "body": raw, "node_count": nodes, "edge_count": edges})
        links.insert_link(db, "codegraph", local_id, str(remote["id"]), project_row["org_id"], None, sha)
    report["pushed"] += 1
    return report
