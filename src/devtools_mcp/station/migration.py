"""Read-only migration preflight for the existing native tracker importer.

The importer does not transfer replica/platform/collaboration state. Report those
records before a user considers adoption; this module never changes ownership.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from contextlib import closing
from pathlib import Path

IMPORTED_TABLES = (
    "projects",
    "tasks",
    "acceptance_criteria",
    "task_deps",
    "task_tags",
    "tags",
    "tag_rules",
    "task_commits",
    "external_refs",
)


def preflight(path: Path) -> dict:
    """Inspect a tracker without migrations, credential reads, or writable opens."""
    source = path.resolve(strict=True)
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as conn:
        conn.execute("PRAGMA query_only=ON")
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"projects", "tasks"}.issubset(tables):
            raise ValueError("not an ai-grind tracker database")
        if len(tables) > 256:
            raise ValueError("tracker contains too many tables to audit")
        counts = {}
        for table in sorted(tables):
            if table == "schema_migrations" or table.startswith("sqlite_"):
                continue
            identifier = table.replace('"', '""')
            counts[table] = conn.execute(f'SELECT COUNT(*) FROM "{identifier}"').fetchone()[0]
    unsupported = {table: count for table, count in counts.items() if table not in IMPORTED_TABLES and count}
    return {
        "source": str(source),
        "mode": "dry-run",
        "ownership_transfer_ready": False,
        "importable_counts": {table: counts[table] for table in IMPORTED_TABLES if table in counts},
        "unsupported_counts": unsupported,
        "blockers": [
            "Native tracker importer does not transfer replica, platform-link or collaboration state.",
            "Validated backup, reverse migration and exclusive workspace ownership are required before transfer.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tracker", type=Path)
    args = parser.parse_args()
    print(json.dumps(preflight(args.tracker), indent=2))


if __name__ == "__main__":
    main()
