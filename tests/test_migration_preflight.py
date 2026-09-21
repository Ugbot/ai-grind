"""Migration must expose unsupported state and never change the live store."""

from devtools_mcp.station.migration import preflight
from devtools_mcp.tracker import tasks
from devtools_mcp.tracker.db import open_tracker


def test_preflight_preserves_database_and_reports_replica_state(tmp_path):
    path = tmp_path / "tracker.db"
    db = open_tracker(path)
    tasks.create_project(db, "TEST", "Test")
    tasks.create_task(db, "TEST", "Keep identity")
    db.close()
    before = path.read_bytes()
    result = preflight(path)
    assert result["importable_counts"]["tasks"] == 1
    assert result["unsupported_counts"]["crdt_ops"] > 0
    assert not result["ownership_transfer_ready"]
    assert path.read_bytes() == before
