"""Station: local-first sync between the tracker and an llm-station-remote
platform.

The platform (FastAPI + Postgres, see C:/code/llm-station-remote) is the
remote backend; local SQLite stays the source of authority. Everything
platform-facing lives here, the only module issuing platform HTTP is
station.client. Integration is HTTP-only by hard rule: this package must
never import llm_station code.

Modules:
    config: per-project rules (.devtools-mcp/station.toml, env precedence)
    client: StationClient, the single HTTP seam
    links: row-level identity map + canonical hashes (echo suppression)
    diff: local change detection (crdt_ops feed + bounded scans)
    engine: run_sync orchestrator (watermarks, pause, sync log)
    domains, per-domain syncers: tasks, coord, claims, skills, perf
"""

from devtools_mcp.station.client import StationClient, StationError
from devtools_mcp.station.config import StationConfig, load_station_config

__all__ = ["StationClient", "StationError", "StationConfig", "load_station_config"]
