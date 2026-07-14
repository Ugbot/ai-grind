"""Per-domain station syncers.

Each module exposes `sync(db, client, rule, project_row, state_row, dry_run)
-> dict` returning bounded counters (pushed/pulled/conflicts/skipped/errors
/deferred + optional notes). The engine owns watermarks, pause logic, and
the sync log; domains own their algorithms.
"""

from devtools_mcp.station.domains import claims, codegraph, coord, perf, skills, tasks

DOMAIN_SYNCERS = {
    "tasks": tasks.sync,
    "sessions": coord.sync,
    "collab": claims.sync,
    "skills": skills.sync,
    "perf": perf.sync,
    "codegraph": codegraph.sync,
}

assert set(DOMAIN_SYNCERS) == {"tasks", "sessions", "collab", "skills", "perf", "codegraph"}, "syncer registry drifted"
