"""DBOS Transact bootstrap — the durable executor for recipe step sequences.

DBOS is a process-global singleton: one `DBOS(config=...)` instance, launched
once, backed by a local SQLite *system database* that checkpoints every
`@DBOS.workflow`/`@DBOS.step`. That checkpoint log is what lets an interrupted
recipe resume from its last completed step instead of re-running.

This module owns that singleton's lifecycle so the rest of the codebase never
touches DBOS construction directly:

    launch_dbos()   — construct (idempotently) + launch; safe to call from
                      every server-startup path and from the console handler.
    destroy_dbos()  — tear the singleton down (tests, clean shutdown).

The system DB path defaults to ``~/.devtools-mcp/dbos.db`` and is overridable
via ``DEVTOOLS_MCP_DBOS_DB`` (the test suite points it at a temp file). The
domain recipes DB (recipes.db) is a *separate* SQLite file — DBOS's DB is the
durability layer, the domain DB stays the human-facing model (console + tools).
"""

from __future__ import annotations

import threading
from pathlib import Path

from dbos import DBOS, DBOSConfig

from devtools_mcp.recipes.db import resolve_dbos_db_path

DBOS_APP_NAME: str = "devtools-mcp"

_lock = threading.Lock()
_instance: DBOS | None = None
_launched: bool = False


def _system_database_url(path: Path) -> str:
    """SQLAlchemy URL for the local SQLite system database at `path`."""
    assert path.name, f"dbos db path has no filename: {path!r}"
    return f"sqlite:///{path}"


def get_dbos() -> DBOS:
    """Return the process-global DBOS instance, constructing it once on demand.

    Construction reads the system-DB path lazily (env override honoured), so the
    test suite can point ``DEVTOOLS_MCP_DBOS_DB`` at a temp file before the first
    call. Does NOT launch — call `launch_dbos()` for that.
    """
    global _instance
    with _lock:
        if _instance is None:
            path = resolve_dbos_db_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            config: DBOSConfig = {
                "name": DBOS_APP_NAME,
                "system_database_url": _system_database_url(path),
            }
            _instance = DBOS(config=config)
        assert _instance is not None, "DBOS construction failed"
        return _instance


def launch_dbos() -> DBOS:
    """Construct (if needed) and launch DBOS exactly once. Idempotent.

    Safe to call from every entrypoint (MCP lifespan, service-mode boot, the
    dashboard's trigger-run handler) — the first call launches, the rest are
    no-ops. Importing the recipe runner (which applies the @workflow/@step
    decorators) must happen before launch so the workflow is registered.
    """
    global _launched
    dbos = get_dbos()
    with _lock:
        if not _launched:
            # Ensure the durable workflow/step decorators are registered.
            import devtools_mcp.recipes.runner  # noqa: F401

            DBOS.launch()
            _launched = True
    return dbos


def destroy_dbos() -> None:
    """Tear down the DBOS singleton (idempotent). For tests and clean shutdown."""
    global _instance, _launched
    with _lock:
        if _instance is not None or _launched:
            DBOS.destroy()
        _instance = None
        _launched = False


def is_launched() -> bool:
    """True once `launch_dbos()` has launched the singleton (test/introspection)."""
    return _launched
