"""Workspace state management — generalized for all tool suites."""

from __future__ import annotations

import contextlib
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass, field

import polars as pl

from devtools_mcp.models import RunBase
from devtools_mcp.store.provenance import capture_provenance
from devtools_mcp.store.run_store import RunStore


@dataclass
class Workspace:
    """Stores runs and cached DataFrames for any tool suite."""

    workspace_id: str
    name: str
    base_dir: str
    runs: dict[str, RunBase] = field(default_factory=dict)
    raw_files: dict[str, str] = field(default_factory=dict)
    _dataframes: dict[str, pl.DataFrame] = field(default_factory=dict)
    _index: pl.DataFrame | None = field(default=None, repr=False)
    _run_store: RunStore | None = field(default=None, repr=False)

    def _store(self) -> RunStore:
        if self._run_store is None:
            self._run_store = RunStore()
        return self._run_store

    def store_run(
        self,
        result: RunBase,
        raw_path: str = "",
        summary: str = "",
        enrich: bool = True,
    ) -> str:
        """Store a parsed run result. Returns run_id."""
        run_id = result.run_id
        if enrich:
            prov = capture_provenance(result.binary)
            for key, val in prov.items():
                if hasattr(result, key) and not getattr(result, key):
                    setattr(result, key, val)
            result.workspace_id = self.workspace_id
            result.workspace_name = self.name
            # tool version from registry if available — set by caller when known
        if summary:
            result.stored_summary = summary
        self.runs[run_id] = result
        persisted_raw = raw_path
        self._store().persist(
            result,
            raw_path=raw_path,
            summary=summary,
            workspace_id=self.workspace_id,
            workspace_name=self.name,
        )
        disk_raw = self._store().load_raw_path(run_id)
        if disk_raw:
            persisted_raw = disk_raw
        if persisted_raw:
            self.raw_files[run_id] = persisted_raw
        self._dataframes.pop(run_id, None)
        self._index = None
        return run_id

    def hydrate_from_disk(self) -> int:
        """Load persisted runs into memory. Returns count loaded."""
        count = 0
        for run_id in self._store().list_run_ids():
            if run_id in self.runs:
                continue
            result = self._store().load_run(run_id)
            if result is None:
                continue
            result.workspace_id = self.workspace_id
            result.workspace_name = self.name
            self.runs[run_id] = result
            raw = self._store().load_raw_path(run_id)
            if raw:
                self.raw_files[run_id] = raw
            count += 1
        return count

    def get_run(self, run_id: str) -> RunBase:
        """Retrieve a run by ID. Raises KeyError if not found."""
        if run_id not in self.runs:
            loaded = self._store().load_run(run_id)
            if loaded is not None:
                self.runs[run_id] = loaded
                raw = self._store().load_raw_path(run_id)
                if raw:
                    self.raw_files[run_id] = raw
        if run_id not in self.runs:
            raise KeyError(f"Run '{run_id}' not found in workspace '{self.name}'")
        return self.runs[run_id]

    def get_raw_path(self, run_id: str) -> str:
        """Get raw output file path for a run."""
        if run_id not in self.raw_files:
            disk = self._store().load_raw_path(run_id)
            if disk:
                self.raw_files[run_id] = disk
        if run_id not in self.raw_files:
            raise KeyError(f"Raw file for run '{run_id}' not found")
        return self.raw_files[run_id]

    def store_artifact(self, run_id: str, name: str, data: str | bytes) -> str:
        """Write a derived artifact into the persistent run store."""
        assert run_id, "run_id required for artifact"
        assert name and "/" not in name and "\\" not in name, f"bad artifact name: {name!r}"
        path = self._store().store_artifact(run_id, name, data)
        assert os.path.exists(path), f"artifact not written: {path}"
        return path

    def get_dataframe(self, run_id: str) -> pl.DataFrame | None:
        """Get cached or persisted DataFrame for a run."""
        if run_id in self._dataframes:
            return self._dataframes[run_id]
        parquet = self._store().load_parquet(run_id)
        if parquet is not None:
            self._dataframes[run_id] = parquet
            return parquet
        return None

    def cache_dataframe(self, run_id: str, df: pl.DataFrame) -> None:
        """Cache a DataFrame for a run and persist to parquet."""
        self._dataframes[run_id] = df
        with contextlib.suppress(OSError):
            self._store().save_parquet(run_id, df)

    def get_index(self) -> pl.DataFrame | None:
        return self._index

    def set_index(self, index: pl.DataFrame) -> None:
        self._index = index

    def invalidate_index(self) -> None:
        self._index = None

    def delete_run(self, run_id: str) -> bool:
        """Remove run from memory and disk."""
        self.runs.pop(run_id, None)
        self.raw_files.pop(run_id, None)
        self._dataframes.pop(run_id, None)
        self._index = None
        return self._store().delete_run(run_id)

    def list_runs(self) -> list[dict[str, str]]:
        """List all runs with summary info."""
        results = []
        for run_id, run in self.runs.items():
            tags = ",".join(run.tags) if run.tags else ""
            results.append(
                {
                    "run_id": run_id,
                    "suite": run.suite,
                    "tool": run.tool,
                    "binary": run.binary,
                    "timestamp": run.timestamp.isoformat(),
                    "exit_code": str(run.exit_code),
                    "duration": f"{run.duration_seconds:.1f}s",
                    "label": run.label,
                    "tags": tags,
                    "task_key": run.task_key,
                    "workspace_id": run.workspace_id or self.workspace_id,
                    "workspace_name": run.workspace_name or self.name,
                    "git_commit": run.git_commit,
                    "git_branch": run.git_branch,
                }
            )
        return results

    def cleanup(self) -> None:
        """Remove temp session files (persisted runs remain on disk)."""
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir, ignore_errors=True)


@dataclass
class AppContext:
    """Application context: workspaces, debug sessions, tool registry."""

    workspaces: dict[str, Workspace] = field(default_factory=dict)
    registry: object = field(default=None)
    default_workspace_id: str = ""
    viz_server: object = field(default=None)
    tracker: object = field(default=None)
    recipes: object = field(default=None)
    skilldocs: object = field(default=None)
    run_store: RunStore = field(default_factory=RunStore)
    debug_manager: object = field(default=None)

    def get_debug_manager(self):
        """Open (or return) the unified debug session manager."""
        if self.debug_manager is None:
            from devtools_mcp.debug.session import DebugSessionManager

            self.debug_manager = DebugSessionManager()
        return self.debug_manager

    def get_tracker(self):
        """Open (or return) the persistent tracker database."""
        if self.tracker is None:
            from devtools_mcp.tracker import open_tracker

            self.tracker = open_tracker()
        assert self.tracker is not None, "tracker failed to open"
        return self.tracker

    def get_recipes(self):
        """Open (or return) the persistent recipes database."""
        if self.recipes is None:
            from devtools_mcp.recipes import open_recipes

            self.recipes = open_recipes()
        assert self.recipes is not None, "recipes db failed to open"
        return self.recipes

    def get_skilldocs(self):
        """Open (or return) the persistent live-skill (skilldocs) store."""
        if self.skilldocs is None:
            from devtools_mcp.skilldocs import SkillDocStore

            self.skilldocs = SkillDocStore()
        assert self.skilldocs is not None, "skilldocs store failed to open"
        return self.skilldocs

    def get_workspace(self, workspace_id: str | None = None) -> Workspace:
        """Get workspace by ID, or default."""
        wid = workspace_id or self.default_workspace_id
        if wid not in self.workspaces:
            raise KeyError(f"Workspace '{wid}' not found")
        return self.workspaces[wid]

    def create_workspace(self, name: str = "default") -> Workspace:
        """Create a new workspace."""
        ws_id = str(uuid.uuid4())
        base_dir = tempfile.mkdtemp(prefix=f"devtools-mcp-{name}-")
        ws = Workspace(workspace_id=ws_id, name=name, base_dir=base_dir, _run_store=self.run_store)
        self.workspaces[ws_id] = ws
        return ws

    def hydrate_all_runs(self) -> int:
        """Load all persisted runs into the default workspace."""
        ws = self.get_workspace()
        return ws.hydrate_from_disk()

    def cleanup_all(self) -> None:
        """Clean up session temp dirs, viz server, and the SQLite stores — not
        persisted runs."""
        if self.viz_server is not None:
            with contextlib.suppress(Exception):
                self.viz_server.stop()
        if self.tracker is not None:
            with contextlib.suppress(Exception):
                self.tracker.close()
            self.tracker = None
        if self.recipes is not None:
            with contextlib.suppress(Exception):
                self.recipes.close()
            self.recipes = None
        if self.skilldocs is not None:
            with contextlib.suppress(Exception):
                self.skilldocs.close()
            self.skilldocs = None
        self._close_debug_manager()
        for ws in self.workspaces.values():
            ws.cleanup()

    def _close_debug_manager(self) -> None:
        """Tear down live debug sessions so adapter subprocesses aren't orphaned.

        DebugSessionManager.stop_all() is async (it awaits per-session DAP
        disconnects). In the server it is already awaited in app_lifespan right
        before cleanup_all, so here (a sync context, usually with the event loop
        still running) we only drop the reference. When cleanup_all is invoked
        standalone with no running loop — e.g. a direct AppContext in a test — we
        best-effort drive stop_all() to completion so subprocesses are reaped.
        """
        mgr = self.debug_manager
        if mgr is None:
            return
        stop_all = getattr(mgr, "stop_all", None)
        if stop_all is not None:
            import asyncio

            try:
                asyncio.get_running_loop()
            except RuntimeError:  # no running loop → safe to drive it ourselves
                with contextlib.suppress(Exception):
                    asyncio.run(stop_all())
        self.debug_manager = None
