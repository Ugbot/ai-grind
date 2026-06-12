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

    def store_run(self, result: RunBase, raw_path: str = "") -> str:
        """Store a parsed run result. Returns run_id."""
        run_id = result.run_id
        self.runs[run_id] = result
        if raw_path:
            self.raw_files[run_id] = raw_path
        self._dataframes.pop(run_id, None)
        self._index = None  # invalidate unified index
        return run_id

    def get_run(self, run_id: str) -> RunBase:
        """Retrieve a run by ID. Raises KeyError if not found."""
        if run_id not in self.runs:
            raise KeyError(f"Run '{run_id}' not found in workspace '{self.name}'")
        return self.runs[run_id]

    def get_raw_path(self, run_id: str) -> str:
        """Get raw output file path for a run."""
        if run_id not in self.raw_files:
            raise KeyError(f"Raw file for run '{run_id}' not found")
        return self.raw_files[run_id]

    def store_artifact(self, run_id: str, name: str, data: str | bytes) -> str:
        """Write a derived artifact (e.g. a flamegraph SVG) into the workspace.

        Returns the absolute path. Large artifacts live on disk so they are never
        inlined into an LLM response — only the path is handed back.
        """
        assert run_id, "run_id required for artifact"
        assert name and "/" not in name and "\\" not in name, f"bad artifact name: {name!r}"
        os.makedirs(self.base_dir, exist_ok=True)
        path = os.path.join(self.base_dir, f"{run_id}-{name}")
        mode = "wb" if isinstance(data, bytes) else "w"
        with open(path, mode, encoding=None if isinstance(data, bytes) else "utf-8") as f:
            f.write(data)
        assert os.path.exists(path), f"artifact not written: {path}"
        return path

    def get_dataframe(self, run_id: str) -> pl.DataFrame | None:
        """Get cached DataFrame for a run."""
        return self._dataframes.get(run_id)

    def cache_dataframe(self, run_id: str, df: pl.DataFrame) -> None:
        """Cache a DataFrame for a run."""
        self._dataframes[run_id] = df

    def list_runs(self) -> list[dict[str, str]]:
        """List all runs with summary info."""
        results = []
        for run_id, run in self.runs.items():
            results.append(
                {
                    "run_id": run_id,
                    "suite": run.suite,
                    "tool": run.tool,
                    "binary": run.binary,
                    "timestamp": run.timestamp.isoformat(),
                    "exit_code": str(run.exit_code),
                    "duration": f"{run.duration_seconds:.1f}s",
                }
            )
        return results

    def cleanup(self) -> None:
        """Remove temp files."""
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir, ignore_errors=True)


@dataclass
class AppContext:
    """Application context: workspaces, debug sessions, tool registry."""

    workspaces: dict[str, Workspace] = field(default_factory=dict)
    lldb_sessions: dict[str, object] = field(default_factory=dict)  # LldbSession (Phase 2)
    registry: object = field(default=None)  # ToolRegistry, set during lifespan
    default_workspace_id: str = ""
    viz_server: object = field(default=None)  # VizServer, started on demand
    tracker: object = field(default=None)  # TrackerDB, opened lazily on first use

    def get_tracker(self):
        """Open (or return) the persistent tracker database.

        Lazy so server startup stays unchanged and tests can point
        DEVTOOLS_MCP_TRACKER_DB at a temp path before first use.
        """
        if self.tracker is None:
            from devtools_mcp.tracker import open_tracker

            self.tracker = open_tracker()
        assert self.tracker is not None, "tracker failed to open"
        return self.tracker

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
        ws = Workspace(workspace_id=ws_id, name=name, base_dir=base_dir)
        self.workspaces[ws_id] = ws
        return ws

    def cleanup_all(self) -> None:
        """Clean up all workspaces, sessions, the viz server, and the tracker DB."""
        if self.viz_server is not None:
            with contextlib.suppress(Exception):
                self.viz_server.stop()
        if self.tracker is not None:
            # Close releases the WAL sidecar files (matters on Windows temp dirs).
            with contextlib.suppress(Exception):
                self.tracker.close()
            self.tracker = None
        for ws in self.workspaces.values():
            ws.cleanup()
