"""Workspace state management — generalized for all tool suites."""

from __future__ import annotations

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
        """Clean up all workspaces and sessions."""
        for ws in self.workspaces.values():
            ws.cleanup()
