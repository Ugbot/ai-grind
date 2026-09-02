"""Persistent run catalog on disk."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import polars as pl

from devtools_mcp.models import RunBase
from devtools_mcp.store.hydrate import hydrate_result, serialize_result
from devtools_mcp.store.paths import data_root
from devtools_mcp.store.run_index import RunIndex, resolve_db_path


class RunStore:
    """Persists runs under ~/.devtools-mcp/runs/{run_id}/.

    Blobs live on disk; a small SQLite index (runs.db) makes id/task_key lookups
    O(index) instead of scanning + JSON-parsing every meta.json. The index is
    self-healing: any run dir present on disk but missing from it is backfilled
    on first read (so runs written before the index existed still resolve).
    """

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or data_root()
        self.runs_path = self.root / "runs"
        self.runs_path.mkdir(parents=True, exist_ok=True)
        self._index: RunIndex | None = None
        self._index_reconciled = False

    def _index_db(self) -> RunIndex:
        if self._index is None:
            # Explicit root → index sits beside this store's runs dir; else the
            # env override / shared data root wins (resolve_db_path).
            path = resolve_db_path(self.root) if self.root != data_root() else resolve_db_path(None)
            self._index = RunIndex(path)
        return self._index

    def _run_dir(self, run_id: str) -> Path:
        return self.runs_path / run_id

    def _index_fields_from_meta(self, run_id: str) -> dict[str, str]:
        """Pull the indexed columns out of a run's meta.json (disk fallback)."""
        meta_path = self._run_dir(run_id) / "meta.json"
        if not meta_path.is_file():
            return {}
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        raw_tags = meta.get("tags") or []
        tags = ",".join(str(t) for t in raw_tags) if isinstance(raw_tags, list) else str(raw_tags)
        return {
            "suite": str(meta.get("suite", "")),
            "tool": str(meta.get("tool", "")),
            "task_key": str(meta.get("task_key", "")),
            "git_commit": str(meta.get("git_commit", "")),
            "created_at": str(meta.get("timestamp", "")),
            "tags": tags,
            "workspace": str(meta.get("workspace_id", "") or meta.get("workspace_name", "")),
        }

    def _index_run(self, run_id: str) -> None:
        fields = self._index_fields_from_meta(run_id)
        if fields:
            self._index_db().upsert(run_id, **fields)  # type: ignore[arg-type]

    def _reconcile_index(self) -> None:
        """One-shot backfill: index any on-disk run dir the index hasn't seen."""
        if self._index_reconciled:
            return
        self._index_reconciled = True
        if not self.runs_path.is_dir():
            return
        known = self._index_db().indexed_ids()
        for p in self.runs_path.iterdir():
            if p.is_dir() and p.name not in known and (p / "result.json").is_file():
                self._index_run(p.name)

    def persist(
        self,
        result: RunBase,
        raw_path: str = "",
        summary: str = "",
        workspace_id: str = "",
        workspace_name: str = "",
    ) -> str:
        """Write run to disk. Returns run_id."""
        run_id = result.run_id
        dest = self._run_dir(run_id)
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "raw").mkdir(exist_ok=True)
        (dest / "artifacts").mkdir(exist_ok=True)

        if raw_path and os.path.isfile(raw_path):
            raw_name = Path(raw_path).name or "output.txt"
            target = dest / "raw" / raw_name
            if Path(raw_path).resolve() != target.resolve():
                shutil.copy2(raw_path, target)
            result_raw = str(target)
        else:
            inline = getattr(result, "raw_output", "") or ""
            result_raw = ""
            if inline:
                target = dest / "raw" / "output.txt"
                target.write_text(inline, encoding="utf-8")
                result_raw = str(target)

        meta = result.model_dump(mode="json")
        meta["raw_file"] = result_raw
        meta["workspace_id"] = workspace_id
        meta["workspace_name"] = workspace_name
        if summary:
            meta["stored_summary"] = summary
        (dest / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        (dest / "result.json").write_text(json.dumps(serialize_result(result), indent=2), encoding="utf-8")
        if summary:
            (dest / "summary.md").write_text(summary, encoding="utf-8")

        self._index_db().upsert(
            run_id,
            suite=result.suite,
            tool=result.tool,
            task_key=result.task_key,
            git_commit=result.git_commit,
            created_at=result.timestamp.isoformat(),
            tags=",".join(result.tags),
            workspace=workspace_id or workspace_name,
        )
        self._prune_if_needed()
        return run_id

    def load_run(self, run_id: str) -> RunBase | None:
        path = self._run_dir(run_id) / "result.json"
        if not path.is_file():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return hydrate_result(data)

    def load_raw_path(self, run_id: str) -> str:
        raw_dir = self._run_dir(run_id) / "raw"
        if not raw_dir.is_dir():
            return ""
        files = sorted(raw_dir.iterdir())
        if not files:
            return ""
        return str(files[0])

    def load_summary(self, run_id: str) -> str:
        summary_path = self._run_dir(run_id) / "summary.md"
        if summary_path.is_file():
            return summary_path.read_text(encoding="utf-8")
        meta_path = self._run_dir(run_id) / "meta.json"
        if meta_path.is_file():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            return str(meta.get("stored_summary") or "")
        return ""

    def load_parquet(self, run_id: str) -> pl.DataFrame | None:
        path = self._run_dir(run_id) / "data.parquet"
        if path.is_file():
            return pl.read_parquet(path)
        return None

    def save_parquet(self, run_id: str, df: pl.DataFrame) -> None:
        dest = self._run_dir(run_id)
        dest.mkdir(parents=True, exist_ok=True)
        df.write_parquet(dest / "data.parquet")

    def store_artifact(self, run_id: str, name: str, data: str | bytes) -> str:
        dest = self._run_dir(run_id) / "artifacts"
        dest.mkdir(parents=True, exist_ok=True)
        path = dest / name
        if isinstance(data, bytes):
            path.write_bytes(data)
        else:
            path.write_text(data, encoding="utf-8")
        return str(path)

    def list_run_ids(self) -> list[str]:
        if not self.runs_path.is_dir():
            return []
        self._reconcile_index()
        return self._index_db().list_ids()

    def delete_run(self, run_id: str) -> bool:
        dest = self._run_dir(run_id)
        existed = dest.is_dir()
        if existed:
            shutil.rmtree(dest, ignore_errors=True)
        self._index_db().delete(run_id)
        return existed

    def export_bundle(self, run_id: str, dest_zip: Path) -> str:
        import zipfile

        src = self._run_dir(run_id)
        if not src.is_dir():
            raise FileNotFoundError(run_id)
        dest_zip.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(dest_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for file in src.rglob("*"):
                if file.is_file():
                    zf.write(file, file.relative_to(src))
        return str(dest_zip)

    def runs_for_task_key(self, task_key: str) -> list[str]:
        if not self.runs_path.is_dir():
            return []
        self._reconcile_index()
        return self._index_db().ids_for_task_key(task_key)

    def close(self) -> None:
        """Close the SQLite index connection (idempotent). Blobs are untouched."""
        if self._index is not None:
            self._index.close()
            self._index = None

    def _run_mtime(self, run_id: str) -> float:
        """Directory mtime as an age key; missing/racing dirs sort newest (kept)."""
        try:
            return self._run_dir(run_id).stat().st_mtime
        except OSError:
            return float("inf")

    def _prune_if_needed(self) -> None:
        max_runs = int(os.environ.get("DEVTOOLS_MCP_MAX_RUNS", "0") or "0")
        if max_runs <= 0:
            return
        ids = self.list_run_ids()
        if len(ids) <= max_runs:
            return
        # Evict oldest first, run_ids are UUIDs, so lexicographic order is not age order.
        by_age = sorted(ids, key=self._run_mtime)
        for run_id in by_age[: len(ids) - max_runs]:
            self.delete_run(run_id)
