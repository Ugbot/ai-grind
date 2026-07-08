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


class RunStore:
    """Persists runs under ~/.devtools-mcp/runs/{run_id}/."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or data_root()
        self.runs_path = self.root / "runs"
        self.runs_path.mkdir(parents=True, exist_ok=True)

    def _run_dir(self, run_id: str) -> Path:
        return self.runs_path / run_id

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
        ids = [p.name for p in self.runs_path.iterdir() if p.is_dir() and (p / "result.json").is_file()]
        return sorted(ids)

    def delete_run(self, run_id: str) -> bool:
        dest = self._run_dir(run_id)
        if not dest.is_dir():
            return False
        shutil.rmtree(dest, ignore_errors=True)
        return True

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
        task_key = task_key.strip().upper()
        out: list[str] = []
        for run_id in self.list_run_ids():
            meta_path = self._run_dir(run_id) / "meta.json"
            if not meta_path.is_file():
                continue
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if str(meta.get("task_key", "")).upper() == task_key:
                out.append(run_id)
        return out

    def _prune_if_needed(self) -> None:
        max_runs = int(os.environ.get("DEVTOOLS_MCP_MAX_RUNS", "0") or "0")
        if max_runs <= 0:
            return
        ids = self.list_run_ids()
        if len(ids) <= max_runs:
            return
        for run_id in ids[: len(ids) - max_runs]:
            self.delete_run(run_id)
