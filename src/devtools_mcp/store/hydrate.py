"""Deserialize persisted run results back into Pydantic models."""

from __future__ import annotations

import importlib
from typing import Any

from devtools_mcp.models import RunBase


def hydrate_result(data: dict[str, Any]) -> RunBase:
    """Rebuild a suite-specific result model from stored JSON."""
    module_name = data.get("_module") or ""
    class_name = data.get("_class") or ""
    if module_name and class_name:
        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            if issubclass(cls, RunBase):
                clean = {k: v for k, v in data.items() if not k.startswith("_")}
                return cls.model_validate(clean)
        except (ImportError, AttributeError, TypeError, ValueError):
            pass
    clean = {k: v for k, v in data.items() if not k.startswith("_")}
    return RunBase.model_validate(clean)


def serialize_result(result: RunBase) -> dict[str, Any]:
    """JSON-safe dict including type metadata for round-trip."""
    data = result.model_dump(mode="json")
    data["_class"] = type(result).__name__
    data["_module"] = type(result).__module__
    return data
