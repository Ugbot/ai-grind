"""Shared formatting utilities."""

from __future__ import annotations

from devtools_mcp.models import RunBase


def format_run_header(result: RunBase) -> str:
    """Format a run header with basic info."""
    lines = [
        f"**Tool:** {result.tool}",
        f"**Binary:** {result.binary}",
        f"**Duration:** {result.duration_seconds:.1f}s",
        f"**Exit code:** {result.exit_code}",
        f"**Run ID:** `{result.run_id}`",
    ]
    if result.args:
        lines.insert(2, f"**Args:** {' '.join(result.args)}")
    return "\n".join(lines)


def human_bytes(n: int | float) -> str:
    """Format byte count into human-readable string."""
    if n < 0:
        return f"-{human_bytes(-n)}"
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
