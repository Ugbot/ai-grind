"""Error detail formatters with full stack traces."""

from __future__ import annotations

from devtools_mcp.formatters.utils import human_bytes
from devtools_mcp.valgrind.models import MemcheckError, ThreadError


def format_error_details(
    errors: list[MemcheckError] | list[ThreadError],
    max_errors: int = 10,
) -> str:
    """Format detailed error info including full stack traces."""
    parts: list[str] = []
    for i, err in enumerate(errors[:max_errors]):
        parts.append(f"### Error {i + 1}: {err.kind}")
        parts.append(f"**{err.what}**")
        if isinstance(err, MemcheckError) and err.bytes_leaked:
            parts.append(f"Leaked: {human_bytes(err.bytes_leaked)} in {err.blocks_leaked} block(s)")
        if isinstance(err, ThreadError) and err.thread_id:
            parts.append(f"Thread: {err.thread_id}")

        parts.append("")
        parts.append("**Stack:**")
        for frame in err.stack:
            parts.append(f"  {frame.location}")

        if err.auxwhat:
            parts.append("")
            parts.append(f"**{err.auxwhat}**")

        if err.auxstack:
            parts.append("**Auxiliary stack:**")
            for frame in err.auxstack:
                parts.append(f"  {frame.location}")

        parts.append("")

    if len(errors) > max_errors:
        parts.append(f"... and {len(errors) - max_errors} more error(s)")

    return "\n".join(parts)
