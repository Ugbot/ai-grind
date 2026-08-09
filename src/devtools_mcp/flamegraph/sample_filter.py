"""Stack-containment filtering: keep or drop WHOLE samples by what's in the stack.

Row-level filters (`FilterSpec.function_pattern` / `exclude_functions`) filter the
RESULT TABLE by function name. That cannot express the question profiling actually
asks, which is about the CALL PATH a sample came from, not the name of its leaf.

The motivating failure: whole-process profiles of a database benchmark spend ~34%
of samples in a table-load phase. Aggregated raw, `_platform_memmove` ranked #1 at
27% — but 73% of that memmove was underneath `decompress_page`/`decode_chunk`, i.e.
loading, not executing. Excluding the function would have deleted the memmove that
mattered too; excluding the *phase* (any sample whose stack contains a loader frame)
is the only filter that answers "where does EXECUTION time go".

Filtering happens on samples, BEFORE any percentage is computed, so shares always
describe the filtered universe: drop 34% of a run and the remainder renormalizes to
100%, rather than summing to 66% and quietly understating everything.

Patterns are case-insensitive regexes matched against every frame of a stack
(`re.search`, so they are substring-style like the row filters' `str.contains`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from devtools_mcp.models import StackSample


@dataclass(frozen=True)
class StackFilter:
    """What a stack-containment filter kept and dropped.

    Carried into report headers so a filtered view can never be mistaken for the
    whole picture — the single most dangerous way to misread a profile.

    Counts come in two flavours because both matter: `stacks` is how many distinct
    call paths survived (shape), `weight` is how many samples they carry (cost).
    """

    kept_stacks: int = 0
    dropped_stacks: int = 0
    kept_weight: int = 0
    dropped_weight: int = 0
    stack_include: str | None = None
    stack_exclude: str | None = None

    @property
    def active(self) -> bool:
        """True when a pattern was supplied (so the view is a subset, not the whole)."""
        return self.stack_include is not None or self.stack_exclude is not None

    @property
    def total_weight(self) -> int:
        """Sample weight before filtering — the denominator the raw profile used."""
        return self.kept_weight + self.dropped_weight

    @property
    def dropped_pct(self) -> float:
        """Share of the original run's weight this filter removed."""
        total = self.total_weight
        return round(100.0 * self.dropped_weight / total, 1) if total else 0.0

    def __add__(self, other: StackFilter) -> StackFilter:
        """Accumulate across runs — devtools_aggregate filters each run separately.

        Patterns are taken from whichever side has them (they are the same
        user-supplied pair in every real use).
        """
        return StackFilter(
            kept_stacks=self.kept_stacks + other.kept_stacks,
            dropped_stacks=self.dropped_stacks + other.dropped_stacks,
            kept_weight=self.kept_weight + other.kept_weight,
            dropped_weight=self.dropped_weight + other.dropped_weight,
            stack_include=self.stack_include or other.stack_include,
            stack_exclude=self.stack_exclude or other.stack_exclude,
        )

    def describe(self) -> str:
        """One-line provenance for a report header; empty when no filter ran."""
        if not self.active:
            return ""
        parts = [
            f"stack-filtered: kept {self.kept_weight:,} of {self.total_weight:,} "
            f"samples ({self.dropped_weight:,} dropped, {self.dropped_pct}%)"
        ]
        if self.stack_include is not None:
            parts.append(f"include={self.stack_include!r}")
        if self.stack_exclude is not None:
            parts.append(f"exclude={self.stack_exclude!r}")
        parts.append("percentages renormalized to the kept subset")
        return " · ".join(parts)


def filter_samples(
    samples: list[StackSample],
    stack_include: str | None = None,
    stack_exclude: str | None = None,
) -> tuple[list[StackSample], StackFilter]:
    """Keep or drop whole samples by what appears ANYWHERE in their stack.

    A sample is kept when it matches `stack_include` (if given) on at least one
    frame AND matches `stack_exclude` (if given) on no frame. Exclusion wins:
    a sample matching both is dropped, because exclusion names a phase you have
    decided is not the subject of the measurement.

    Args:
        samples: StackSamples from any sampling backend, root-first frames.
        stack_include: Case-insensitive regex; keep only samples whose stack
            contains a matching frame (e.g. narrow to one subsystem's call paths).
        stack_exclude: Case-insensitive regex; drop samples whose stack contains
            a matching frame (e.g. `"load_parquet|parquet_read_file"` to remove a
            setup phase from a whole-process profile).

    Returns:
        (kept samples, StackFilter describing the cut). With no patterns the
        input list is returned unchanged and the filter reports itself inactive.

    Raises:
        re.error: if a pattern is not a valid regex. Deliberately not swallowed —
            silently ignoring a bad filter would present the unfiltered profile as
            if it had been filtered.
    """
    assert isinstance(samples, list), "samples must be a list"

    if stack_include is None and stack_exclude is None:
        return samples, StackFilter(
            kept_stacks=len(samples),
            kept_weight=sum(s.weight for s in samples),
        )

    inc = re.compile(stack_include, re.IGNORECASE) if stack_include is not None else None
    exc = re.compile(stack_exclude, re.IGNORECASE) if stack_exclude is not None else None

    kept: list[StackSample] = []
    dropped_stacks = 0
    dropped_weight = 0
    kept_weight = 0
    for sample in samples:
        frames = sample.frames
        keep = True
        if inc is not None:
            keep = any(inc.search(f) for f in frames)
        if keep and exc is not None:
            keep = not any(exc.search(f) for f in frames)
        if keep:
            kept.append(sample)
            kept_weight += sample.weight
        else:
            dropped_stacks += 1
            dropped_weight += sample.weight

    assert len(kept) + dropped_stacks == len(samples), "sample accounting lost a stack"
    return kept, StackFilter(
        kept_stacks=len(kept),
        dropped_stacks=dropped_stacks,
        kept_weight=kept_weight,
        dropped_weight=dropped_weight,
        stack_include=stack_include,
        stack_exclude=stack_exclude,
    )
