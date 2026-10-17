"""Same-thread DTrace CPU intervals; residuals are not off-CPU estimates."""

from __future__ import annotations

from .benchmark import EvidenceParseError

_PRODUCER = ("gen_in", "gen_out", "commit_in", "commit_out")
_CONSUMER = ("read_in", "read_out", "observe_in", "stamp", "observe_out")
_SPANS = {
    "generation": ("gen_in", "gen_out"),
    "post_generation_producer": ("gen_out", "commit_in"),
    "commit_bracket": ("commit_in", "commit_out"),
    "reader_validation": ("read_in", "read_out"),
    "downstream": ("read_out", "observe_in"),
    "observer": ("observe_in", "observe_out"),
    "producer_work": ("gen_in", "commit_out"),
    "consumer_work": ("read_in", "observe_out"),
}
_DISCLAIMER = (
    "DTrace vtimestamp is thread CPU accounting, not a synchronized wall-clock sample. "
    "Signed wall-minus-thread-CPU residuals include off-CPU intervals, probe overhead, "
    "clock-accounting and reading differences; they do not identify preemption, "
    "off-CPU time, or an algorithmic cause. Negative residuals are retained. "
    "Reported spans overlap; their residuals must not be summed as elapsed time."
)


def validate_thread_cpu(events: list[dict], total: int) -> dict | None:
    """Validate logical per-thread order, including warm events and EOF."""
    if len(events[1]["gen_in"]) == 2:
        return None
    advances = {}
    for name, phases in (("producer", _PRODUCER), ("consumer", _CONSUMER)):
        first = previous = None
        for event in range(1, total + 1):
            for phase in phases:
                value = events[event][phase][2]
                if previous is not None and value < previous:
                    raise EvidenceParseError(f"{name} thread CPU clock decreased")
                if first is None:
                    first = value
                previous = value
        if name == "consumer":
            for phase in ("observe_in", "observe_out"):
                value = events[total + 1][phase][2]
                assert previous is not None
                if value < previous:
                    raise EvidenceParseError("consumer EOF thread CPU clock decreased")
                previous = value
        assert first is not None and previous is not None
        if previous <= first:
            raise EvidenceParseError(f"{name} thread CPU clock unavailable or did not advance")
        advances[name] = previous - first
    return advances


def _interval(start: tuple, end: tuple) -> dict:
    wall = end[0] - start[0]
    cpu = end[2] - start[2]
    assert wall >= 0 and cpu >= 0  # Both orderings were validated before reporting.
    return {"wall_ns": wall, "thread_cpu_ns": cpu, "wall_minus_thread_cpu_ns": wall - cpu}


def cpu_event_spans(events: list[dict], event: int) -> dict:
    """Only subtract endpoints belonging to the same validated thread."""
    current = events[event]
    result = {name: _interval(current[start], current[end]) for name, (start, end) in _SPANS.items()}
    if event > 1:
        previous = events[event - 1]
        result["prior_producer_gap"] = _interval(previous["commit_out"], current["gen_in"])
        result["prior_consumer_gap"] = _interval(previous["observe_out"], current["read_in"])
    return result


def _stats(values: list[int]) -> dict:
    ordered = sorted(values)
    count = len(ordered)
    assert count > 0
    return {
        "p50_ns": ordered[(count * 50 + 99) // 100 - 1],
        "p99_ns": ordered[(count * 99 + 99) // 100 - 1],
        "max_ns": ordered[-1],
    }


def summarize_thread_cpu(
    events: list[dict], warm: int, total: int, advances: dict | None, worst_events: list[dict]
) -> dict | None:
    if advances is None:
        return None
    phases: dict[str, dict[str, list[int]]] = {}
    for event in range(warm + 1, total + 1):
        for phase, spans in cpu_event_spans(events, event).items():
            columns = phases.setdefault(phase, {key: [] for key in spans})
            for key, value in spans.items():
                columns[key].append(value)
    summaries = {}
    for phase, columns in phases.items():
        negative = sum(value < 0 for value in columns["wall_minus_thread_cpu_ns"])
        summaries[phase] = {
            "count": len(columns["wall_ns"]),
            **{key: _stats(values) for key, values in columns.items()},
            "negative_residual_spans": negative,
        }
    for row in worst_events:
        row["thread_cpu"] = cpu_event_spans(events, row["event"])
    return {
        "validation": "PASS",
        "clock": "DTrace vtimestamp_ns",
        "instrumentation_disclaimer": _DISCLAIMER,
        "summary_scope": (
            "Phase summaries cover measured events; whole-thread advances include warmup and consumer EOF."
        ),
        "whole_thread_cpu_advance_ns": advances,
        "negative_residual_spans": sum(row["negative_residual_spans"] for row in summaries.values()),
        "phase_summaries": summaries,
    }
