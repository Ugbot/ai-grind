"""Strict offline fixed-arrival boundary attribution; never a performance certificate.

Only dedicated B records and the native arrival CSV are accepted. Instrumented
wall intervals include tracing/scheduling overhead; no clock cost is subtracted.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .artifacts import ArtifactError, fingerprint_file, verify_binding
from .benchmark import EvidenceParseError
from .ring_cpu import summarize_thread_cpu, validate_thread_cpu

_U64 = (1 << 64) - 1
_PHASES = ("gen_in", "gen_out", "commit_in", "commit_out", "read_in", "read_out", "observe_in", "observe_out", "stamp")
_PRODUCER = frozenset(_PHASES[:4])
_NUMBER = re.compile(r"(?:0|[1-9][0-9]{0,19})\Z")
_HEADER = "event,prior_rich_entries,due_ns,terminal_entry_ns"
_DISCLAIMER = (
    "Instrumented diagnostic wall intervals include probe and scheduling overhead. "
    "Publication-to-read is bounded and includes acquisition/decoding, not exact queue time. "
    "Clock domains are related only by the reported intersected offset bounds. "
    "Analysis PASS does not certify the latency gate or establish CPU-profile causality."
)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceParseError(message)


def _integer(value: Any, label: str, *, positive: bool = False) -> int:
    _require(type(value) is int and (1 if positive else 0) <= value <= _U64, f"invalid {label}")
    return value


def _number(value: str) -> int:
    _require(bool(_NUMBER.fullmatch(value)), "noncanonical boundary/CSV integer")
    return _integer(int(value), "boundary/CSV integer")


def _read(path: str | Path, cap: int, rows: int) -> tuple[list[str], dict]:
    try:
        binding = fingerprint_file(Path(path), max_bytes=cap)
        with open(binding.path, "rb") as stream:
            data = stream.read(binding.size_bytes + 1)
        _require(
            len(data) <= cap and hashlib.sha256(data).hexdigest() == binding.sha256, "evidence changed while reading"
        )
        verify_binding(binding, max_bytes=cap)
    except (OSError, ArtifactError) as exc:
        raise EvidenceParseError(f"cannot read stable evidence: {exc}") from exc
    _require(data.count(b"\n") <= rows + 2, "evidence row cap exceeded")
    # Reject mixed native JSON, banners, whitespace and overlong records rather
    # than recovering fragments from an interleaved stdout stream.
    try:
        lines = data.decode("ascii").split("\n")
    except UnicodeError as exc:
        raise EvidenceParseError("evidence must be ASCII") from exc
    # LF is the only separator. A CR is accepted only immediately before LF.
    for index, line in enumerate(lines):
        if line.endswith("\r") and index < len(lines) - 1:
            lines[index] = line[:-1]
    _require(all(not any(ord(char) < 32 for char in line) for line in lines), "embedded control character in evidence")
    while lines and lines[-1] == "":
        lines.pop()
    _require(all(lines), "embedded blank record")
    _require(len(lines) <= rows and all(len(line) <= 192 for line in lines), "evidence line/row cap exceeded")
    return lines, asdict(binding)


def _benchmark(report: dict, limit: int) -> tuple[int, int, dict]:
    _require(type(report) is dict and report.get("mode") == "ring", "benchmark must be a ring report")
    warm = _integer(report.get("warm_events"), "warm_events")
    measured = _integer(report.get("measured_events"), "measured_events", positive=True)
    total = warm + measured
    _require(total <= limit and total < _U64, "event cap exceeded")
    for key, value in (
        ("error", 0),
        ("eof", 1),
        ("producer_phase", 2),
        ("accepted_batches", total),
        ("output_rows", total),
        ("output_events", total),
    ):
        _require(_integer(report.get(key), key) == value, f"benchmark {key} mismatch")
    source = _integer(report.get("source_entries"), "source_entries", positive=True)
    _require(
        _integer(report.get("accepted_rows"), "accepted_rows") == source and source >= total,
        "source/accepted rows mismatch",
    )
    a = report.get("arrival")
    _require(type(a) is dict, "paced arrival report required")
    assert isinstance(a, dict)  # Proven by runtime admission above.
    for key in ("warm_admitted_before_epoch", "trace_requested", "trace_written"):
        _require(a.get(key) is True, f"arrival {key} required")
    for key in ("samples", "trace_rows"):
        _require(_integer(a.get(key), key) == measured, f"arrival {key} mismatch")
    for key in ("epoch_steady_ns", "target_rich_entries_per_second", "gate_ns"):
        _integer(a.get(key), key, positive=True)
    _require(a["target_rich_entries_per_second"] <= 1_000_000_000, "arrival rate outside native range")
    for key in ("p50_ns", "p99_ns", "max_ns", "warm_source_entries", "measured_source_entries"):
        _integer(a.get(key), key)
    _require(a["warm_source_entries"] + a["measured_source_entries"] == source, "warm/measured source prefix mismatch")
    _require(
        a["warm_source_entries"] >= warm and a["measured_source_entries"] >= measured,
        "source prefix smaller than events",
    )
    expected = "PASS" if a["p99_ns"] <= a["gate_ns"] else "FAIL"
    _require(
        a.get("validation") == expected and report.get("validation") == expected,
        "benchmark/arrival validation contradicts latency gate",
    )
    return warm, total, a


def _boundaries(lines: list[str], total: int) -> tuple[list[dict], int, int, int]:
    # Missing/tiny files must refuse before allocating proportional event state.
    _require(len(lines) == total * 9 + 2, "boundary count mismatch")
    events: list[dict] = [{} for _ in range(total + 2)]
    pid = producer = consumer = None
    count = 0
    width = len(lines[0].split(","))
    _require(width in (7, 8), "expected seven or eight dedicated B columns")
    for line in lines:
        if line == "":
            continue
        fields = line.split(",")
        _require(
            len(fields) == width and fields[0] == "B" and fields[4] in _PHASES, "expected exact dedicated B record"
        )
        process, thread, event, phase, stamp, arg = (
            _number(fields[1]),
            _number(fields[2]),
            _number(fields[3]),
            fields[4],
            _number(fields[5]),
            _number(fields[6]),
        )
        _require(process > 0 and thread > 0 and 1 <= event <= total + 1, "invalid process/thread/event")
        _require(event <= total or phase in ("observe_in", "observe_out"), "unexpected EOF phase")
        _require(phase not in events[event], "duplicate event phase")
        pid = process if pid is None else pid
        _require(pid == process, "multiple target PIDs")
        if phase in _PRODUCER:
            producer = thread if producer is None else producer
            _require(producer == thread, "multiple producer threads")
        else:
            consumer = thread if consumer is None else consumer
            _require(consumer == thread, "multiple consumer threads")
        expected = 1 if phase == "read_out" else 0
        _require(phase == "stamp" or arg == expected, f"unexpected {phase} return/argument")
        events[event][phase] = (stamp, arg) if width == 7 else (stamp, arg, _number(fields[7]))
        count += 1
        _require(count <= total * 9 + 2, "boundary row cap exceeded")
    _require(
        pid is not None and producer is not None and consumer is not None and producer != consumer,
        "distinct producer/consumer threads required",
    )
    _require(count == total * 9 + 2, "boundary count mismatch")
    for event in range(1, total + 1):
        e = events[event]
        _require(set(e) == set(_PHASES), "missing event phase")
        t = {key: val[0] for key, val in e.items()}
        _require(t["gen_in"] <= t["gen_out"] <= t["commit_in"] <= t["commit_out"], "producer ordering")
        _require(
            t["commit_in"] <= t["read_in"] <= t["read_out"] <= t["observe_in"] <= t["stamp"] <= t["observe_out"],
            "consumer/publication ordering",
        )
        if event > 1:
            previous = events[event - 1]
            _require(
                previous["commit_out"][0] <= t["gen_in"] and previous["observe_out"][0] <= t["read_in"],
                "cross-event thread ordering",
            )
    eof = events[total + 1]
    _require(
        set(eof) == {"observe_in", "observe_out"}
        and events[total]["observe_out"][0] <= eof["observe_in"][0] <= eof["observe_out"][0],
        "invalid EOF observation",
    )
    assert pid is not None and producer is not None and consumer is not None
    return events, pid, producer, consumer


def _csv(
    lines: list[str], warm: int, total: int, a: dict, events: list[dict] | None
) -> list[tuple[int, int, int, int]]:
    _require(len(lines) == total - warm + 1 and lines[0] == _HEADER, "CSV header/count mismatch")
    result = []
    previous = -1
    rate, epoch = a["target_rich_entries_per_second"], a["epoch_steady_ns"]
    for event, line in enumerate(lines[1:], warm + 1):
        fields = line.split(",")
        _require(len(fields) == 4, "CSV must have four exact fields")
        identity, prefix, due, stamp = map(_number, fields)
        _require(
            identity == event and prefix > previous and (event != warm + 1 or prefix == 0), "CSV event/prefix ordering"
        )
        _require(prefix < a["measured_source_entries"], "CSV prefix outside source extent")
        quotient, remainder = divmod(prefix, rate)
        expected = epoch + quotient * 1_000_000_000 + remainder * 1_000_000_000 // rate
        _require(expected <= _U64 and due == expected and stamp >= due, "CSV due/causality mismatch")
        if events is not None:
            _require(stamp == events[event]["stamp"][1], "CSV/native stamp mismatch")
        result.append((event, prefix, due, stamp))
        previous = prefix
    latency = [row[3] - row[2] for row in result]
    _require(
        _stats(latency) == {"p50_ns": a["p50_ns"], "p99_ns": a["p99_ns"], "max_ns": a["max_ns"]},
        "independent latency percentiles mismatch",
    )
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


def analyze_ring_arrival_boundaries(
    boundary_path: str | Path,
    arrival_csv_path: str | Path,
    benchmark_json_dict: dict,
    *,
    max_events: int = 1_000_000,
    max_bytes: int = 256 << 20,
    top_n: int = 20,
    preceding_events: int = 0,
) -> dict:
    """Validate complete dedicated evidence and return bounded clock-safe attribution.

    ``max_events`` caps warm plus measured events; ``max_bytes`` caps both files
    combined. ``top_n`` caps returned event detail. All limits are positive;
    refusal raises EvidenceParseError (invalid caller limits raise ValueError).
    ``preceding_events`` independently caps measured events immediately before
    the single worst event; zero returns no predecessor rows. Warm events are
    never included. No input is written, repaired, truncated, or interpreted as
    execution authority.
    """
    if any(type(v) is not int or v <= 0 for v in (max_events, max_bytes, top_n)) or max_bytes > 1 << 40:
        raise ValueError("positive bounded event/byte/report caps required")
    if type(preceding_events) is not int or not 0 <= preceding_events <= max_events:
        raise ValueError("preceding_events requires a builtin int in [0, max_events]")
    warm, total, a = _benchmark(benchmark_json_dict, max_events)
    lines, boundary_binding = _read(boundary_path, max_bytes, total * 9 + 2)
    remaining = max_bytes - boundary_binding["size_bytes"]
    _require(remaining > 0, "combined byte cap exhausted")
    csv_lines, csv_binding = _read(arrival_csv_path, remaining, total - warm + 1)
    events, pid, producer, consumer = _boundaries(lines, total)
    cpu_advances = validate_thread_cpu(events, total)
    samples = _csv(csv_lines, warm, total, a, events)
    lower, upper = -_U64, _U64
    for event in range(1, total + 1):
        e = events[event]
        steady = e["stamp"][1]
        lower = max(lower, steady - e["stamp"][0])
        upper = min(upper, steady - e["observe_in"][0])
    _require(lower <= upper, "empty clock offset intersection")
    phases: dict[str, list[int]] = {
        name: []
        for name in (
            "generation",
            "post_generation_producer",
            "commit_bracket",
            "publication_to_read_lower",
            "publication_to_read_upper",
            "reader_validation",
            "downstream",
            "observer",
            "source_lateness_lower",
            "source_lateness_upper",
            "prior_producer_gap",
            "prior_consumer_gap",
        )
    }
    details = []
    definitely_late = definitely_within = 0
    for event, prefix, due, stamp in samples:
        e = {key: val[0] for key, val in events[event].items()}
        values = {
            "generation": e["gen_out"] - e["gen_in"],
            "post_generation_producer": e["commit_in"] - e["gen_out"],
            "commit_bracket": e["commit_out"] - e["commit_in"],
            "publication_to_read_lower": max(0, e["read_in"] - e["commit_out"]),
            "publication_to_read_upper": e["read_in"] - e["commit_in"],
            "reader_validation": e["read_out"] - e["read_in"],
            "downstream": e["observe_in"] - e["read_out"],
            "observer": e["observe_out"] - e["observe_in"],
            "source_lateness_lower": e["gen_in"] + lower - due,
            "source_lateness_upper": e["gen_in"] + upper - due,
        }
        if event > 1:
            values["prior_producer_gap"] = e["gen_in"] - events[event - 1]["commit_out"][0]
            values["prior_consumer_gap"] = e["read_in"] - events[event - 1]["observe_out"][0]
        for key, value in values.items():
            phases[key].append(value)
        if stamp - due > a["gate_ns"]:
            definitely_late += values["source_lateness_lower"] > a["gate_ns"]
            definitely_within += values["source_lateness_upper"] <= a["gate_ns"]
        details.append(
            {
                "event": event,
                "prior_rich_entries": prefix,
                "due_ns": due,
                "terminal_entry_ns": stamp,
                "latency_ns": stamp - due,
                **values,
            }
        )
    late = sum(row["latency_ns"] > a["gate_ns"] for row in details)
    worst_index = max(range(len(details)), key=lambda index: details[index]["latency_ns"])
    predecessors = details[max(0, worst_index - preceding_events) : worst_index]
    details.sort(key=lambda row: (-row["latency_ns"], row["event"]))
    worst = details[:top_n]
    thread_cpu = summarize_thread_cpu(events, warm, total, cpu_advances, worst + predecessors)
    return {
        "analysis_validation": "PASS",
        "performance_validation": a["validation"],
        "instrumentation_disclaimer": _DISCLAIMER,
        "events": total,
        "warm_events": warm,
        "measured_events": total - warm,
        "pid": pid,
        "producer_tid": producer,
        "consumer_tid": consumer,
        "input_bindings": {"boundaries": boundary_binding, "arrival_csv": csv_binding},
        "clock_offset_bounds_ns": {"lower": lower, "upper": upper},
        "latency": _stats([row[3] - row[2] for row in samples]),
        "phase_summaries": {key: {"count": len(values), **_stats(values)} for key, values in phases.items() if values},
        "late_events": {
            "count": late,
            "source_definitely_over_gate": definitely_late,
            "source_definitely_within_gate": definitely_within,
            "source_ambiguous": late - definitely_late - definitely_within,
        },
        "worst_events": worst,
        "worst_event_predecessors": {
            "requested": preceding_events,
            "available": len(predecessors),
            "start_event": predecessors[0]["event"] if predecessors else None,
            "end_event": predecessors[-1]["event"] if predecessors else None,
            "events": predecessors,
        },
        "thread_cpu": thread_cpu,
    }
