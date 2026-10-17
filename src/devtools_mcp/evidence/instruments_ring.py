"""Conditional single-finish-anchor ring arrival / scheduler diagnostic.

Caller-associated exports do not authenticate one capture. No clock join occurs
without explicit constant-offset extrapolation; drift remains unmeasured.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .artifacts import FileBinding, fingerprint_file, verify_binding
from .benchmark import decode_json_records
from .instruments_ring_xml import marker, states
from .instruments_threads import _metadata
from .instruments_xml import U64, read_document, require
from .ring_arrival import _benchmark, _csv, _integer, _read, _stats


def _native(path: str | Path, cap: int) -> tuple[dict, dict]:
    binding = fingerprint_file(Path(path), max_bytes=min(cap, 1 << 20))
    with Path(binding.path).open("rb") as stream:
        data = stream.read(binding.size_bytes + 1)
    require(
        len(data) == binding.size_bytes and hashlib.sha256(data).hexdigest() == binding.sha256, "native report changed"
    )
    verify_binding(binding, max_bytes=min(cap, 1 << 20))
    try:
        text = data.decode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise ValueError("native report requires UTF-8") from exc
    records = decode_json_records(text, max_bytes=min(cap, 1 << 20), max_records=1)
    require(len(records) == 1, "exactly one native JSON report required")
    return records[0], asdict(binding)


def _count_nodes(value: Any, cap: int) -> int:
    pending = [value]
    count = 0
    while pending:
        current = pending.pop()
        count += 1
        require(count <= cap, "combined node cap exceeded")
        if isinstance(current, dict):
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)
    return count


def _window(intervals: list[dict], left: int, right: int, details: int) -> dict:
    require(left <= right, "reversed mapped window")
    covered = 0
    cursor = left
    gaps = []
    overlaps = []
    groups: dict[tuple, int] = {}
    unknown = 0
    for interval in intervals:
        lo, hi = max(left, interval["start_ns"]), min(right, interval["end_ns"])
        if lo >= hi:
            continue
        if lo > cursor:
            gaps.append({"start_ns": cursor, "end_ns": lo, "duration_ns": lo - cursor})
        duration = hi - lo
        covered += duration
        cursor = hi
        state = interval["state"]
        key = (state["code"], state["label"])
        groups[key] = groups.get(key, 0) + duration
        if state["code"] == "Unknown":
            unknown += duration
        overlaps.append(
            {**interval, "intersection_start_ns": lo, "intersection_end_ns": hi, "intersection_ns": duration}
        )
    if cursor < right:
        gaps.append({"start_ns": cursor, "end_ns": right, "duration_ns": right - cursor})
    grouped = sorted(groups.items(), key=lambda item: (-item[1], str(item[0])))
    return {
        "start_ns": left,
        "end_ns": right,
        "duration_ns": right - left,
        "coverage": "COMPLETE" if covered == right - left else "PARTIAL",
        "covered_ns": covered,
        "uncovered_ns": right - left - covered,
        "observed_unknown_state_ns": unknown,
        "gap_count": len(gaps),
        "gaps": gaps[:details],
        "detail_count": len(overlaps),
        "omitted_detail_count": max(0, len(overlaps) - details),
        "omitted_detail_duration_ns": sum(row["intersection_ns"] for row in overlaps[details:]),
        "details": overlaps[:details],
        "state_count": len(groups),
        "omitted_state_count": max(0, len(groups) - details),
        "omitted_state_duration_ns": sum(duration for _, duration in grouped[details:]),
        "state_durations": [
            {"code": key[0], "label": key[1], "duration_ns": duration} for key, duration in grouped[:details]
        ],
    }


def _mapped(intervals: list[dict], left: int, right: int, lo: int, hi: int, details: int) -> dict:
    require(left <= right, "native window ordering")
    possible = (left + lo, right + hi)
    guaranteed = (left + hi, right + lo)
    return {
        "native_start_ns": left,
        "native_end_ns": right,
        "possible_envelope": _window(intervals, *possible, details),
        "guaranteed_core": _window(intervals, *guaranteed, details) if guaranteed[0] < guaranteed[1] else None,
        "guaranteed_core_empty": guaranteed[0] >= guaranteed[1],
    }


def analyze_instruments_ring_arrival(
    native_report_path: str | Path,
    csv_path: str | Path,
    signpost_path: str | Path,
    thread_state_path: str | Path,
    thread_info_path: str | Path,
    *,
    pid: int,
    assume_constant_offset: bool = False,
    max_bytes: int = 128 << 20,
    max_rows: int = 500000,
    max_nodes: int = 3000000,
    max_threads: int = 64,
    top_n: int = 5,
    state_details: int = 20,
) -> dict[str, Any]:
    """Validate all evidence; join only with explicit unmeasured-drift assumption.

    Input bytes/rows/nodes are combined budgets, not aggregate memory budgets.
    XML depth is 64; native JSON is additionally limited to 1MiB. CSV validation
    checks native metrics but has no independent DTrace boundary-stamp proof.
    """
    if type(pid) is not int or not 0 < pid <= U64:
        raise ValueError("pid requires positive builtin uint64")
    if type(assume_constant_offset) is not bool:
        raise ValueError("assume_constant_offset requires builtin bool")
    for name, value in (
        ("max_bytes", max_bytes),
        ("max_rows", max_rows),
        ("max_nodes", max_nodes),
        ("max_threads", max_threads),
        ("top_n", top_n),
        ("state_details", state_details),
    ):
        if type(value) is not int or not 0 < value <= 1 << 40:
            raise ValueError(f"{name} requires positive bounded builtin int")
    report, native_binding = _native(native_report_path, max_bytes)
    warm, total, arrival = _benchmark(report, max_rows)
    require(_integer(report.get("producer_error"), "producer_error") == 0, "producer error must be zero")
    anchor = report.get("clock_anchor")
    require(type(anchor) is dict, "native anchor required")
    assert isinstance(anchor, dict)
    require(anchor.get("requested") is True and anchor.get("status") == "emitted", "emitted native anchor required")
    require(
        anchor.get("name") == "RingClockAnchor"
        and anchor.get("subsystem") == "com.ugbot.chukonu.benchmark"
        and type(anchor.get("signpost_id")) is int
        and anchor["signpost_id"] == 1,
        "native marker identity mismatch",
    )
    before = _integer(anchor.get("native_before_ns"), "anchor before")
    after = _integer(anchor.get("native_after_ns"), "anchor after")
    require(before <= after, "reversed native anchor bracket")
    remaining_bytes = max_bytes - native_binding["size_bytes"]
    require(remaining_bytes > 0, "combined byte cap exceeded")
    lines, csv_binding = _read(csv_path, remaining_bytes, max_rows - 1)
    samples = _csv(lines, warm, total, arrival, None)
    require(all(samples[index - 1][3] <= samples[index][3] for index in range(1, len(samples))), "CSV terminal order")
    require(samples[-1][3] <= before, "finish anchor precedes measured terminal")
    remaining_bytes -= csv_binding["size_bytes"]
    remaining_rows = max_rows - len(lines) - 1
    remaining_nodes = max_nodes - _count_nodes(report, max_nodes) - len(lines) * 4
    require(remaining_rows >= 0 and remaining_nodes >= 0, "combined row/node cap exceeded")
    documents = []
    bindings = [native_binding, csv_binding]
    for path in (signpost_path, thread_state_path, thread_info_path):
        require(remaining_bytes > 0, "combined byte cap exceeded")
        doc, binding = read_document(path, remaining_bytes, remaining_nodes, remaining_rows)
        remaining_bytes -= binding["size_bytes"]
        remaining_nodes -= doc.nodes
        remaining_rows -= doc.rows
        documents.append(doc)
        bindings.append(binding)
    owners: dict[int, tuple[int, str]] = {}
    process_labels: dict[int, str] = {}
    for doc in documents:
        doc.process_labels = process_labels
    threads = _metadata(documents[2], pid, max_threads, owners)
    observed = marker(documents[0], pid, before, threads, owners)
    intervals = states(documents[1], pid, threads, owners)
    lower, upper = observed["trace_time_ns"] - after, observed["trace_time_ns"] - before
    joined = []
    if assume_constant_offset:
        # Sort indices, not sample rows: prior-terminal relation remains original order.
        worst = sorted(
            range(len(samples)), key=lambda index: (-(samples[index][3] - samples[index][2]), samples[index][0])
        )[:top_n]
        main_intervals = intervals.get(observed["tid"], [])
        for index in worst:
            event, prefix, due, terminal = samples[index]
            previous = samples[index - 1][3] if index > 0 else None
            joined.append(
                {
                    "event": event,
                    "prior_rich_entries": prefix,
                    "due_ns": due,
                    "terminal_entry_ns": terminal,
                    "latency_ns": terminal - due,
                    "due_to_terminal": _mapped(main_intervals, due, terminal, lower, upper, state_details),
                    "prior_terminal_to_terminal": _mapped(
                        main_intervals, previous, terminal, lower, upper, state_details
                    )
                    if previous is not None
                    else None,
                    "prior_terminal_unavailable": previous is None,
                }
            )
    for binding in bindings:
        verify_binding(FileBinding(**binding), max_bytes=max_bytes)
    return {
        "analysis_validation": "PASS",
        "performance_validation": arrival["validation"],
        "clock_join_validation": "CONDITIONAL" if assume_constant_offset else "UNVERIFIED",
        "assume_constant_offset": assume_constant_offset,
        "clock_drift": "UNMEASURED",
        "anchor": {
            **observed,
            "native_before_ns": before,
            "native_after_ns": after,
            "offset_lower_ns": lower,
            "offset_upper_ns": upper,
            "offset_width_ns": upper - lower,
        },
        "native_metrics": {
            "events": total,
            "warm_events": warm,
            "measured_events": len(samples),
            "latency": _stats([row[3] - row[2] for row in samples]),
        },
        "csv_validation": "Native metric/schedule consistency only; no DTrace boundary-stamp proof.",
        "marker_thread": threads[observed["tid"]],
        "thread_role": (
            "Collected marker main thread / native fixture caller; no producer role or source stacks inferred."
        ),
        "provenance": (
            "Caller-associated files; shared PID/TID and marker payload do not authenticate a capture UUID."
        ),
        "limitations": (
            "Single-finish-anchor constant-offset extrapolation only when explicitly enabled; drift UNMEASURED. "
            "Possible/guaranteed windows are conditional bounds, not exact CPU time or causal off-CPU/latency proof. "
            "Overlapping windows must not be summed. Analysis PASS is not performance qualification."
        ),
        "input_bindings": dict(
            zip(("native", "arrival_csv", "signpost", "thread_state", "thread_info"), bindings, strict=True)
        ),
        "worst_events": joined,
    }
