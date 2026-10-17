"""Read-only thread-policy coverage from caller-associated xctrace exports.

PID/TID equality does not authenticate a shared execution. No native clock
mapping, latency qualification, scheduling change, or causal attribution occurs.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any
from xml.etree.ElementTree import Element

from .instruments_xml import U64, Document, read_document, require, uint

SCHEMAS = {
    "thread-info": [
        ("time", "event-time"),
        ("pid", "pid"),
        ("tid", "tid"),
        ("process", "process"),
        ("thread", "thread"),
        ("name", "thread-name"),
        ("main-thread", "boolean"),
    ],
    "ThreadQoSTable": [
        ("start", "start-time"),
        ("duration", "duration"),
        ("process", "process"),
        ("thread", "thread"),
        ("requested-qo-s", "quality-of-service-class"),
        ("effective-qo-s", "quality-of-service-class"),
        ("mismatch-qo-s", "state"),
    ],
    "ThreadPriority": [
        ("start", "start-time"),
        ("duration", "duration"),
        ("process", "process"),
        ("thread", "thread"),
        ("scheduled-priority", "sched-priority"),
        ("base-priority", "sched-priority"),
    ],
}


def _child(doc: Document, node: Element, tag: str) -> Element:
    children = [doc.resolve(child) for child in node if child.tag == tag]
    require(len(children) == 1, f"expected one {tag}")
    return children[0]


def _number(doc: Document, node: Element) -> int:
    node = doc.resolve(node)
    require(not len(node), "numeric node has children")
    return uint(node.text)


def _identity(doc: Document, process: Element, thread: Element) -> tuple[int, int, str]:
    process, thread = doc.resolve(process), doc.resolve(thread)
    require([child.tag for child in process] == ["pid", "device-session"], "invalid process shape")
    require([child.tag for child in thread] == ["tid", "process"], "invalid thread shape")
    pid = _number(doc, _child(doc, process, "pid"))
    nested = _child(doc, thread, "process")
    require(_number(doc, _child(doc, nested, "pid")) == pid, "row/thread PID mismatch")
    require(nested.attrib.get("fmt") == process.attrib.get("fmt"), "process identity changed")
    label = process.attrib.get("fmt", "")
    prior = getattr(doc, "process_labels", {})
    require(pid not in prior or prior[pid] == label, "process identity changed")
    prior[pid] = label
    doc.process_labels = prior
    return pid, _number(doc, _child(doc, thread, "tid")), label


def _rows(doc: Document, name: str) -> list[list[Element]]:
    require([node.tag for node in doc.root] == ["node"], "expected one exported table")
    table = doc.root[0]
    require(len(table) >= 1 and table[0].tag == "schema", "missing schema")
    schema = table[0]
    require(schema.attrib.get("name") == name, "unexpected schema name")
    expected = SCHEMAS[name]
    require(len(schema) == len(expected), "schema column count mismatch")
    for col, (mnemonic, kind) in zip(schema, expected, strict=True):
        require(
            col.tag == "col" and [x.tag for x in col] == ["mnemonic", "name", "engineering-type"],
            "invalid schema column",
        )
        require(col[0].text == mnemonic and col[2].text == kind, "schema column mismatch")
    result = []
    for row in list(table)[1:]:
        require(row.tag == "row" and len(row) == len(expected), "row column count mismatch")
        cells = [doc.resolve(cell) for cell in row]
        for cell, (_, kind) in zip(cells, expected, strict=True):
            require(cell.tag == kind or (kind == "state" and cell.tag == "sentinel"), "row column type mismatch")
        result.append(cells)
    return result


def _metadata(doc: Document, pid: int, max_threads: int, identities: dict) -> dict[int, dict[str, Any]]:
    threads: dict[int, dict[str, Any]] = {}
    for row in _rows(doc, "thread-info"):
        _number(doc, row[0])
        owner, tid, process = _identity(doc, row[3], row[4])
        require(_number(doc, row[1]) == owner and _number(doc, row[2]) == tid, "thread-info identity mismatch")
        main = _number(doc, row[6])
        require(main in (0, 1), "main-thread must be 0 or 1")
        identity = (owner, process)
        require(tid not in identities or identities[tid] == identity, "thread ownership changed")
        identities[tid] = identity
        name = row[5]
        require(len(name) == 2, "invalid thread-name shape")
        if name[0].tag == "sentinel":
            require(name[1].tag == "sentinel", "invalid unnamed thread")
            label = None
        else:
            require([x.tag for x in name] == ["string", "thread"], "invalid thread-name shape")
            require(_identity(doc, row[3], name[1])[:2] == (owner, tid), "thread-name identity mismatch")
            label = doc.resolve(name[0]).text or ""
        if owner != pid:
            continue
        value = {"tid": tid, "main_thread": bool(main), "name": label, "process_label": process}
        require(tid not in threads or threads[tid] == value, "thread metadata changed")
        threads[tid] = value
        require(len(threads) <= max_threads, "target thread cap exceeded")
    require(bool(threads), "target PID absent from thread-info")
    return threads


def _intervals(
    doc: Document, name: str, pid: int, threads: dict[int, dict[str, Any]], identities: dict
) -> dict[int, list]:
    intervals: dict[int, list] = defaultdict(list)
    for row in _rows(doc, name):
        start, duration = _number(doc, row[0]), _number(doc, row[1])
        require(start <= U64 - duration, "interval end overflow")
        owner, tid, process = _identity(doc, row[2], row[3])
        require(tid not in identities or identities[tid] == (owner, process), "thread ownership changed")
        identities[tid] = (owner, process)
        pair_values = tuple((_number(doc, cell), cell.attrib.get("fmt")) for cell in row[4:6])
        state = None
        if name == "ThreadQoSTable":
            cell = row[6]
            require(not len(cell), "invalid mismatch state children")
            if cell.tag == "sentinel":
                require(not (cell.text or "").strip(), "invalid mismatch sentinel")
            else:
                require(bool((cell.text or "").strip()), "empty mismatch state")
            state = (cell.tag, cell.text or "", cell.attrib.get("fmt"))
        pair = (*pair_values, state)
        intervals[tid].append((start, start + duration, pair))
        if owner != pid:
            continue
        require(tid in threads, "policy row references unknown target thread")
        require(process == threads[tid]["process_label"], "process identity changed across exports")
    # Reject overlaps even outside the requested window; zero intervals cover nothing.
    for values in intervals.values():
        previous_end = 0
        for start, end, _ in sorted(values, key=lambda value: value[:2]):
            if start == end:
                continue
            require(start >= previous_end, "overlapping policy intervals")
            previous_end = end
    return intervals


def _summary(intervals: list, start: int, end: int, top_n: int) -> dict[str, Any]:
    covered, cursor = 0, start
    gaps: list[dict[str, int]] = []
    pairs: dict[tuple, int] = defaultdict(int)
    for left, right, pair in sorted(intervals, key=lambda value: value[:2]):
        left, right = max(left, start), min(right, end)
        if left >= right:
            continue
        if left > cursor:
            gaps.append({"start_ns": cursor, "end_ns": left, "duration_ns": left - cursor})
        covered += right - left
        pairs[pair] += right - left
        cursor = right
    if cursor < end:
        gaps.append({"start_ns": cursor, "end_ns": end, "duration_ns": end - cursor})
    ordered_pairs = sorted(pairs.items(), key=lambda item: (-item[1], str(item[0])))
    return {
        "coverage": "COMPLETE" if covered == end - start else "PARTIAL",
        "covered_ns": covered,
        "uncovered_ns": end - start - covered,
        "gap_count": len(gaps),
        "gaps": gaps[:top_n],
        "pair_count": len(pairs),
        "omitted_pairs": max(0, len(pairs) - top_n),
        "omitted_pair_covered_ns": sum(duration for _, duration in ordered_pairs[top_n:]),
        "different_pair_codes_ns": sum(duration for pair, duration in pairs.items() if pair[0][0] != pair[1][0]),
        "exporter_state_present_ns": sum(
            duration for pair, duration in pairs.items() if pair[2] is not None and pair[2][0] == "state"
        ),
        "observed_pairs": [
            {
                "first": {"code": pair[0][0], "label": pair[0][1]},
                "second": {"code": pair[1][0], "label": pair[1][1]},
                "covered_ns": duration,
                "exporter_mismatch_state": (
                    {"tag": pair[2][0], "text": pair[2][1], "label": pair[2][2]} if pair[2] is not None else None
                ),
            }
            for pair, duration in ordered_pairs[:top_n]
        ],
    }


def analyze_instruments_thread_policy(
    thread_info_path: str | Path,
    qos_path: str | Path,
    priority_path: str | Path,
    *,
    pid: int,
    window_start_ns: int,
    window_end_ns: int,
    max_bytes: int = 64 << 20,
    max_rows: int = 100000,
    max_nodes: int = 500000,
    max_threads: int = 64,
    top_n: int = 20,
) -> dict[str, Any]:
    """Validate three caller-associated exports and report half-open window coverage.

    Caps cover combined input bytes/rows/nodes, target threads, and each gap/pair
    report. XML and reference graph depth is limited to 64. Sparse evidence is
    valid PARTIAL coverage; unknown policy is never filled with a default.
    """
    for name, value in (("pid", pid), ("window_start_ns", window_start_ns), ("window_end_ns", window_end_ns)):
        if type(value) is not int or not 0 <= value <= U64:
            raise ValueError(f"{name} requires a builtin uint64")
    for name, value in (
        ("max_bytes", max_bytes),
        ("max_rows", max_rows),
        ("max_nodes", max_nodes),
        ("max_threads", max_threads),
        ("top_n", top_n),
    ):
        if type(value) is not int or not 0 < value <= (1 << 40):
            raise ValueError(f"{name} requires a positive bounded builtin integer")
    if window_start_ns >= window_end_ns:
        raise ValueError("requested window must be nonempty and half-open")
    documents, bindings = [], []
    remaining_bytes, remaining_nodes, remaining_rows = max_bytes, max_nodes, max_rows
    for path in (thread_info_path, qos_path, priority_path):
        require(remaining_bytes > 0, "combined byte cap exceeded")
        doc, binding = read_document(path, remaining_bytes, remaining_nodes, remaining_rows)
        remaining_bytes -= binding["size_bytes"]
        remaining_nodes -= doc.nodes
        remaining_rows -= doc.rows
        documents.append(doc)
        bindings.append(binding)
    identities: dict[int, tuple[int, str]] = {}
    process_labels: dict[int, str] = {}
    for document in documents:
        document.process_labels = process_labels
    threads = _metadata(documents[0], pid, max_threads, identities)
    qos = _intervals(documents[1], "ThreadQoSTable", pid, threads, identities)
    priority = _intervals(documents[2], "ThreadPriority", pid, threads, identities)
    result = []
    for tid, metadata in sorted(threads.items()):
        result.append(
            {
                **metadata,
                "qos": _summary(qos.get(tid, []), window_start_ns, window_end_ns, top_n),
                "priority": _summary(priority.get(tid, []), window_start_ns, window_end_ns, top_n),
            }
        )
    for thread in result:
        thread["qos"]["pair_roles"] = ["requested_qos", "effective_qos"]
        thread["priority"]["pair_roles"] = ["scheduled_priority", "base_priority"]
    return {
        "analysis_validation": "PASS",
        "performance_validation": "UNVERIFIED",
        "coverage": "COMPLETE"
        if all(t[k]["coverage"] == "COMPLETE" for t in result for k in ("qos", "priority"))
        else "PARTIAL",
        "pid": pid,
        "window": {"start_ns": window_start_ns, "end_ns": window_end_ns},
        "input_bindings": dict(zip(("thread_info", "qos", "priority"), bindings, strict=True)),
        "provenance": (
            "Caller-associated exports; PID/TID equality does not authenticate the same "
            "execution. No capture UUID is present."
        ),
        "limitations": (
            "Trace-relative window only; no native clock mapping, default policy extrapolation, "
            "off-CPU/latency causality or performance qualification."
        ),
        "threads": result,
    }
