"""Private exact exported signpost/thread-state schemas and semantic validation."""

from __future__ import annotations

from typing import Any
from xml.etree.ElementTree import Element

from .instruments_threads import _child, _identity, _number
from .instruments_xml import U64, Document, require

SIGNPOST = [
    ("time", "event-time"),
    ("thread", "thread"),
    ("process", "process"),
    ("event-type", "event-type"),
    ("scope", "string"),
    ("identifier", "os-signpost-identifier"),
    ("name", "signpost-name"),
    ("format-string", "format-string"),
    ("backtrace", "text-backtrace"),
    ("subsystem", "subsystem"),
    ("category", "category"),
    ("message", "os-log-metadata"),
    ("emit-location", "return-location"),
]
STATE = [
    ("start", "start-time"),
    ("thread", "thread"),
    ("state", "thread-state"),
    ("duration", "duration"),
    ("process", "process"),
    ("core", "core"),
    ("cputime", "duration-on-core"),
    ("waittime", "duration-waiting"),
    ("priority", "sched-priority"),
    ("note", "narrative"),
    ("summary", "narrative"),
    ("made-runnable-by-thread", "thread"),
    ("preempted-by-thread", "thread"),
    ("yielded-to-thread", "thread"),
    ("rebalanced-from-cpu", "core"),
    ("thermal-throttled", "boolean"),
]


def table_rows(doc: Document, schema_name: str, columns: list, optional: set[int]) -> list[tuple[str, list[Element]]]:
    require(len(doc.root) > 0 and all(n.tag == "node" for n in doc.root), "expected exported table nodes")
    identities: set[str] = set()
    declared = False
    result = []
    for table in doc.root:
        identity = table.attrib.get("xpath", "")
        require(bool(identity) and identity not in identities, "duplicate/missing table identity")
        identities.add(identity)
        for item in table:
            if item.tag == "schema":
                require(item is table[0] and item.attrib.get("name") == schema_name, "unexpected schema")
                require(len(item) == len(columns), "schema column count mismatch")
                for col, (mnemonic, kind) in zip(item, columns, strict=True):
                    require(
                        col.tag == "col" and [x.tag for x in col] == ["mnemonic", "name", "engineering-type"],
                        "schema shape",
                    )
                    require(col[0].text == mnemonic and col[2].text == kind, "schema column mismatch")
                declared = True
                continue
            require(declared and item.tag == "row" and len(item) == len(columns), "row/schema extent mismatch")
            cells = [doc.resolve(x) for x in item]
            for index, (cell, (_, kind)) in enumerate(zip(cells, columns, strict=True)):
                require(cell.tag == kind or (index in optional and cell.tag == "sentinel"), "row column type mismatch")
                if cell.tag == "sentinel":
                    require(not len(cell) and not (cell.text or "").strip(), "invalid sentinel")
            result.append((identity, cells))
    require(declared, "missing declared schema")
    return result


def identity(
    doc: Document, process: Element, thread: Element, owners: dict[int, tuple[int, str]]
) -> tuple[int, int, str]:
    pid, tid, label = _identity(doc, process, thread)
    require(tid not in owners or owners[tid] == (pid, label), "thread ownership changed across exports")
    owners[tid] = (pid, label)
    return pid, tid, label


def marker(doc: Document, pid: int, before: int, threads: dict, owners: dict) -> dict[str, Any]:
    matches = []
    tables: set[str] = set()
    for table, row in table_rows(doc, "os-signpost", SIGNPOST, {8, 12}):
        owner, tid, _ = identity(doc, row[2], row[1], owners)
        time = _number(doc, row[0])
        identifier = _number(doc, row[5])
        for index in (3, 4, 6, 7, 9, 10):
            require(not len(row[index]), "invalid marker scalar")
        if owner != pid:
            continue
        require(tid in threads, "marker target thread unknown")
        # Target marker-like rows cannot evade validation by changing one identity field.
        candidate = row[6].text == "RingClockAnchor" or row[9].text == "com.ugbot.chukonu.benchmark"
        if not candidate:
            continue
        require(
            row[6].text == "RingClockAnchor" and row[9].text == "com.ugbot.chukonu.benchmark",
            "marker name/subsystem mismatch",
        )
        require(
            identifier == 1
            and row[3].text == "Event"
            and row[4].text == "Process"
            and row[10].text == "PointsOfInterest",
            "marker kind/id/category mismatch",
        )
        require(row[7].text == "native_before_ns=%{public}llu", "marker format mismatch")
        message = row[11]
        require([x.tag for x in message] == ["narrative-text", "uint64"], "marker payload shape")
        narrative = doc.resolve(message[0])
        payload = _number(doc, message[1])
        require(
            not len(narrative) and narrative.text == "native_before_ns=" and payload == before,
            "marker payload mismatch",
        )
        require(threads[tid]["main_thread"], "marker must identify main thread")
        require(table not in tables, "duplicate marker within table")
        tables.add(table)
        matches.append(
            (
                time,
                tid,
                payload,
                identifier,
                row[3].text,
                row[4].text,
                row[6].text,
                row[7].text,
                row[9].text,
                row[10].text,
            )
        )
    require(bool(matches) and all(value == matches[0] for value in matches), "missing/contradictory marker")
    return {
        "trace_time_ns": matches[0][0],
        "tid": matches[0][1],
        "projection_count": len(matches),
        "name": "RingClockAnchor",
        "subsystem": "com.ugbot.chukonu.benchmark",
        "signpost_id": 1,
    }


def states(doc: Document, pid: int, threads: dict, owners: dict) -> dict[int, list[dict]]:
    by_thread: dict[int, list[dict]] = {}
    for _, row in table_rows(doc, "thread-state", STATE, {5, 6, 7, 9, 10, 11, 12, 13, 14, 15}):
        owner, tid, _ = identity(doc, row[4], row[1], owners)
        start, duration = _number(doc, row[0]), _number(doc, row[3])
        require(start <= U64 - duration, "state interval overflow")
        require(not len(row[2]) and bool(row[2].text), "invalid raw thread state")
        priority = {"code": _number(doc, row[8]), "label": row[8].attrib.get("fmt")}
        core = None
        for index in (5, 6, 7, 14, 15):
            if row[index].tag != "sentinel":
                number = _number(doc, row[index])
                if index == 15:
                    require(number in (0, 1), "thermal boolean must be 0 or 1")
                if index == 5:
                    core = {"code": number, "label": row[index].attrib.get("fmt")}
        for index in (11, 12, 13):
            if row[index].tag == "thread":
                causing = doc.resolve(row[index])
                process = _child(doc, causing, "process")
                identity(doc, process, causing, owners)
        if owner == pid:
            require(tid in threads, "state target thread unknown")
        by_thread.setdefault(tid, []).append(
            {
                "start_ns": start,
                "end_ns": start + duration,
                "state": {"code": row[2].text, "label": row[2].attrib.get("fmt")},
                "core": core,
                "priority": priority,
            }
        )
    for intervals in by_thread.values():
        intervals.sort(key=lambda value: (value["start_ns"], value["end_ns"]))
        end = 0
        for interval in intervals:
            if interval["start_ns"] == interval["end_ns"]:
                continue
            require(interval["start_ns"] >= end, "overlapping thread-state intervals")
            end = interval["end_ns"]
    return {tid: values for tid, values in by_thread.items() if tid in threads}
