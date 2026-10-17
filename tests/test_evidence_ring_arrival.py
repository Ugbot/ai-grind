"""Literal unequal-domain ring evidence; no native generator dependency."""

import json

import pytest

from devtools_mcp.evidence import EvidenceParseError
from devtools_mcp.evidence.ring_arrival import analyze_ring_arrival_boundaries


def fixture(tmp_path):
    # Steady clock is approximately DTrace+1000. Commit return may trail read.
    rows = []
    for event, base, steady in ((1, 0, 1105), (2, 100, 1205), (3, 200, 1310)):
        for phase, offset, arg in (
            ("gen_in", 60, 0),
            ("gen_out", 70, 0),
            ("commit_in", 80, 0),
            ("commit_out", 105, 0),
            ("read_in", 90, 0),
            ("read_out", 95, 1),
            ("observe_in", 100, 0),
            ("stamp", 110, steady),
            ("observe_out", 120, 0),
        ):
            tid = 10 if phase.startswith(("gen_", "commit_")) else 20
            rows.append(f"B,7,{tid},{event},{phase},{base + offset},{arg}")
    rows += ["B,7,20,4,observe_in,330,0", "B,7,20,4,observe_out,340,0"]
    boundaries = tmp_path / "boundaries.csv"
    boundaries.write_text("\n".join(rows) + "\n", encoding="ascii")
    csv = tmp_path / "arrival.csv"
    csv.write_text(
        "event,prior_rich_entries,due_ns,terminal_entry_ns\n2,0,1190,1205\n3,1,1290,1310\n", encoding="ascii"
    )
    report = dict(
        mode="ring",
        validation="FAIL",
        error=0,
        eof=1,
        producer_phase=2,
        warm_events=1,
        measured_events=2,
        accepted_batches=3,
        output_rows=3,
        output_events=3,
        source_entries=5,
        accepted_rows=5,
        arrival=dict(
            validation="FAIL",
            warm_admitted_before_epoch=True,
            trace_requested=True,
            trace_written=True,
            samples=2,
            trace_rows=2,
            epoch_steady_ns=1190,
            target_rich_entries_per_second=10_000_000,
            gate_ns=18,
            p50_ns=15,
            p99_ns=20,
            max_ns=20,
            warm_source_entries=2,
            measured_source_entries=3,
        ),
    )
    return boundaries, csv, report


def analyze(paths, **kwargs):
    return analyze_ring_arrival_boundaries(*paths, max_events=3, top_n=2, **kwargs)


def test_literal_overlap_clock_intersection_and_no_performance_certificate(tmp_path):
    paths = fixture(tmp_path)
    result = analyze(paths)
    assert result["analysis_validation"] == "PASS"
    assert result["performance_validation"] == "FAIL"
    assert result["clock_offset_bounds_ns"] == {"lower": 1000, "upper": 1005}
    assert result["latency"] == {"p50_ns": 15, "p99_ns": 20, "max_ns": 20}
    assert [r["event"] for r in result["worst_events"]] == [3, 2]
    row = result["worst_events"][0]
    assert row["generation"] == 10 and row["post_generation_producer"] == 10
    assert row["commit_bracket"] == 25
    assert row["publication_to_read_lower"] == 0 and row["publication_to_read_upper"] == 10
    assert row["reader_validation"] == 5 and row["downstream"] == 5 and row["observer"] == 20
    assert (row["source_lateness_lower"], row["source_lateness_upper"]) == (-30, -25)
    assert row["prior_producer_gap"] == 55 and row["prior_consumer_gap"] == 70
    assert result["late_events"] == dict(
        count=1, source_definitely_over_gate=0, source_definitely_within_gate=1, source_ambiguous=0
    )
    assert len(result["input_bindings"]["boundaries"]["sha256"]) == 64
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "duplicate",
        "thread",
        "pid",
        "phase",
        "event",
        "return",
        "negative",
        "overflow",
        "mixed_json",
        "eof_extra",
        "ordering",
        "offset",
    ],
)
def test_malformed_boundaries_refuse(tmp_path, fault):
    paths = fixture(tmp_path)
    text = paths[0].read_text()
    changes = {
        "missing": ("B,7,10,2,gen_out,170,0\n", ""),
        "duplicate": ("B,7,10,2,gen_out,170,0", "B,7,10,2,gen_out,170,0\nB,7,10,2,gen_out,170,0"),
        "thread": ("B,7,10,2,gen_in", "B,7,21,2,gen_in"),
        "pid": ("B,7,10,2,gen_in", "B,8,10,2,gen_in"),
        "phase": (",gen_in,160,", ",unknown,160,"),
        "event": (",2,gen_in,", ",5,gen_in,"),
        "return": (",2,read_out,195,1", ",2,read_out,195,0"),
        "negative": (",gen_in,160,", ",gen_in,-1,"),
        "overflow": (",gen_in,160,", ",gen_in,18446744073709551616,"),
        "mixed_json": ("B,7,10,2,gen_in,160,0", 'B,7,10,2,gen_in,1{"validation":"FAIL"}60,0'),
        "eof_extra": ("B,7,20,4,observe_in", "B,7,20,4,stamp"),
        "ordering": (",2,read_in,190,0", ",2,read_in,179,0"),
        "offset": (",1,stamp,110,1105", ",1,stamp,110,1005"),
    }
    old, new = changes[fault]
    assert old in text
    paths[0].write_text(text.replace(old, new))
    with pytest.raises(EvidenceParseError):
        analyze(paths)


@pytest.mark.parametrize("fault", ["stamp", "due", "prefix", "count", "header", "percentile", "bool", "completion"])
def test_csv_and_native_evidence_refuse_inconsistency(tmp_path, fault):
    boundary, csv, report = fixture(tmp_path)
    if fault in {"stamp", "due", "prefix", "count", "header"}:
        text = csv.read_text()
        old, new = {
            "stamp": ("1290,1310", "1290,1311"),
            "due": ("1290,1310", "1291,1310"),
            "prefix": ("3,1,1290", "3,0,1290"),
            "count": ("3,1,1290,1310\n", ""),
            "header": ("event,prior_rich_entries", "event,prefix"),
        }[fault]
        csv.write_text(text.replace(old, new))
    elif fault == "percentile":
        report["arrival"]["p50_ns"] = 16
    elif fault == "bool":
        report["accepted_batches"] = True
    else:
        report["producer_phase"] = 3
    with pytest.raises(EvidenceParseError):
        analyze((boundary, csv, report))


def test_positive_explicit_caps_and_no_input_writes(tmp_path):
    paths = fixture(tmp_path)
    original = [p.read_bytes() for p in paths[:2]]
    with pytest.raises(EvidenceParseError):
        analyze(paths, max_bytes=sum(map(len, original)) - 1)
    with pytest.raises(ValueError):
        analyze(paths, max_bytes=0)
    with pytest.raises(ValueError):
        analyze_ring_arrival_boundaries(*paths, top_n=0)
    with pytest.raises(EvidenceParseError):
        analyze_ring_arrival_boundaries(*paths, max_events=2, top_n=1)
    assert [p.read_bytes() for p in paths[:2]] == original


def test_overlong_line_and_symlink_refuse(tmp_path):
    paths = fixture(tmp_path)
    paths[0].write_text("B" * 193 + "\n")
    with pytest.raises(EvidenceParseError):
        analyze(paths)
    alias = tmp_path / "alias"
    alias.symlink_to(paths[1])
    with pytest.raises(EvidenceParseError):
        analyze((alias, paths[1], paths[2]))


@pytest.mark.parametrize("separator", ["\v", "\f", "\r", "\n\n"])
def test_only_lf_or_crlf_and_trailing_blank_records(tmp_path, separator):
    paths = fixture(tmp_path)
    data = paths[0].read_bytes()
    paths[0].write_bytes(data.replace(b"\n", separator.encode("ascii"), 1))
    with pytest.raises(EvidenceParseError):
        analyze(paths)


def test_crlf_and_optional_final_newline_are_accepted(tmp_path):
    paths = fixture(tmp_path)
    paths[0].write_bytes(paths[0].read_bytes().replace(b"\n", b"\r\n") + b"\r\n")
    paths[1].write_bytes(paths[1].read_bytes().rstrip(b"\n"))
    assert analyze(paths)["analysis_validation"] == "PASS"


def test_huge_claimed_horizon_refuses_before_event_allocation(tmp_path):
    import tracemalloc

    paths = fixture(tmp_path)
    report = paths[2]
    huge = 10**12
    report.update(
        measured_events=huge - 1,
        accepted_batches=huge,
        output_rows=huge,
        output_events=huge,
        source_entries=huge + 2,
        accepted_rows=huge + 2,
    )
    report["arrival"].update(samples=huge - 1, trace_rows=huge - 1, measured_source_entries=huge)
    tracemalloc.start()
    try:
        with pytest.raises(EvidenceParseError, match="boundary count mismatch"):
            analyze_ring_arrival_boundaries(*paths, max_events=huge, top_n=1)
        _, peak = tracemalloc.get_traced_memory()
        assert peak < 2 << 20
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize("source_case", ["late", "ambiguous"])
def test_source_lateness_classification_uses_both_offset_bounds(tmp_path, source_case):
    paths = fixture(tmp_path)
    text = paths[0].read_text()
    times = (
        [309, 309, 309, 310, 309, 309, 310, 320, 310]
        if source_case == "late"
        else [308, 308, 308, 310, 308, 309, 309, 320, 310]
    )
    phases = [
        "gen_in",
        "gen_out",
        "commit_in",
        "commit_out",
        "read_in",
        "read_out",
        "observe_in",
        "observe_out",
        "stamp",
    ]
    for phase, stamp in zip(phases, times, strict=True):
        tid = 10 if phase.startswith(("gen_", "commit_")) else 20
        old = next(line for line in text.splitlines() if line.startswith(f"B,7,{tid},3,{phase},"))
        arg = 1310 if phase == "stamp" else 1 if phase == "read_out" else 0
        text = text.replace(old, f"B,7,{tid},3,{phase},{stamp},{arg}")
    paths[0].write_text(text)
    counts = analyze(paths)["late_events"]
    assert counts["source_definitely_over_gate"] == (source_case == "late")
    assert counts["source_ambiguous"] == (source_case == "ambiguous")


def cpu_fixture(tmp_path):
    paths = fixture(tmp_path)
    producer = {"gen_in": 0, "gen_out": 4, "commit_in": 6, "commit_out": 10}
    consumer = {"read_in": 0, "read_out": 3, "observe_in": 6, "stamp": 10, "observe_out": 35}
    lines = []
    for line in paths[0].read_text().splitlines():
        fields = line.split(",")
        event, phase = int(fields[3]), fields[4]
        if event == 4:
            cpu = 650 if phase == "observe_in" else 652
        elif phase in producer:
            cpu = 10000 + (event - 1) * 30 + producer[phase]
        else:
            cpu = 500 + (event - 1) * 50 + consumer[phase]
        lines.append(f"{line},{cpu}")
    paths[0].write_text("\n".join(lines) + "\n")
    return paths


def test_literal_thread_cpu_differences_and_negative_residuals_remain_visible(tmp_path):
    paths = cpu_fixture(tmp_path)
    report = analyze(paths)
    assert report["analysis_validation"] == "PASS" and report["performance_validation"] == "FAIL"
    cpu = report["thread_cpu"]
    assert cpu["clock"] == "DTrace vtimestamp_ns"
    assert cpu["whole_thread_cpu_advance_ns"] == {"producer": 70, "consumer": 152}
    assert cpu["negative_residual_spans"] == 4
    assert "not identify preemption" in cpu["instrumentation_disclaimer"]
    row = report["worst_events"][0]
    assert row["event"] == 3
    assert row["thread_cpu"]["generation"] == dict(wall_ns=10, thread_cpu_ns=4, wall_minus_thread_cpu_ns=6)
    assert row["thread_cpu"]["post_generation_producer"] == dict(
        wall_ns=10, thread_cpu_ns=2, wall_minus_thread_cpu_ns=8
    )
    assert row["thread_cpu"]["commit_bracket"] == dict(wall_ns=25, thread_cpu_ns=4, wall_minus_thread_cpu_ns=21)
    assert row["thread_cpu"]["reader_validation"] == dict(wall_ns=5, thread_cpu_ns=3, wall_minus_thread_cpu_ns=2)
    assert row["thread_cpu"]["downstream"] == dict(wall_ns=5, thread_cpu_ns=3, wall_minus_thread_cpu_ns=2)
    assert row["thread_cpu"]["observer"] == dict(wall_ns=20, thread_cpu_ns=29, wall_minus_thread_cpu_ns=-9)
    assert row["thread_cpu"]["producer_work"] == dict(wall_ns=45, thread_cpu_ns=10, wall_minus_thread_cpu_ns=35)
    assert row["thread_cpu"]["consumer_work"] == dict(wall_ns=30, thread_cpu_ns=35, wall_minus_thread_cpu_ns=-5)
    assert row["thread_cpu"]["prior_producer_gap"] == dict(wall_ns=55, thread_cpu_ns=20, wall_minus_thread_cpu_ns=35)
    assert row["thread_cpu"]["prior_consumer_gap"] == dict(wall_ns=70, thread_cpu_ns=15, wall_minus_thread_cpu_ns=55)
    observer = cpu["phase_summaries"]["observer"]
    assert observer["negative_residual_spans"] == 2
    assert observer["wall_minus_thread_cpu_ns"] == dict(p50_ns=-9, p99_ns=-9, max_ns=-9)
    assert "publication_to_read_lower" not in cpu["phase_summaries"]
    assert "publication_to_read_upper" not in cpu["phase_summaries"]
    json.dumps(report, allow_nan=False)


def test_seven_column_legacy_capture_has_no_cpu_attribution(tmp_path):
    result = analyze(fixture(tmp_path))
    assert result["thread_cpu"] is None
    assert all("thread_cpu" not in row for row in result["worst_events"])


@pytest.mark.parametrize(
    "fault",
    [
        "mixed",
        "missing",
        "ninth",
        "malformed",
        "negative",
        "overflow",
        "producer_phase",
        "producer_reset",
        "consumer_reset",
        "eof_reset",
        "all_zero",
        "producer_zero",
    ],
)
def test_cpu_records_refuse_missing_mixed_reset_and_unavailable_accounting(tmp_path, fault):
    paths = cpu_fixture(tmp_path)
    lines = paths[0].read_text().splitlines()
    if fault in {"all_zero", "producer_zero"}:
        for index, line in enumerate(lines):
            fields = line.split(",")
            if fault == "all_zero" or fields[2] == "10":
                fields[7] = "0"
                lines[index] = ",".join(fields)
    else:
        target = {
            "producer_phase": ("1", "gen_out"),
            "producer_reset": ("2", "gen_in"),
            "consumer_reset": ("2", "read_in"),
            "eof_reset": ("4", "observe_in"),
        }.get(fault, ("2", "gen_in"))
        index = next(i for i, line in enumerate(lines) if tuple(line.split(",")[3:5]) == target)
        fields = lines[index].split(",")
        if fault in {"mixed", "missing"}:
            if fault == "mixed":
                fields.pop()
            else:
                fields[7] = ""
        elif fault == "ninth":
            fields.append("0")
        else:
            fields[7] = {
                "malformed": "1x",
                "negative": "-1",
                "overflow": "18446744073709551616",
                "producer_phase": "9999",
                "producer_reset": "10005",
                "consumer_reset": "534",
                "eof_reset": "634",
            }[fault]
        lines[index] = ",".join(fields)
    paths[0].write_text("\n".join(lines) + "\n")
    with pytest.raises(EvidenceParseError):
        analyze(paths)


def test_invalid_cpu_clock_refuses_before_csv_percentile_work(tmp_path, monkeypatch):
    from devtools_mcp.evidence import ring_arrival

    paths = cpu_fixture(tmp_path)
    text = paths[0].read_text().replace("B,7,20,4,observe_in,330,0,650", "B,7,20,4,observe_in,330,0,634")
    paths[0].write_text(text)

    def forbidden(*_args):
        raise AssertionError("CSV/percentile processing ran before CPU validation")

    monkeypatch.setattr(ring_arrival, "_csv", forbidden)
    with pytest.raises(EvidenceParseError, match="EOF thread CPU"):
        analyze(paths)


def test_predecessors_have_independent_budget_and_never_include_warm_or_worst(tmp_path):
    paths = cpu_fixture(tmp_path)
    result = analyze_ring_arrival_boundaries(*paths, max_events=3, top_n=1, preceding_events=3)
    assert [row["event"] for row in result["worst_events"]] == [3]
    window = result["worst_event_predecessors"]
    assert {key: value for key, value in window.items() if key != "events"} == dict(
        requested=3, available=1, start_event=2, end_event=2
    )
    assert [row["event"] for row in window["events"]] == [2]
    assert window["events"][0]["due_ns"] == 1190
    assert window["events"][0]["latency_ns"] == 15
    assert window["events"][0]["thread_cpu"]["observer"] == dict(
        wall_ns=20, thread_cpu_ns=29, wall_minus_thread_cpu_ns=-9
    )
    empty = analyze(paths)["worst_event_predecessors"]
    assert empty == dict(requested=0, available=0, start_event=None, end_event=None, events=[])


def test_multiple_predecessors_remain_chronological_after_latency_sort(tmp_path):
    paths = fixture(tmp_path)
    lines = paths[0].read_text().splitlines()[:-2]
    for phase, stamp, arg in (
        ("gen_in", 360, 0),
        ("gen_out", 370, 0),
        ("commit_in", 380, 0),
        ("commit_out", 405, 0),
        ("read_in", 390, 0),
        ("read_out", 395, 1),
        ("observe_in", 400, 0),
        ("stamp", 430, 1420),
        ("observe_out", 440, 0),
    ):
        tid = 10 if phase.startswith(("gen_", "commit_")) else 20
        lines.append(f"B,7,{tid},4,{phase},{stamp},{arg}")
    lines += ["B,7,20,5,observe_in,450,0", "B,7,20,5,observe_out,460,0"]
    paths[0].write_text("\n".join(lines) + "\n")
    paths[1].write_text(paths[1].read_text() + "4,2,1390,1420\n")
    paths[2].update(
        measured_events=3, accepted_batches=4, output_rows=4, output_events=4, source_entries=6, accepted_rows=6
    )
    paths[2]["arrival"].update(samples=3, trace_rows=3, measured_source_entries=4, p50_ns=20, p99_ns=30, max_ns=30)
    result = analyze_ring_arrival_boundaries(*paths, max_events=4, top_n=1, preceding_events=3)
    assert [row["event"] for row in result["worst_events"]] == [4]
    window = result["worst_event_predecessors"]
    assert (window["requested"], window["available"], window["start_event"], window["end_event"]) == (3, 2, 2, 3)
    assert [(row["event"], row["latency_ns"]) for row in window["events"]] == [(2, 15), (3, 20)]
    assert all("thread_cpu" not in row for row in window["events"])


def test_worst_first_measured_event_reports_empty_available_window(tmp_path):
    paths = fixture(tmp_path)
    paths[0].write_text(paths[0].read_text().replace("B,7,20,2,stamp,210,1205", "B,7,20,2,stamp,210,1215"))
    paths[1].write_text(paths[1].read_text().replace("1190,1205", "1190,1215"))
    paths[2]["arrival"].update(p50_ns=20, p99_ns=25, max_ns=25)
    result = analyze_ring_arrival_boundaries(*paths, max_events=3, top_n=1, preceding_events=3)
    assert result["worst_events"][0]["event"] == 2
    assert result["worst_event_predecessors"] == dict(
        requested=3, available=0, start_event=None, end_event=None, events=[]
    )


@pytest.mark.parametrize("bad", [True, False, -1, 4, 0.0, "1", None])
def test_predecessor_cap_is_exact_and_validated_before_io(bad):
    with pytest.raises(ValueError, match="preceding_events"):
        analyze_ring_arrival_boundaries("missing-boundaries", "missing-arrival", {}, max_events=3, preceding_events=bad)
