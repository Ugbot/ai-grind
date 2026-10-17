"""Independent literal clock bracket / scheduler coverage oracles."""

import json

import pytest

from devtools_mcp.evidence import EvidenceParseError
from devtools_mcp.evidence.instruments_ring import analyze_instruments_ring_arrival as analyze
from devtools_mcp.evidence.ring_arrival import _benchmark, _boundaries, _csv, _read
from tests.test_evidence_ring_arrival import fixture as ring_fixture
from tests.test_instruments_threads import fixture as info_fixture
from tests.test_instruments_threads import replace

SP_COLUMNS = (
    "time:event-time thread:thread process:process event-type:event-type scope:string "
    "identifier:os-signpost-identifier name:signpost-name format-string:format-string "
    "backtrace:text-backtrace subsystem:subsystem category:category "
    "message:os-log-metadata emit-location:return-location"
)
STATE_COLUMNS = (
    "start:start-time thread:thread state:thread-state duration:duration process:process "
    "core:core cputime:duration-on-core waittime:duration-waiting priority:sched-priority "
    "note:narrative summary:narrative made-runnable-by-thread:thread "
    "preempted-by-thread:thread yielded-to-thread:thread rebalanced-from-cpu:core "
    "thermal-throttled:boolean"
)
PROCESS = '<process id="1" fmt="bench (7)"><pid>7</pid><device-session>TODO</device-session></process>'
THREAD = '<thread id="3"><tid>11</tid><process ref="1"/></thread>'


def schema(name, columns):
    fields = "".join(
        (f"<col><mnemonic>{a}</mnemonic><name>{a}</name><engineering-type>{b}</engineering-type></col>")
        for a, b in (x.split(":") for x in columns.split())
    )
    return f'<schema name="{name}">{fields}</schema>'


def fixture(tmp_path):
    info = info_fixture(tmp_path)[0]
    _, csv, report = ring_fixture(tmp_path)
    report["producer_error"] = 0
    report["clock_anchor"] = dict(
        requested=True,
        status="emitted",
        name="RingClockAnchor",
        subsystem="com.ugbot.chukonu.benchmark",
        signpost_id=1,
        native_before_ns=2000,
        native_after_ns=2010,
    )
    native = tmp_path / "native.json"
    native.write_text(json.dumps(report) + "\n")
    row = (
        f"<row>"
        f'<event-time id="10">1000</event-time>{THREAD}{PROCESS}<event-type>Event</event-type>'
        f"<string>Process</string><os-signpost-identifier>1</os-signpost-identifier>"
        f"<signpost-name>RingClockAnchor</signpost-name>"
        f"<format-string>native_before_ns=%{{public}}llu</format-string><sentinel/>"
        f"<subsystem>com.ugbot.chukonu.benchmark</subsystem>"
        f"<category>PointsOfInterest</category><os-log-metadata>"
        f"<narrative-text>native_before_ns=</narrative-text><uint64>2000</uint64>"
        f"</os-log-metadata><sentinel/></row>"
    )
    # References may point forward; all projection rows share the file namespace.
    projected = (
        row.replace('<event-time id="10">1000</event-time>', '<event-time ref="10"/>')
        .replace(PROCESS, '<process ref="1"/>')
        .replace(THREAD, '<thread ref="3"/>')
    )
    signpost = tmp_path / "signpost.xml"
    body = '<node xpath="table1">' + schema("os-signpost", SP_COLUMNS) + row + "</node>"
    body += "".join(f'<node xpath="table{x}">{projected}</node>' for x in (2, 3, 4))
    signpost.write_text("<trace-query-result>" + body + "</trace-query-result>")
    state = tmp_path / "state.xml"
    rows = []
    for index, (start, duration, code, core) in enumerate(
        ((0, 285, "Running", 2), (285, 15, "Preempted", None), (305, 695, "Running", 2))
    ):
        process = PROCESS if index == 0 else '<process ref="1"/>'
        thread = THREAD if index == 0 else '<thread ref="3"/>'
        cpu = "<sentinel/>" if core is None else f'<core fmt="P Core">{core}</core>'
        rows.append(
            (
                f"<row>"
                f"<start-time>{start}</start-time>{thread}<thread-state "
                f'fmt="{code}">{code}</thread-state>'
                f"<duration>{duration}</duration>{process}{cpu}<sentinel/><sentinel/>"
                f'<sched-priority fmt="Default - 31">31</sched-priority>'
            )
            + ("<sentinel/>" * 7)
            + "</row>"
        )
    state.write_text(
        '<trace-query-result><node xpath="state1">'
        + schema("thread-state", STATE_COLUMNS)
        + "".join(rows)
        + "</node></trace-query-result>"
    )
    return native, csv, signpost, state, info


def run(paths, **kwargs):
    return analyze(*paths, pid=7, **kwargs)


def test_default_no_clock_join_even_valid_four_projection_marker(tmp_path):
    result = run(fixture(tmp_path))
    assert (result["analysis_validation"], result["performance_validation"], result["clock_join_validation"]) == (
        "PASS",
        "FAIL",
        "UNVERIFIED",
    )
    assert result["worst_events"] == [] and result["clock_drift"] == "UNMEASURED"
    assert result["anchor"]["projection_count"] == 4
    assert (result["anchor"]["offset_lower_ns"], result["anchor"]["offset_upper_ns"]) == (-1010, -1000)
    assert result["native_metrics"]["latency"] == dict(p50_ns=15, p99_ns=20, max_ns=20)


def test_conditional_possible_and_guaranteed_literal_coverage(tmp_path):
    r = run(fixture(tmp_path), assume_constant_offset=True, top_n=1, state_details=1)
    assert r["clock_join_validation"] == "CONDITIONAL"
    event = r["worst_events"][0]
    assert event["event"] == 3
    window = event["due_to_terminal"]
    assert (window["native_start_ns"], window["native_end_ns"]) == (1290, 1310)
    possible = window["possible_envelope"]
    core = window["guaranteed_core"]
    assert (possible["start_ns"], possible["end_ns"], possible["covered_ns"], possible["uncovered_ns"]) == (
        280,
        310,
        25,
        5,
    )
    assert (
        possible["detail_count"] == 3
        and possible["omitted_detail_count"] == 2
        and possible["omitted_detail_duration_ns"] == 20
    )
    assert possible["gaps"] == [dict(start_ns=300, end_ns=305, duration_ns=5)]
    assert possible["state_durations"][0] == dict(code="Preempted", label="Preempted", duration_ns=15)
    assert possible["omitted_state_duration_ns"] == 10
    assert (core["start_ns"], core["end_ns"], core["covered_ns"]) == (290, 300, 10)
    previous = event["prior_terminal_to_terminal"]
    assert (previous["native_start_ns"], previous["native_end_ns"]) == (1205, 1310)
    assert event["due_to_terminal"]["possible_envelope"]["details"][0]["core"] == dict(code=2, label="P Core")


def test_shifted_marker_changes_conditional_coordinates_not_authenticated_identity(tmp_path):
    paths = fixture(tmp_path)
    replace(paths[2], ">1000</event-time>", ">1020</event-time>")
    r = run(paths, assume_constant_offset=True, top_n=1)
    assert r["anchor"]["offset_lower_ns"] == -990
    assert r["worst_events"][0]["due_to_terminal"]["possible_envelope"]["start_ns"] == 300
    assert "do not authenticate" in r["provenance"]


def test_empty_guaranteed_core_and_first_measured_prior_is_unavailable(tmp_path):
    paths = fixture(tmp_path)
    report = json.loads(paths[0].read_text())
    report["clock_anchor"]["native_after_ns"] = 2030
    paths[0].write_text(json.dumps(report))
    r = run(paths, assume_constant_offset=True, top_n=2)
    assert r["worst_events"][0]["due_to_terminal"]["guaranteed_core"] is None
    assert r["worst_events"][1]["prior_terminal_unavailable"]


@pytest.mark.parametrize(
    ("index", "old", "new", "message"),
    [
        (2, '<node xpath="table2">', '<node xpath="table1">', "table identity"),
        (2, '<event-time ref="10"/>', "<event-time>1001</event-time>", "contradictory"),
        (2, "<uint64>2000</uint64>", "<uint64>2001</uint64>", "payload"),
        (2, "<signpost-name>RingClockAnchor</signpost-name>", "<signpost-name>Different</signpost-name>", "name"),
        (2, "<os-signpost-identifier>1", "<os-signpost-identifier>2", "kind/id"),
        (2, "<tid>11</tid>", "<tid>99</tid>", "unknown"),
        (2, "<pid>7</pid>", "<pid>8</pid>", "ownership|missing|identity"),
        (2, '<thread ref="3"/>', '<thread ref="999"/>', "missing"),
        (3, "<start-time>285</start-time>", "<start-time>284</start-time>", "overlapping"),
        (3, "<duration>15</duration>", "<duration>18446744073709551615</duration>", "overflow"),
        (3, "<tid>11</tid>", "<tid>99</tid>", "unknown"),
        (3, "<mnemonic>start</mnemonic>", "<mnemonic>wrong</mnemonic>", "schema"),
        (1, "1190,1205", "1191,1205", "due"),
        (1, "1290,1310", "1290,1311", "percentiles"),
    ],
)
def test_invalid_evidence_refused(tmp_path, index, old, new, message):
    paths = fixture(tmp_path)
    replace(paths[index], old, new)
    with pytest.raises(EvidenceParseError, match=message):
        run(paths)


def test_duplicate_marker_within_table_refused(tmp_path):
    paths = fixture(tmp_path)
    text = paths[2].read_text()
    projection = text.split('<node xpath="table2">')[1].split("</node>")[0]
    replace(paths[2], '<node xpath="table2">' + projection, '<node xpath="table2">' + projection + projection)
    with pytest.raises(EvidenceParseError, match="within table"):
        run(paths)


def test_contradictory_projection_schema_refused(tmp_path):
    paths = fixture(tmp_path)
    replace(paths[2], '<node xpath="table2">', '<node xpath="table2">' + schema("wrong", SP_COLUMNS))
    with pytest.raises(EvidenceParseError, match="schema"):
        run(paths)


def test_existing_real_dtrace_stamp_comparison_still_required(tmp_path):
    boundary, csv, report = ring_fixture(tmp_path)
    warm, total, arrival = _benchmark(report, 3)
    events, *_ = _boundaries(_read(boundary, 8192, 99)[0], total)
    events[2]["stamp"] = (210, 1206)
    with pytest.raises(EvidenceParseError, match="stamp mismatch"):
        _csv(_read(csv, 8192, 9)[0], warm, total, arrival, events)


@pytest.mark.parametrize("key", ["pid", "max_bytes", "max_rows", "max_nodes", "max_threads", "top_n", "state_details"])
@pytest.mark.parametrize("value", [True, 0, -1, 1.5, "2"])
def test_strict_positive_controls_before_io(key, value):
    kwargs = dict(pid=7)
    kwargs[key] = value
    with pytest.raises(ValueError):
        analyze(*(["absent"] * 5), **kwargs)


@pytest.mark.parametrize("value", [0, 1, "true", None])
def test_assumption_requires_explicit_builtin_boolean(value):
    with pytest.raises(ValueError, match="builtin bool"):
        analyze(*(["absent"] * 5), pid=7, assume_constant_offset=value)


@pytest.mark.parametrize(
    ("key", "value"), [("max_bytes", 100), ("max_rows", 5), ("max_nodes", 100), ("max_threads", 1)]
)
def test_combined_caps_refuse(tmp_path, key, value):
    with pytest.raises(ValueError):
        run(fixture(tmp_path), **{key: value})


def test_observed_unknown_state_is_distinct_from_missing_time(tmp_path):
    paths = fixture(tmp_path)
    replace(paths[3], ">Preempted</thread-state>", ">Unknown</thread-state>")
    r = run(paths, assume_constant_offset=True, top_n=1)
    window = r["worst_events"][0]["due_to_terminal"]["possible_envelope"]
    assert window["observed_unknown_state_ns"] == 15 and window["uncovered_ns"] == 5


def test_csv_terminal_order_is_checked_without_dtrace(tmp_path):
    paths = fixture(tmp_path)
    report = json.loads(paths[0].read_text())
    report["validation"] = "PASS"
    report["arrival"].update(
        target_rich_entries_per_second=1000000000, p50_ns=13, p99_ns=15, max_ns=15, validation="PASS"
    )
    paths[0].write_text(json.dumps(report))
    replace(paths[1], "1290,1310", "1191,1204")
    with pytest.raises(EvidenceParseError, match="terminal order"):
        run(paths)


def test_reverse_native_bracket_and_early_finish_refused(tmp_path):
    paths = fixture(tmp_path)
    report = json.loads(paths[0].read_text())
    report["clock_anchor"]["native_after_ns"] = 1999
    paths[0].write_text(json.dumps(report))
    with pytest.raises(EvidenceParseError, match="reversed"):
        run(paths)
    report["clock_anchor"].update(native_before_ns=1300, native_after_ns=1310)
    paths[0].write_text(json.dumps(report))
    with pytest.raises(EvidenceParseError, match="precedes"):
        run(paths)


@pytest.mark.parametrize("bad", ["dtd", "utf8"])
def test_unsafe_xml_is_rejected_in_ring_evidence(tmp_path, bad):
    paths = fixture(tmp_path)
    if bad == "dtd":
        replace(paths[3], "<trace-query-result>", "<!DOCTYPE x><trace-query-result>")
    else:
        paths[3].write_bytes(b"\xff")
    with pytest.raises(EvidenceParseError):
        run(paths)


def test_final_binding_verification_detects_change_after_validation(tmp_path, monkeypatch):
    from devtools_mcp.evidence import instruments_ring

    paths = fixture(tmp_path)
    original = instruments_ring.states

    def changed(*args):
        result = original(*args)
        paths[0].write_text("{}")
        return result

    monkeypatch.setattr(instruments_ring, "states", changed)
    with pytest.raises(ValueError, match="binding mismatch"):
        run(paths)
