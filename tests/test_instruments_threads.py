"""Literal xctrace DAG fixtures with sparse coverage and independent durations."""

from pathlib import Path

import pytest

from devtools_mcp.evidence import EvidenceParseError, instruments_xml
from devtools_mcp.evidence.instruments_threads import analyze_instruments_thread_policy as analyze

INFO = [
    ("time", "event-time"),
    ("pid", "pid"),
    ("tid", "tid"),
    ("process", "process"),
    ("thread", "thread"),
    ("name", "thread-name"),
    ("main-thread", "boolean"),
]
QOS = [
    ("start", "start-time"),
    ("duration", "duration"),
    ("process", "process"),
    ("thread", "thread"),
    ("requested-qo-s", "quality-of-service-class"),
    ("effective-qo-s", "quality-of-service-class"),
    ("mismatch-qo-s", "state"),
]
PRI = [
    ("start", "start-time"),
    ("duration", "duration"),
    ("process", "process"),
    ("thread", "thread"),
    ("scheduled-priority", "sched-priority"),
    ("base-priority", "sched-priority"),
]


def export(name, columns, rows):
    schema = "".join(
        f"<col><mnemonic>{key}</mnemonic><name>{key}</name><engineering-type>{kind}</engineering-type></col>"
        for key, kind in columns
    )
    return (
        f'<?xml version="1.0"?><trace-query-result><node xpath="caller">'
        f'<schema name="{name}">{schema}</schema>{rows}</node></trace-query-result>'
    )


def fixture(tmp_path):
    process = '<process id="1" fmt="bench (7)"><pid id="2">7</pid><device-session>TODO</device-session></process>'
    thread = '<thread id="3"><tid>11</tid><process ref="1"/></thread>'
    other = '<thread id="4"><tid>12</tid><process ref="1"/></thread>'
    info = (
        f"<row><event-time>0</event-time><pid>7</pid>"
        f'<tid>11</tid>{process}{thread}<thread-name><string>main</string><thread ref="3"/>'
        f"</thread-name><boolean>1</boolean></row>"
    )
    info += (
        f'<row><event-time>0</event-time><pid ref="2"/><tid>12</tid>'
        f'<process ref="1"/>{other}<thread-name><sentinel/><sentinel/></thread-name>'
        f"<boolean>0</boolean></row>"
    )
    foreign = (
        '<process id="8" fmt="foreign (8)"><pid>8</pid><device-session>TODO</device-session>'
        '</process><thread id="9"><tid>21</tid><process ref="8"/></thread>'
    )
    info += (
        f"<row><event-time>0</event-time><pid>8</pid><tid>21</tid>{foreign}<thread-name>"
        f"<sentinel/><sentinel/></thread-name><boolean>0</boolean></row>"
    )
    qos = (
        f"<row><start-time>80</start-time>"
        f'<duration>40</duration>{process}{thread}<quality-of-service-class id="5" '
        f'fmt="Unspecified">6</quality-of-service-class><quality-of-service-class ref="5"/>'
        f"<sentinel/></row>"
    )
    qos += (
        f"<row><start-time>100</start-time><duration>100</duration>"
        f'<process ref="1"/>{other}<quality-of-service-class id="6" '
        f'fmt="Default">2</quality-of-service-class><quality-of-service-class ref="6"/>'
        f"<sentinel/></row>"
    )
    qos += (
        '<row><start-time>160</start-time><duration>50</duration><process ref="1"/>'
        '<thread ref="3"/><quality-of-service-class ref="6"/>'
        '<quality-of-service-class ref="5"/><state>Mismatch</state></row>'
    )
    qos += (
        f"<row><start-time>0</start-time>"
        f'<duration>1</duration>{foreign}<quality-of-service-class ref="6"/>'
        f'<quality-of-service-class ref="6"/><sentinel/></row>'
    )
    pri = (
        f"<row><start-time>0</start-time>"
        f'<duration>300</duration>{process}{thread}<sched-priority id="5" fmt="Default - '
        f'31">31</sched-priority><sched-priority ref="5"/></row>'
    )
    pri += (
        f"<row><start-time>0</start-time><duration>300</duration>"
        f'<process ref="1"/>{other}<sched-priority ref="5"/><sched-priority ref="5"/></row>'
    )
    paths = [tmp_path / name for name in ("info.xml", "qos.xml", "priority.xml")]
    for path, text in zip(
        paths,
        (export("thread-info", INFO, info), export("ThreadQoSTable", QOS, qos), export("ThreadPriority", PRI, pri)),
        strict=True,
    ):
        path.write_text(text)
    return paths


def run(paths, **kwargs):
    return analyze(*paths, pid=7, window_start_ns=100, window_end_ns=200, **kwargs)


def replace(path, old, new):
    text = path.read_text()
    assert old in text
    path.write_text(text.replace(old, new, 1))


def test_sparse_main_coverage_clipping_and_no_default_fill(tmp_path):
    result = run(fixture(tmp_path), top_n=1)
    assert (result["analysis_validation"], result["performance_validation"], result["coverage"]) == (
        "PASS",
        "UNVERIFIED",
        "PARTIAL",
    )
    main, other = result["threads"]
    assert main["main_thread"] and not other["main_thread"]
    q = main["qos"]
    assert (q["covered_ns"], q["uncovered_ns"], q["gap_count"]) == (60, 40, 1)
    assert q["gaps"] == [dict(start_ns=120, end_ns=160, duration_ns=40)]
    assert q["pair_count"] == 2 and q["omitted_pairs"] == 1 and q["omitted_pair_covered_ns"] == 20
    assert q["different_pair_codes_ns"] == 40
    assert q["observed_pairs"][0] == dict(
        first=dict(code=2, label="Default"),
        second=dict(code=6, label="Unspecified"),
        covered_ns=40,
        exporter_mismatch_state=dict(tag="state", text="Mismatch", label=None),
    )
    assert other["qos"]["covered_ns"] == 100 and main["priority"]["covered_ns"] == 100
    assert "does not authenticate" in result["provenance"]
    assert len(result["input_bindings"]["qos"]["sha256"]) == 64


def test_zero_and_outside_intervals_do_not_cover_or_create_overlap(tmp_path):
    paths = fixture(tmp_path)
    replace(paths[1], "<duration>40</duration>", "<duration>0</duration>")
    result = run(paths)
    q = result["threads"][0]["qos"]
    assert (q["covered_ns"], q["uncovered_ns"]) == (40, 60)
    assert q["gaps"] == [dict(start_ns=100, end_ns=160, duration_ns=60)]


@pytest.mark.parametrize(
    ("index", "old", "new", "message"),
    [
        (1, "<start-time>160</start-time>", "<start-time>110</start-time>", "overlapping"),
        (1, "<duration>40</duration>", "<duration>18446744073709551615</duration>", "overflow"),
        (1, '<thread ref="3"/>', '<thread ref="999"/>', "missing"),
        (1, '<thread ref="3"/>', '<thread ref="2"/>', "tag-mismatched"),
        (1, '<quality-of-service-class id="6"', '<quality-of-service-class id="5"', "duplicate"),
        (1, '<pid id="2">7</pid>', '<pid id="2">8</pid>', "identity|PID|ownership"),
        (1, "<tid>11</tid>", "<tid>99</tid>", "unknown"),
        (0, "<boolean>1</boolean>", "<boolean>2</boolean>", "main-thread"),
        (0, "<tid>11</tid>", "<tid>99</tid>", "identity"),
        (1, "<mnemonic>start</mnemonic>", "<mnemonic>wrong</mnemonic>", "schema"),
        (1, "<sentinel/></row>", "<boolean>0</boolean></row>", "type"),
        (1, '<thread ref="3"/>', '<thread ref="3" fmt="extra"/>', "invalid ref"),
        (
            1,
            '<quality-of-service-class id="5" fmt="Unspecified">6</quality-of-service-class>',
            '<quality-of-service-class id="5"><quality-of-service-class ref="5"/></quality-of-service-class>',
            "cyclic",
        ),
    ],
)
def test_malformed_evidence_fails_closed(tmp_path, index, old, new, message):
    paths = fixture(tmp_path)
    replace(paths[index], old, new)
    with pytest.raises(EvidenceParseError, match=message):
        run(paths)


@pytest.mark.parametrize("kind", ["dtd", "entity", "deep", "utf8"])
def test_unsafe_xml_refused(tmp_path, kind):
    paths = fixture(tmp_path)
    if kind == "dtd":
        replace(paths[0], "?><trace", "?><!DOCTYPE trace-query-result><trace")
    elif kind == "entity":
        replace(paths[0], "?><trace", '?><!DOCTYPE x [<!ENTITY boom "xx">]><trace')
    elif kind == "deep":
        replace(paths[0], "<event-time>0</event-time>", "<x>" * 65 + "0" + "</x>" * 65)
    else:
        paths[0].write_bytes(b"\xff")
    with pytest.raises(EvidenceParseError):
        run(paths)


@pytest.mark.parametrize(("key", "value"), [("max_bytes", 20), ("max_nodes", 20), ("max_rows", 3), ("max_threads", 1)])
def test_combined_caps_fail_not_truncate(tmp_path, key, value):
    with pytest.raises(ValueError, match="cap|limit"):
        run(fixture(tmp_path), **{key: value})


@pytest.mark.parametrize(
    "key", ["pid", "window_start_ns", "window_end_ns", "max_bytes", "max_rows", "max_nodes", "max_threads", "top_n"]
)
@pytest.mark.parametrize("value", [True, -1, 1.5, "2"])
def test_exact_integer_limits_precede_io(key, value):
    options = dict(pid=7, window_start_ns=100, window_end_ns=200)
    options[key] = value
    with pytest.raises(ValueError):
        analyze("absent", "absent", "absent", **options)


def test_changed_file_refused(tmp_path, monkeypatch):
    paths = fixture(tmp_path)
    original = instruments_xml.verify_binding

    def changed(binding, **kwargs):
        Path(binding.path).write_text("changed")
        original(binding, **kwargs)

    monkeypatch.setattr(instruments_xml, "verify_binding", changed)
    with pytest.raises(ValueError, match="binding mismatch"):
        run(paths)


def test_same_code_exporter_mismatch_state_survives_detail_truncation(tmp_path):
    paths = fixture(tmp_path)
    replace(
        paths[1],
        '<quality-of-service-class ref="5"/><state>Mismatch</state>',
        '<quality-of-service-class ref="6"/><state fmt="Unusual">Exporter says mismatch</state>',
    )
    result = run(paths, top_n=1)
    qos = result["threads"][0]["qos"]
    assert qos["different_pair_codes_ns"] == 0 and qos["exporter_state_present_ns"] == 40
    assert qos["observed_pairs"][0]["exporter_mismatch_state"] == dict(
        tag="state", text="Exporter says mismatch", label="Unusual"
    )


def test_foreign_rows_are_validated_before_filtering(tmp_path):
    paths = fixture(tmp_path)
    replace(
        paths[1],
        '<thread id="9"><tid>21</tid><process ref="8"/></thread>',
        '<thread id="9"><tid>21</tid><process ref="1"/></thread>',
    )
    with pytest.raises(EvidenceParseError, match="PID mismatch"):
        run(paths)


def test_cross_file_process_identity_is_stable(tmp_path):
    paths = fixture(tmp_path)
    replace(paths[2], 'fmt="bench (7)"', 'fmt="different (7)"')
    with pytest.raises(EvidenceParseError, match="process identity"):
        run(paths)


def test_mismatch_state_children_refused(tmp_path):
    paths = fixture(tmp_path)
    replace(paths[1], "<state>Mismatch</state>", "<state><string>Mismatch</string></state>")
    with pytest.raises(EvidenceParseError, match="state children"):
        run(paths)


def test_reference_only_cycle_refused(tmp_path):
    paths = fixture(tmp_path)
    replace(paths[0], "<boolean>1</boolean>", '<boolean id="90" ref="91"/>')
    replace(paths[0], "<boolean>0</boolean>", '<boolean id="91" ref="90"/>')
    with pytest.raises(EvidenceParseError, match="invalid ref|cyclic"):
        run(paths)


def test_declared_non_utf8_encoding_refused(tmp_path):
    paths = fixture(tmp_path)
    replace(paths[0], 'version="1.0"', 'version="1.0" encoding="ISO-8859-1"')
    with pytest.raises(EvidenceParseError, match="UTF-8"):
        run(paths)


def test_gap_details_are_bounded_without_hiding_total_coverage(tmp_path):
    paths = fixture(tmp_path)
    replace(
        paths[1],
        "<start-time>80</start-time><duration>40</duration>",
        "<start-time>110</start-time><duration>10</duration>",
    )
    replace(
        paths[1],
        "<start-time>160</start-time><duration>50</duration>",
        "<start-time>160</start-time><duration>10</duration>",
    )
    q = run(paths, top_n=1)["threads"][0]["qos"]
    assert q["covered_ns"] == 20 and q["uncovered_ns"] == 80 and q["gap_count"] == 3
    assert q["gaps"] == [dict(start_ns=100, end_ns=110, duration_ns=10)]
