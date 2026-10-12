"""Strict parsing must preserve missing/bad facts as failures, never defaults."""

import pytest

from devtools_mcp.evidence import EvidenceParseError, MetricField, decode_json_records, parse_metric_line

SCHEMA = {
    "rows": MetricField("integer", minimum=0),
    "seconds": MetricField("number", minimum=0),
    "durable": MetricField("boolean"),
    "engine": MetricField("text"),
}
VALID = "RESULT_V1 rows=12 seconds=1.5e-2 durable=1 engine=native"


def test_schema_metrics_are_typed_and_prefix_literal():
    result = parse_metric_line(VALID, SCHEMA, prefix="RESULT_V1")
    assert result == {"rows": 12, "seconds": 0.015, "durable": True, "engine": "native"}
    assert type(result["rows"]) is int and type(result["durable"]) is bool


@pytest.mark.parametrize(
    "text",
    [
        VALID + " rows=13",
        VALID + " mystery=1",
        VALID.replace("rows=12 ", ""),
        VALID.replace("rows=12", "rows=-1"),
        VALID.replace("rows=12", "rows=1.0"),
        VALID.replace("rows=12", "rows="),
        VALID.replace("durable=1", "durable=true"),
        VALID.replace("seconds=1.5e-2", "seconds=NaN"),
        VALID.replace("seconds=1.5e-2", "seconds=inf"),
        VALID.replace("seconds=1.5e-2", "seconds=1e999"),
        VALID.replace("seconds=1.5e-2", "seconds=1_000"),
        VALID.replace("RESULT_V1", "RESULT_V0"),
        VALID + "\n" + VALID,
        VALID + " junk",
        "",
    ],
)
def test_ambiguous_incomplete_and_malformed_metrics_fail(text: str):
    with pytest.raises(EvidenceParseError):
        parse_metric_line(text, SCHEMA, prefix="RESULT_V1")


def test_metric_bounds_and_text_size():
    schema = {"ratio": MetricField("number", minimum=0, maximum=1)}
    assert parse_metric_line("ratio=1", schema)["ratio"] == 1
    with pytest.raises(EvidenceParseError):
        parse_metric_line("ratio=1.01", schema)
    with pytest.raises(EvidenceParseError):
        parse_metric_line("ratio=1", schema, max_bytes=4)
    with pytest.raises(EvidenceParseError):
        parse_metric_line("name=éé", {"name": MetricField("text")}, max_bytes=8)


@pytest.mark.parametrize(
    "field",
    [
        lambda: MetricField("number", minimum=float("nan")),
        lambda: MetricField("text", minimum=0),
        lambda: MetricField("number", minimum=2, maximum=1),
    ],
)
def test_invalid_schema_bounds(field):
    with pytest.raises(ValueError):
        field()


def test_jsonl_objects_and_empty_input():
    assert decode_json_records('{"phase":"start"}\n{"phase":"end","seconds":0.25}\n') == (
        {"phase": "start"},
        {"phase": "end", "seconds": 0.25},
    )
    assert decode_json_records("") == ()


@pytest.mark.parametrize(
    "text",
    [
        '{"x":1,"x":2}',
        '{"nested":{"x":1,"x":2}}',
        '{"x":NaN}',
        '{"x":Infinity}',
        '{"nested":[1e999]}',
        "[]",
        "null",
        "{}\n\n{}",
        "{broken}",
    ],
)
def test_jsonl_rejects_ambiguous_nonfinite_and_nonobject(text: str):
    with pytest.raises(EvidenceParseError):
        decode_json_records(text)


def test_jsonl_limits():
    assert len(decode_json_records("{}\n{}", max_records=2)) == 2
    with pytest.raises(EvidenceParseError):
        decode_json_records("{}\n{}", max_records=1)
    with pytest.raises(EvidenceParseError):
        decode_json_records('{"x":"large"}', max_bytes=4)
    with pytest.raises(ValueError):
        decode_json_records("{}", max_records=0)


def test_json_depth_has_fixed_cap_and_ignores_escaped_strings():
    with pytest.raises(EvidenceParseError, match="nesting"):
        decode_json_records('{"x":' + "[" * 64 + "0" + "]" * 64 + "}")
    assert decode_json_records('{"x":' + "[" * 63 + "0" + "]" * 63 + "}")
    assert decode_json_records('{"x":"[[[\\"{{{"}') == ({"x": '[[["{{{'},)


@pytest.mark.parametrize("character", ["\u2028", "\u2029", "\u0085"])
def test_jsonl_preserves_unicode_line_characters_inside_strings(character: str):
    assert decode_json_records('{"value":"before' + character + 'after"}\n') == (
        {"value": "before" + character + "after"},
    )


@pytest.mark.parametrize("separator", ["\r", "\x1c", "\x1d", "\x1e", "\v", "\f", "\u0085", "\u2028", "\u2029"])
def test_jsonl_rejects_non_lf_record_separators(separator: str):
    with pytest.raises(EvidenceParseError):
        decode_json_records("{}" + separator + "{}")


@pytest.mark.parametrize("text", ["{}\r\n{}\r\n", "{}\n{}\n", "{}\r\n{}", "{}\n{}"])
def test_jsonl_accepts_lf_and_crlf_with_optional_final_terminator(text: str):
    assert decode_json_records(text) == ({}, {})


@pytest.mark.parametrize("text", ["\n", "\r\n", "{}\n\n", "{}\r\n\r\n", "\n{}", "{}\n \n"])
def test_jsonl_rejects_blank_lf_records(text: str):
    with pytest.raises(EvidenceParseError):
        decode_json_records(text)
