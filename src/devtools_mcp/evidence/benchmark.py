"""Strict bounded decoding of machine-readable evidence, without scoring policy.

Missing facts never become zero/pass; callers decide benchmark acceptance.
Source/adaptation provenance: docs/research/tmp-python-mining-20261012.md.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

MetricValue = int | float | bool | str
MAX_TEXT_BYTES = 1 << 20
MAX_FIELDS = 256
MAX_RECORDS = 10_000
MAX_JSON_DEPTH = 64
_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}\Z")
_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")
_INTEGER = re.compile(r"[+-]?[0-9]{1,100}\Z")


class EvidenceParseError(ValueError):
    """Evidence is malformed, ambiguous, incomplete, or exceeds a bound."""


@dataclass(frozen=True)
class MetricField:
    kind: Literal["integer", "number", "boolean", "text"]
    minimum: float | None = None
    maximum: float | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"integer", "number", "boolean", "text"}:
            raise ValueError("unknown metric kind")
        bounds = (self.minimum, self.maximum)
        if any(v is not None and not math.isfinite(v) for v in bounds):
            raise ValueError("metric bounds must be finite")
        if self.kind not in {"integer", "number"} and any(v is not None for v in bounds):
            raise ValueError("only numeric fields accept bounds")
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("metric minimum exceeds maximum")
        assert self.kind in {"integer", "number", "boolean", "text"}
        assert len(bounds) == 2


def _bounded_text(text: str, max_bytes: int) -> None:
    if not (0 < max_bytes <= MAX_TEXT_BYTES):
        raise ValueError("text byte limit must be positive and at most 1 MiB")
    if len(text) > max_bytes or len(text.encode("utf-8")) > max_bytes:
        raise EvidenceParseError("evidence exceeds text byte limit")
    assert max_bytes <= MAX_TEXT_BYTES
    assert len(text) <= max_bytes


def _metric_value(raw: str, field: MetricField) -> MetricValue:
    assert raw
    assert isinstance(field, MetricField)
    if field.kind == "text":
        return raw
    if field.kind == "boolean":
        if raw not in {"0", "1"}:
            raise EvidenceParseError("boolean metric must be 0 or 1")
        return raw == "1"
    if field.kind == "integer":
        if not _INTEGER.fullmatch(raw):
            raise EvidenceParseError("integer metric must use decimal integer syntax")
        value: int | float = int(raw)
    else:
        if not _NUMBER.fullmatch(raw):
            raise EvidenceParseError("number metric must use decimal numeric syntax")
        try:
            value = float(raw)
        except ValueError as exc:
            raise EvidenceParseError("number metric must be numeric") from exc
        if not math.isfinite(value):
            raise EvidenceParseError("number metric must be finite")
    if (field.minimum is not None and value < field.minimum) or (field.maximum is not None and value > field.maximum):
        raise EvidenceParseError("metric lies outside its declared bounds")
    return value


def parse_metric_line(
    text: str, schema: Mapping[str, MetricField], *, prefix: str = "", max_bytes: int = 64 << 10
) -> dict[str, MetricValue]:
    """Decode exactly one whitespace-separated key=value line using a schema.

    Every declared field is required. Duplicate/unknown keys, empty values,
    malformed tokens, nonfinite numbers, and extra lines are rejected. `prefix`
    is a literal first token (e.g. a version marker), never a regular expression.
    """
    _bounded_text(text, max_bytes)
    if not (0 < len(schema) <= MAX_FIELDS) or any(not _KEY.fullmatch(key) for key in schema):
        raise ValueError("schema must have 1..256 valid field names")
    if prefix and (prefix != prefix.strip() or any(c.isspace() for c in prefix)):
        raise ValueError("prefix must be one non-whitespace token")
    stripped = text.strip()
    if not stripped or any(c in stripped for c in "\r\n"):
        raise EvidenceParseError("expected exactly one metric line")
    tokens = stripped.split(maxsplit=MAX_FIELDS + 1)
    if prefix:
        if tokens[0] != prefix:
            raise EvidenceParseError("metric prefix mismatch")
        tokens = tokens[1:]
    if len(tokens) > MAX_FIELDS:
        raise EvidenceParseError("metric field limit exceeded")
    values: dict[str, MetricValue] = {}
    for token in tokens:
        key, separator, raw = token.partition("=")
        if not separator or not raw or key not in schema or key in values:
            raise EvidenceParseError("malformed, unknown, or duplicate metric field")
        values[key] = _metric_value(raw, schema[key])
    if values.keys() != schema.keys():
        raise EvidenceParseError("required metric field missing")
    assert len(values) == len(schema)
    assert len(values) <= MAX_FIELDS
    return values


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceParseError("duplicate JSON object key")
        result[key] = value
    assert len(result) <= len(pairs)
    assert len(result) == len(pairs)
    return result


def _reject_constant(value: str) -> object:
    raise EvidenceParseError("nonfinite JSON constant")


def decode_json_records(
    text: str, *, max_bytes: int = MAX_TEXT_BYTES, max_records: int = MAX_RECORDS
) -> tuple[dict[str, object], ...]:
    """Decode JSONL objects with a fixed nesting cap of 64 and strict numbers.

    Records are framed by LF only; CRLF is accepted as JSON whitespace. Empty
    input returns no records and one final LF is optional; blank rows fail.
    Deep or malformed JSON becomes a typed parse error. `1e999` also fails,
    including inside nested arrays/objects.
    """
    _bounded_text(text, max_bytes)
    if not (0 < max_records <= MAX_RECORDS):
        raise ValueError("record limit must be 1..10000")
    records: list[dict[str, object]] = []
    framed = text.removesuffix("\n")
    lines = framed.split("\n") if text else []
    for line in lines:
        if len(records) == max_records:
            raise EvidenceParseError("JSON record limit exceeded")
        _check_json_depth(line)
        try:
            record = json.loads(
                line, object_pairs_hook=_unique_object, parse_constant=_reject_constant, parse_float=_finite_float
            )
        except (ValueError, RecursionError) as exc:
            raise EvidenceParseError("malformed JSON evidence record") from exc
        if not isinstance(record, dict):
            raise EvidenceParseError("JSON evidence record must be an object")
        records.append(record)
    assert len(records) <= max_records
    assert all(isinstance(record, dict) for record in records)
    return tuple(records)


def _finite_float(raw: str) -> float:
    result = float(raw)
    if not math.isfinite(result):
        raise EvidenceParseError("nonfinite JSON number")
    assert math.isfinite(result)
    assert isinstance(result, float)
    return result


def _check_json_depth(line: str) -> None:
    depth = 0
    in_string = escaped = False
    for character in line:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
        elif character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > MAX_JSON_DEPTH:
                raise EvidenceParseError("JSON nesting limit exceeded")
        elif character in "]}":
            depth -= 1
            if depth < 0:
                raise EvidenceParseError("unbalanced JSON evidence")
    assert depth <= MAX_JSON_DEPTH
    assert isinstance(line, str)
