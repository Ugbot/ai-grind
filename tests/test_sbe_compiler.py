"""Independent schema layout, hostile XML and deterministic generation tests."""

import dataclasses
import hashlib
import struct
import subprocess
import sys
from pathlib import Path

import pytest

from devtools_mcp.sbe import CompilerBudgets, SchemaError, compile_schema, generate_cpp

FIXTURES = Path(__file__).parent / "fixtures" / "sbe"
FEATURES = (FIXTURES / "features.xml").read_bytes()
NESTED = (FIXTURES / "nested.xml").read_bytes()


def change(old, new, source=FEATURES):
    assert old in source
    return source.replace(old, new, 1)


def test_independently_specified_offsets_and_binary_values():
    ir = compile_schema(FEATURES, expect_sha256=hashlib.sha256(FEATURES).hexdigest())
    msg = ir.messages[0]
    assert (ir.schema_id, ir.version, msg.block_length) == (701, 2, 40)
    assert [(f.name, f.offset, f.size) for f in msg.fields] == [
        ("id", 0, 2),
        ("numbers", 2, 6),
        ("price", 8, 8),
        ("side", 16, 1),
        ("flags", 17, 1),
        ("symbol", 18, 3),
        ("real", 24, 4),
        ("added", 32, 8),
        ("constant", 40, 0),
    ]
    # Fixed external byte oracle, not bytes from our encoder (there is none).
    body = bytes.fromhex("2a00010002000300 15cd5b0700000000 0205 414200 000000 0000c07f 00000000 00000000000004c0")
    assert len(body) == 40
    assert struct.unpack_from("<H", body, msg.fields[0].offset)[0] == 42
    assert struct.unpack_from("<3H", body, msg.fields[1].offset) == (1, 2, 3)
    assert struct.unpack_from("<q", body, msg.fields[2].offset)[0] == 123456789
    assert struct.unpack_from("<d", body, msg.fields[7].offset)[0] == -2.5
    assert msg.fields[6].encoding.null == "NaN"
    assert msg.fields[8].constant == "-7"
    assert msg.fields[7].deprecated == 2
    assert ir.messages[1].fields[2].constant == "2"
    assert ir.messages[2].fields[0].encoding.members[0].encoding.name == "Price"
    with pytest.raises(dataclasses.FrozenInstanceError):
        ir.version = 99


def test_nested_groups_and_packed_variable_data():
    ir = compile_schema(NESTED)
    root = ir.messages[0]
    group, data = root.tails
    assert root.block_length == 4 and group.block_length == 2
    assert group.dimension == ("uint16", "uint16")
    assert group.tails[0].block_length == 4
    assert group.tails[1].length_primitive == "uint8"
    assert data.length_primitive == "uint16"
    output = generate_cpp(ir)
    assert "message_Nested_rows_samples_group" in output
    assert "read_data<order, std::uint8_t" in output
    assert "read_data<order, std::uint16_t" in output
    assert output.index("message_Nested_rows_group(reader") < output.index("visitor.on_message_Nested_name(value)")


def test_deterministic_generation_provenance_and_typed_contract():
    ir = compile_schema(FEATURES)
    output = generate_cpp(ir, namespace="owned::fixture")
    assert output == generate_cpp(compile_schema(FEATURES), namespace="owned::fixture")
    assert ir.sha256 in output
    assert "decode_Features" in output and "decode_Other" in output
    assert "get_price_mantissa" in output and "get_price_exponent" in output
    assert "has_flags_Indicative" in output and "enum class message_Features_side_enum" in output
    assert "std::numeric_limits<float>::quiet_NaN()" in output
    assert "get_numbers(std::uint32_t index, std::uint16_t& out)" in output
    assert "std::string" not in output and "std::vector" not in output and "XML" not in output.split("#pragma once")[1]
    assert "ByteOrder::Big" in generate_cpp(compile_schema(change(b"littleEndian", b"bigEndian")))
    with pytest.raises(SchemaError, match="namespace"):
        generate_cpp(ir, namespace="bad; code")
    with pytest.raises(SchemaError, match="SHA-256"):
        compile_schema(FEATURES, expect_sha256="0" * 64)


@pytest.mark.parametrize(
    ("old", "new", "diagnostic"),
    [
        (
            b'<type name="Unsigned16" primitiveType="uint16"/>',
            b'<type name="Unsigned16" primitiveType="uint128"/>',
            "primitive",
        ),
        (b'name="ThreeNumbers"', b'name="Unsigned16"', "duplicate"),
        (b'type="ThreeNumbers"', b'type="Unknown"', "undefined"),
        (b'offset="24"', b'offset="20"', "overlapping"),
        (b'blockLength="40"', b'blockLength="30"', "blockLength"),
        (b'sinceVersion="2" deprecated="2"', b'sinceVersion="3" deprecated="2"', "limit"),
        (b'presence="optional" nullValue="9223372036854775807"', b'nullValue="9223372036854775807"', "nullValue"),
        (b'nullValue="9223372036854775807"', b'nullValue="9223372036854775808"', "range"),
        (b'presence="constant">-7', b'presence="constant">', "literal"),
        (b'valueRef="Side.Sell"', b'valueRef="Side.Unknown"', "constant enum"),
        (b'<choice name="Indicative">2', b'<choice name="Indicative">8', "bit"),
        (b'<validValue name="Sell">2', b'<validValue name="Sell">1', "duplicate"),
        (b'presence="constant">-7', b'presence="constant" length="2">-7', "constant arrays"),
        (
            b'name="ThreeNumbers" primitiveType="uint16"',
            b'name="ThreeNumbers" primitiveType="uint16" presence="optional"',
            "optional arrays",
        ),
        (
            b'<field name="price" id="3" type="Price"/>',
            b'<field name="price" id="3" type="Price" presence="optional"/>',
            "optional composites",
        ),
        (b'<type name="Unsigned16"', b'<type align="8" name="Unsigned16"', "attribute"),
        (
            b'<field name="id" id="1" type="Unsigned16"/>',
            b'<field name="id" id="1" type="Unsigned16"><type name="X" primitiveType="uint8"/></field>',
            "child",
        ),
    ],
)
def test_invalid_or_unsupported_schema_is_explicit(old, new, diagnostic):
    with pytest.raises(SchemaError, match=diagnostic):
        compile_schema(change(old, new))


def test_cycle_nonpacked_header_dimensions_and_order_rejected():
    cyclic = change(b'<ref name="amount" type="Price"/>', b'<ref name="amount" type="PriceRef"/>')
    with pytest.raises(SchemaError, match="cyclic"):
        compile_schema(cyclic)
    with pytest.raises(SchemaError, match="header"):
        compile_schema(change(b'name="templateId" primitiveType="uint16"', b'name="templateId" primitiveType="uint32"'))
    nonpacked = change(
        b'name="numInGroup" primitiveType="uint16"', b'name="numInGroup" primitiveType="uint16" offset="4"', NESTED
    )
    with pytest.raises(SchemaError, match="dimensions"):
        compile_schema(nonpacked)
    disorder = change(
        b'<data name="name" id="9" type="largeData"/>',
        b'<data name="name" id="9" type="largeData"/><field name="bad" id="10" type="uint8"/>',
        NESTED,
    )
    with pytest.raises(SchemaError, match="wire order"):
        compile_schema(disorder)


@pytest.mark.parametrize(
    "prefix",
    [
        b'<!DOCTYPE messageSchema [<!ENTITY secret SYSTEM "file:///etc/passwd">]>',
        b'<!DOCTYPE messageSchema SYSTEM "https://example.invalid/evil.dtd">',
        b'<!DOCTYPE messageSchema [<!ENTITY x "expanded">]>',
    ],
)
def test_dtd_entities_are_forbidden(prefix):
    payload = FEATURES.replace(b'<?xml version="1.0" encoding="UTF-8"?>', prefix)
    with pytest.raises(SchemaError, match="DTD/entities"):
        compile_schema(payload)


def test_xinclude_is_never_loaded():
    payload = change(
        b"<types>", b'<types xmlns:xi="http://www.w3.org/2001/XInclude"><xi:include href="file:///etc/passwd"/>'
    )
    with pytest.raises(SchemaError, match="namespace"):
        compile_schema(payload)


@pytest.mark.parametrize(
    "budgets",
    [
        CompilerBudgets(file_bytes=10),
        CompilerBudgets(xml_nodes=5),
        CompilerBudgets(types=2),
        CompilerBudgets(messages=1),
        CompilerBudgets(fields=2),
        CompilerBudgets(array_elements=2),
        CompilerBudgets(block_bytes=10),
        CompilerBudgets(nesting=1),
    ],
)
def test_schema_resource_budgets(budgets):
    with pytest.raises(SchemaError):
        compile_schema(FEATURES, budgets=budgets)


def test_group_budget_and_codegen_budgets():
    with pytest.raises(SchemaError, match="group budget"):
        compile_schema(NESTED, budgets=CompilerBudgets(groups=1))
    for kwargs in ({"group_entries": 0}, {"nesting": 100}, {"message_bytes": 1 << 32}, {"block_bytes": 65536}):
        with pytest.raises(SchemaError):
            CompilerBudgets(**kwargs)
    output = generate_cpp(compile_schema(NESTED, budgets=CompilerBudgets(group_entries=3, data_bytes=7)))
    assert "std::uint16_t, 3, 8>" in output
    assert "std::uint8_t, 7>" in output


def test_cli_is_reusable_and_hash_mismatch_preserves_output(tmp_path):
    source, output = tmp_path / "schema.xml", tmp_path / "generated.h"
    source.write_bytes(FEATURES)
    args = [
        sys.executable,
        "-m",
        "devtools_mcp.sbe",
        "compile",
        str(source),
        "--output",
        str(output),
        "--expect-sha256",
    ]
    result = subprocess.run([*args, hashlib.sha256(FEATURES).hexdigest()], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    original = output.read_bytes()
    result = subprocess.run([*args, "0" * 64], capture_output=True, text=True)
    assert result.returncode == 1 and "SHA-256 mismatch" in result.stderr
    assert output.read_bytes() == original


@pytest.mark.parametrize("member", ["blockLength", "numInGroup"])
def test_array_dimensions_rejected(member):
    source = change(
        f'name="{member}" primitiveType="uint16"'.encode(),
        f'name="{member}" primitiveType="uint16" length="2"'.encode(),
        NESTED,
    )
    with pytest.raises(SchemaError, match="scalar"):
        compile_schema(source)


def test_array_header_with_same_total_size_rejected():
    source = change(
        b'name="blockLength" primitiveType="uint16"', b'name="blockLength" primitiveType="uint16" length="2"'
    )
    source = change(
        b'name="version" primitiveType="uint16"', b'name="version" primitiveType="uint16" length="0"', source
    )
    with pytest.raises(SchemaError, match="scalar"):
        compile_schema(source)


def test_resolved_composite_member_version_rejected():
    source = change(b"<types>", b'<types><type name="Later" primitiveType="int64" sinceVersion="1"/>')
    source = change(b'<type name="mantissa" primitiveType="int64"/>', b'<ref name="mantissa" type="Later"/>', source)
    with pytest.raises(SchemaError, match="versioned composite"):
        compile_schema(source)


@pytest.mark.parametrize("attribute", ["id", "version"])
def test_oversized_decimal_metadata_returns_schema_error(attribute):
    source = change(f'{attribute}="{701 if attribute == "id" else 2}"'.encode(), f'{attribute}="{"9" * 5000}"'.encode())
    with pytest.raises(SchemaError, match="limit"):
        compile_schema(source)


def test_oversized_set_choice_returns_schema_error():
    with pytest.raises(SchemaError, match="bit"):
        compile_schema(change(b'<choice name="Indicative">2', b'<choice name="Indicative">' + b"9" * 5000))


@pytest.mark.asyncio
async def test_discoverable_mcp_compile_and_failure_contract():
    from devtools_mcp.server import mcp

    assert "devtools_sbe_compile" in {tool.name for tool in await mcp.list_tools()}
    arguments = {"schema_xml": FEATURES.decode(), "expect_sha256": hashlib.sha256(FEATURES).hexdigest()}
    _, result = await mcp.call_tool("devtools_sbe_compile", arguments)
    assert result["status"] == "ok" and result["schema_id"] == 701
    assert "decode_Features" in result["header"] and result["sha256"] == arguments["expect_sha256"]
    _, failure = await mcp.call_tool("devtools_sbe_compile", {**arguments, "expect_sha256": "0" * 64})
    assert failure["status"] == "error" and "header" not in failure
    assert "SHA-256 mismatch" in failure["detail"]
    _, failure = await mcp.call_tool("devtools_sbe_compile", {**arguments, "schema_xml": "x" * (256 * 1024 + 1)})
    assert failure["code"] == "limit_exceeded" and "header" not in failure
    _, failure = await mcp.call_tool("devtools_sbe_compile", {**arguments, "max_group_entries": 65536})
    assert failure["code"] == "limit_exceeded"


def test_integer_metadata_range_comparison_is_exact():
    source = change(
        b'<type name="Unsigned16" primitiveType="uint16"/>',
        b'<type name="Unsigned16" primitiveType="uint64" minValue="9007199254740993" maxValue="9007199254740992"/>',
    )
    with pytest.raises(SchemaError, match="minValue exceeds maxValue"):
        compile_schema(source)


def reused_constant_schema(width=8, depth=8):
    types = ['<type name="Zero" primitiveType="uint8" presence="constant">0</type>']
    for level in range(depth):
        child = "Zero" if level == 0 else f"Constant{level - 1}"
        members = "".join(f'<ref name="member{i}" type="{child}"/>' for i in range(width))
        types.append(f'<composite name="Constant{level}">{members}</composite>')
    return change(b"<types>", ("<types>" + "".join(types)).encode()).replace(
        b'<field name="price" id="3" type="Price"/>',
        f'<field name="price" id="3" type="Constant{depth - 1}"/>'.encode(),
    )


def test_reused_zero_byte_dag_refuses_before_expansion():
    source = reused_constant_schema()
    assert len(source) < 8192  # Tiny source; expanded leaves would be 8**8.
    with pytest.raises(SchemaError, match="expanded traversal budget"):
        compile_schema(source)


def test_output_budget_enforced_during_emission():
    ir = compile_schema(FEATURES, budgets=CompilerBudgets(header_bytes=512))
    with pytest.raises(SchemaError, match="header output budget"):
        generate_cpp(ir)
    with pytest.raises(SchemaError, match="expanded (traversal|emission)"):
        generate_cpp(dataclasses.replace(compile_schema(FEATURES), budgets=CompilerBudgets(expanded_nodes=2)))


@pytest.mark.asyncio
async def test_mcp_reused_dag_and_output_budget_refusal(monkeypatch):
    from devtools_mcp.server import mcp
    from devtools_mcp.tools import sbe_tools

    source = reused_constant_schema()
    _, refusal = await mcp.call_tool(
        "devtools_sbe_compile", {"schema_xml": source.decode(), "expect_sha256": hashlib.sha256(source).hexdigest()}
    )
    assert refusal["status"] == "error" and "expanded traversal budget" in refusal["detail"]
    assert "header" not in refusal
    monkeypatch.setattr(sbe_tools, "_MAX_HEADER_BYTES", 512)
    _, refusal = await mcp.call_tool(
        "devtools_sbe_compile", {"schema_xml": FEATURES.decode(), "expect_sha256": hashlib.sha256(FEATURES).hexdigest()}
    )
    assert refusal["status"] == "error" and "header output budget" in refusal["detail"]
    assert "header" not in refusal


def test_exact_output_budget_boundary_and_shared_small_dag():
    ir = compile_schema(FEATURES)
    output = generate_cpp(ir)
    exact = dataclasses.replace(ir, budgets=CompilerBudgets(header_bytes=len(output.encode())))
    assert generate_cpp(exact) == output
    with pytest.raises(SchemaError, match="header output budget"):
        generate_cpp(dataclasses.replace(ir, budgets=CompilerBudgets(header_bytes=len(output.encode()) - 1)))
    small = compile_schema(reused_constant_schema(width=2, depth=2))
    assert "get_price_member0_member0" in generate_cpp(small)
