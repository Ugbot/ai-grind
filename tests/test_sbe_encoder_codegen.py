"""Independent literal-wire oracles for maintained native encoder generation."""

import dataclasses
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from devtools_mcp.sbe import CompilerBudgets, SchemaError, compile_schema, generate_cpp, generate_encoder_cpp

FIXTURES = Path(__file__).parent / "fixtures" / "sbe"


def native(tmp_path, fixture, body, *, big=False, xml=None):
    compiler = shutil.which("clang++")
    if not compiler:
        pytest.skip("native encoder oracle requires clang++")
    include = Path(os.environ.get("BOLT_INCLUDE_DIR", "/Users/bengamble/Gestalt2/extern/bolt/include"))
    if not (include / "bolt/wire/sbe_writer.h").exists():
        pytest.skip("set BOLT_INCLUDE_DIR to owned Bolt with sbe_writer.h")
    xml = (FIXTURES / fixture).read_bytes() if xml is None else xml
    if big:
        xml = xml.replace(b"littleEndian", b"bigEndian")
    ir = compile_schema(xml, budgets=CompilerBudgets(group_entries=3, data_bytes=7))
    decoder = generate_cpp(ir, namespace="fixture")
    (tmp_path / "decoder.h").write_text(decoder)
    (tmp_path / "encoder.h").write_text(generate_encoder_cpp(ir, namespace="fixture"))
    source = (
        '#include "decoder.h"\n#include "encoder.h"\n#include <cassert>\n#include <cstring>\n'
        "using namespace fixture::encoder;\nint main() {\n" + body + "\n}\n"
    )
    (tmp_path / "main.cpp").write_text(source)
    binary = tmp_path / "oracle"
    result = subprocess.run(
        [
            compiler,
            "-std=c++20",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-I",
            str(include),
            str(tmp_path / "main.cpp"),
            "-o",
            str(binary),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    subprocess.run([str(binary)], check=True)


def test_fixed_literal_oracle_null_constants_version_and_atomicity(tmp_path):
    native(
        tmp_path,
        "features.xml",
        r"""
    message_Features_input input{};
    input.value_id=42; input.value_numbers[0]=1; input.value_numbers[1]=2; input.value_numbers[2]=3;
    input.value_price_mantissa=123456789; input.value_side=2; input.value_flags=5;
    input.value_symbol[0]='A'; input.value_symbol[1]='B'; input.value_added=-2.5;
    std::uint8_t output[96]; std::memset(output,0xa5,sizeof(output));
    std::uint32_t measured=0, written=999;
    assert(measure_Features(input,2,measured)==Status::Ok && measured==48);
    assert(encode_Features(input,2,output,sizeof(output),written)==Status::Ok && written==48);
    const std::uint8_t oracle[]={40,0,8,0,0xbd,2,2,0,42,0,1,0,2,0,3,0,
        0x15,0xcd,0x5b,7,0,0,0,0,2,5,65,66,0,0,0,0,0,0,0xc0,0x7f,0,0,0,0,0,0,0,0,0,0,4,0xc0};
    static_assert(sizeof(oracle)==48);
    assert(std::memcmp(output,oracle,48)==0 && output[48]==0xa5);
    std::memset(output,0xa5,sizeof(output)); written=999;
    assert(encode_Features(input,2,output,47,written)!=Status::Ok && written==999);
    for(auto byte:output) assert(byte==0xa5);
    for (auto version : {65538, -65534}) {
        assert(encode_Features(input,version,output,sizeof(output),written)==Status::UnsupportedVersion);
        assert(measure_Features(input,version,measured)==Status::UnsupportedVersion);
    }
    assert(encode_Features(input,true,output,sizeof(output),written)==Status::InvalidInput);
    assert(encode_Features(input,2.0,output,sizeof(output),written)==Status::InvalidInput);
    assert(measure_Features(input,false,measured)==Status::InvalidInput);
    assert(measure_Features(input,2.0f,measured)==Status::InvalidInput);
    input.value_id=65535;
    assert(encode_Features(input,2,output,sizeof(output),written)==Status::InvalidInput);
    input.value_id=42; input.value_price_mantissa=INT64_MIN;
    assert(encode_Features(input,2,output,sizeof(output),written)==Status::InvalidInput);
    input.value_price_mantissa=123456789;
    input.value_added=std::numeric_limits<double>::quiet_NaN();
    assert(encode_Features(input,2,output,sizeof(output),written)==Status::InvalidInput);
    input.value_added=-2.5;
    input.value_side=99;
    assert(encode_Features(input,2,output,sizeof(output),written)==Status::InvalidInput);
    for(auto byte:output) assert(byte==0xa5);
    input.value_side=3; // Introduced only in v2.
    assert(encode_Features(input,1,output,sizeof(output),written)==Status::InvalidInput);
    input.value_side=1; input.value_added=123; // v1 omits added entirely.
    assert(encode_Features(input,1,output,sizeof(output),written)==Status::Ok && written==36);
    assert(output[0]==28 && output[6]==1);
    message_Other_input other{}; other.value_code=17;
    assert(encode_Other(other,2,output,sizeof(output),written)==Status::Ok && written==20);
    for(unsigned i=8;i<15;++i) assert(output[i]==0xff);
    assert(output[15]==0x7f && output[16]==0xff && output[17]==0 && output[18]==17 && output[19]==0);
    assert(encode_Other(other,3,output,sizeof(output),written)==Status::UnsupportedVersion);
    assert(encode_Other(other,2,reinterpret_cast<std::uint8_t*>(&other),sizeof(other),written)==Status::InvalidInput);
    """,
    )


@pytest.mark.parametrize("big", [False, True])
def test_nested_literal_oracle_and_preflight_graph_alias(tmp_path, big):
    expected = (
        "0,4,0,7,2,188,0,2,0,42,1,5,0,2,0,1,0xff,0xfe,0,4,0,1,0x3f,0x80,0,0,1,88,0,2,65,66"
        if big
        else "4,0,7,0,188,2,2,0,42,0,1,5,2,0,1,0,0xfe,0xff,4,0,1,0,0,0,0x80,0x3f,1,88,2,0,65,66"
    )
    native(
        tmp_path,
        "nested.xml",
        r"""
    message_Nested_rows_samples_input samples[1]{}; samples[0].value_sample=1.0f;
    message_Nested_rows_input rows[1]{}; rows[0].value_value=-2; rows[0].tail_samples={samples,1};
    const std::uint8_t label[]={88}, name[]={65,66};
    rows[0].tail_label={label,1};
    message_Nested_input input{}; input.value_id=42; input.value_side=1; input.value_flags=5;
    input.tail_rows={rows,1}; input.tail_name={name,2};
    std::uint8_t output[128]; std::memset(output,0xa5,sizeof(output)); std::uint32_t n=99,m=0;
    assert(measure_Nested(input,2,m)==Status::Ok && m==32);
    assert(encode_Nested(input,2,output,sizeof(output),n)==Status::Ok && n==32);
    const std::uint8_t oracle[]={"""
        + expected
        + r"""};
    assert(std::memcmp(output,oracle,32)==0 && output[32]==0xa5);
    std::memset(output,0xa5,sizeof(output)); n=99;
    rows[0].tail_label={output+90,1};
    assert(encode_Nested(input,2,output,sizeof(output),n)==Status::InvalidInput && n==99);
    for(auto byte:output) assert(byte==0xa5);
    rows[0].tail_label={label,1}; rows[0].tail_samples={samples,4}; // Cap is 3, before dereference.
    assert(encode_Nested(input,2,output,sizeof(output),n)!=Status::Ok && n==99);
    for(auto byte:output) assert(byte==0xa5);
    rows[0].tail_samples={nullptr,1};
    assert(encode_Nested(input,2,output,sizeof(output),n)!=Status::Ok);
    rows[0].tail_samples={samples,1}; rows[0].tail_label={nullptr,1};
    assert(encode_Nested(input,2,output,sizeof(output),n)!=Status::Ok);
    """,
        big=big,
    )


def test_deterministic_additive_api_and_budgets():
    ir = compile_schema((FIXTURES / "nested.xml").read_bytes())
    before = generate_cpp(ir)
    result = generate_encoder_cpp(ir)
    assert result == generate_encoder_cpp(ir)
    assert before == generate_cpp(ir)
    assert "namespace sbe_generated::encoder" in result and ir.sha256 in result
    assert "std::vector" not in result and "std::string" not in result
    with pytest.raises(SchemaError, match="namespace"):
        generate_encoder_cpp(ir, namespace="bad;")
    with pytest.raises(SchemaError, match="budget"):
        generate_encoder_cpp(dataclasses.replace(ir, budgets=CompilerBudgets(header_bytes=200)))


def test_declared_bounds_optional_presence_and_failure_atomicity(tmp_path):
    xml = (FIXTURES / "features.xml").read_bytes()
    xml = xml.replace(
        b'name="Unsigned16" primitiveType="uint16"',
        b'name="Unsigned16" primitiveType="uint16" minValue="40" maxValue="50"',
    )
    xml = xml.replace(
        b'name="OptionalPrice" primitiveType="int64"',
        b'name="OptionalPrice" primitiveType="int64" minValue="-2" maxValue="2"',
    )
    ir = compile_schema(xml)
    assert ir.messages[0].fields[0].encoding.minimum == "40"
    assert ir.messages[0].fields[0].encoding.maximum == "50"
    native(
        tmp_path,
        "features.xml",
        r"""
    message_Features_input input{}; input.value_id=42; input.value_side=1;
    std::uint8_t output[64]; std::memset(output,0xa5,sizeof(output)); std::uint32_t n=999;
    input.value_id=39;
    assert(encode_Features(input,2,output,sizeof(output),n)==Status::InvalidInput && n==999);
    for(auto byte:output) assert(byte==0xa5);
    input.value_id=51;
    assert(encode_Features(input,2,output,sizeof(output),n)==Status::InvalidInput && n==999);
    for(auto byte:output) assert(byte==0xa5);
    message_Other_input other{}; other.value_code=42; // Optional price absent beyond its domain is legal.
    assert(encode_Other(other,2,output,sizeof(output),n)==Status::Ok);
    std::memset(output,0xa5,sizeof(output)); n=999; other.present_price=true;
    assert(encode_Other(other,2,output,sizeof(output),n)==Status::InvalidInput && n==999); // present null
    other.value_price=3;
    assert(encode_Other(other,2,output,sizeof(output),n)==Status::InvalidInput && n==999);
    other.value_price=-3;
    assert(encode_Other(other,2,output,sizeof(output),n)==Status::InvalidInput && n==999);
    for(auto byte:output) assert(byte==0xa5);
    other.value_price=2;
    assert(encode_Other(other,2,output,sizeof(output),n)==Status::Ok);
    """,
        xml=xml,
    )


def test_declared_prefix_bounds_retained_and_enforced(tmp_path):
    xml = (FIXTURES / "nested.xml").read_bytes()
    xml = xml.replace(
        b'name="numInGroup" primitiveType="uint16"',
        b'name="numInGroup" primitiveType="uint16" minValue="1" maxValue="2"',
    )
    xml = xml.replace(
        b'name="blockLength" primitiveType="uint16"/>\n      <type name="numInGroup"',
        b'name="blockLength" primitiveType="uint16" minValue="2" maxValue="4"/>\n      <type name="numInGroup"',
    )
    xml = xml.replace(b'primitiveType="uint8" maxValue="254"', b'primitiveType="uint8" minValue="1" maxValue="2"')
    ir = compile_schema(xml)
    group = ir.messages[0].tails[0]
    assert group.dimension_bounds == (("2", "4"), ("1", "2"))
    assert group.tails[1].length_minimum == "1" and group.tails[1].length_maximum == "2"
    native(
        tmp_path,
        "nested.xml",
        r"""
    message_Nested_input input{}; input.value_side=1;
    message_Nested_rows_input rows[3]{};
    std::uint8_t output[128]; std::memset(output,0xa5,sizeof(output)); std::uint32_t n=999;
    input.tail_rows={rows,0}; // Declared count minimum1.
    assert(encode_Nested(input,2,output,sizeof(output),n)==Status::InvalidInput);
    input.tail_rows={rows,3}; // Compiler cap3, schema maximum2.
    assert(encode_Nested(input,2,output,sizeof(output),n)==Status::InvalidInput);
    message_Nested_rows_samples_input samples[1]{}; rows[0].tail_samples={samples,1};
    input.tail_rows={rows,1}; const std::uint8_t label[]={65,66,67};
    rows[0].tail_label={label,0}; // Declared data length minimum1.
    assert(encode_Nested(input,2,output,sizeof(output),n)==Status::InvalidInput);
    rows[0].tail_label={label,3}; // Declared data length maximum2.
    assert(encode_Nested(input,2,output,sizeof(output),n)==Status::InvalidInput && n==999);
    for(auto byte:output) assert(byte==0xa5);
    rows[0].tail_label={label,2};
    assert(encode_Nested(input,2,output,sizeof(output),n)==Status::Ok);
    """,
        xml=xml,
    )


def test_constrained_header_and_invalid_bound_are_explicit():
    xml = (FIXTURES / "nested.xml").read_bytes()
    with pytest.raises(SchemaError, match="constrained message header"):
        compile_schema(
            xml.replace(
                b'name="schemaId" primitiveType="uint16"', b'name="schemaId" primitiveType="uint16" maxValue="699"'
            )
        )
    with pytest.raises(SchemaError, match="NaN"):
        compile_schema(
            (FIXTURES / "features.xml")
            .read_bytes()
            .replace(
                b'name="OptionalReal" primitiveType="float"',
                b'name="OptionalReal" primitiveType="float" minValue="NaN"',
            )
        )


def test_encoder_cli_is_additive(tmp_path):
    import hashlib
    import sys

    source = FIXTURES / "nested.xml"
    xml = source.read_bytes()
    output = tmp_path / "encoder.h"
    command = [
        sys.executable,
        "-m",
        "devtools_mcp.sbe",
        "compile",
        str(source),
        "--output",
        str(output),
        "--expect-sha256",
        hashlib.sha256(xml).hexdigest(),
    ]
    subprocess.run(command + ["--encoder"], check=True, capture_output=True, text=True)
    assert output.read_text() == generate_encoder_cpp(compile_schema(xml))
    subprocess.run(command, check=True, capture_output=True, text=True)
    assert output.read_text() == generate_cpp(compile_schema(xml))


@pytest.mark.asyncio
async def test_existing_mcp_compile_encoder_selection_and_refusals():
    import hashlib

    from devtools_mcp.server import mcp
    from devtools_mcp.tools.sbe_tools import _compile

    xml = (FIXTURES / "nested.xml").read_bytes()
    digest = hashlib.sha256(xml).hexdigest()
    arguments = {"schema_xml": xml.decode(), "expect_sha256": digest}
    tool = next(tool for tool in await mcp.list_tools() if tool.name == "devtools_sbe_compile")
    assert tool.inputSchema["properties"]["emit"]["enum"] == ["decoder", "encoder"]
    _, default = await mcp.call_tool("devtools_sbe_compile", arguments)
    assert default["status"] == "ok" and "emit" not in default
    assert "decode_Nested" in default["header"] and "encode_Nested" not in default["header"]
    assert _compile(xml.decode(), digest, "sbe_generated", 65535, 1024 * 1024) == default
    _, encoded = await mcp.call_tool("devtools_sbe_compile", {**arguments, "emit": "encoder"})
    assert encoded["status"] == "ok" and encoded["emit"] == "encoder"
    assert encoded["sha256"] == digest and encoded["schema_id"] == 700
    assert "namespace sbe_generated::encoder" in encoded["header"]
    assert "encode_Nested" in encoded["header"] and "decode_Nested" not in encoded["header"]
    _, mismatch = await mcp.call_tool(
        "devtools_sbe_compile", {**arguments, "emit": "encoder", "expect_sha256": "0" * 64}
    )
    assert mismatch["status"] == "error" and "header" not in mismatch
    _, budget = await mcp.call_tool(
        "devtools_sbe_compile", {**arguments, "emit": "encoder", "max_group_entries": 65536}
    )
    assert budget["code"] == "limit_exceeded" and "header" not in budget
    refused = _compile(xml.decode(), digest, "sbe_generated", 65535, 1024 * 1024, "both")
    assert refused["status"] == "error" and "header" not in refused
    with pytest.raises(Exception, match="decoder|encoder"):
        await mcp.call_tool("devtools_sbe_compile", {**arguments, "emit": "both"})
