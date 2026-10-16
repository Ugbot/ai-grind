"""Bounded cold schema compiler: immutable in-memory XML, no filesystem access."""

from __future__ import annotations

import re
from typing import Any, Literal

import anyio

from devtools_mcp.sbe import CompilerBudgets, SchemaError, compile_schema, generate_cpp, generate_encoder_cpp
from devtools_mcp.server import mcp

_MAX_XML_BYTES = 256 * 1024
_MAX_HEADER_BYTES = 1024 * 1024
_COMPILE_LIMITER = anyio.CapacityLimiter(2)


def _compile(
    schema_xml: str,
    expect_sha256: str,
    namespace: str,
    max_group_entries: int,
    max_data_bytes: int,
    emit: Literal["decoder", "encoder"] = "decoder",
) -> dict[str, Any]:
    if emit not in ("decoder", "encoder"):
        return {"status": "error", "code": "invalid_schema", "detail": "emit must be decoder or encoder"}
    if len(schema_xml) > _MAX_XML_BYTES or len(namespace) > 128:
        return {"status": "error", "code": "limit_exceeded", "detail": "XML/namespace exceeds callable cap"}
    if not re.fullmatch(r"[0-9a-f]{64}", expect_sha256):
        return {
            "status": "error",
            "code": "invalid_schema",
            "detail": "expected SHA-256 must be 64 lowercase hex digits",
        }
    if not 0 < max_group_entries <= 65535 or not 0 < max_data_bytes <= 1024 * 1024:
        return {"status": "error", "code": "limit_exceeded", "detail": "wire budgets exceed callable caps"}
    try:
        xml = schema_xml.encode("utf-8")
        budgets = CompilerBudgets(
            file_bytes=_MAX_XML_BYTES,
            xml_nodes=8192,
            types=256,
            messages=64,
            groups=256,
            fields=2048,
            expanded_nodes=4096,
            header_bytes=_MAX_HEADER_BYTES,
            group_entries=max_group_entries,
            data_bytes=max_data_bytes,
        )
        ir = compile_schema(xml, budgets=budgets, expect_sha256=expect_sha256)
        header = (generate_encoder_cpp if emit == "encoder" else generate_cpp)(ir, namespace=namespace)
    except (SchemaError, UnicodeError) as error:
        return {"status": "error", "code": "invalid_schema", "detail": str(error)[:512]}
    result = {
        "status": "ok",
        "schema_id": ir.schema_id,
        "version": ir.version,
        "sha256": ir.sha256,
        "message_count": len(ir.messages),
        "header": header,
    }
    if emit == "encoder":
        result["emit"] = "encoder"
    return result


@mcp.tool()
async def devtools_sbe_compile(
    schema_xml: str,
    expect_sha256: str,
    namespace: str = "sbe_generated",
    max_group_entries: int = 65535,
    max_data_bytes: int = 1024 * 1024,
    emit: Literal["decoder", "encoder"] = "decoder",
) -> dict[str, Any]:
    """Compile hash-pinned FIX SBE XML into borrowed views or native encoder inputs.

    emit=decoder preserves the original borrowed-view header and receipt.
    emit=encoder returns a standalone namespace::encoder header with preflighted
    caller-owned inputs, measure and encode; no runtime XML or allocation.

    Cold authoring only. No paths, URLs, includes, writes, subprocesses or code
    execution. XML cap 256 KiB, output cap 1 MiB, 64 messages/2048 members and
    two concurrent compilations. Returns typed status/error or header/provenance.
    This does not register schemas, ingest market data, or qualify CME support.
    """
    return await anyio.to_thread.run_sync(
        _compile,
        schema_xml,
        expect_sha256,
        namespace,
        max_group_entries,
        max_data_bytes,
        emit,
        limiter=_COMPILE_LIMITER,
    )
