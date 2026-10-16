# Bounded native SBE compiler

`devtools_sbe_compile` accepts hash-pinned XML text in memory. Its default
`emit="decoder"` preserves the existing decoder header and receipt. Select
`emit="encoder"` for a standalone native C++ header in `<namespace>::encoder`:

```json
{
  "schema_xml": "<exact schema XML text>",
  "expect_sha256": "<SHA-256 of those exact UTF-8 bytes, 64 lowercase hex digits>",
  "namespace": "owned_feed",
  "emit": "encoder",
  "max_group_entries": 128,
  "max_data_bytes": 4096
}
```

The successful encoder receipt includes `emit`, schema ID/version, XML SHA-256,
message count and `header`. Invalid schemas, hashes and compiler budgets return
an error without a header. The MCP boundary rejects any other `emit` value.
This tool performs no filesystem access, network requests, subprocesses or code
execution. Caps remain 256 KiB XML, 1 MiB generated output, 64 messages,
2048 members and two concurrent compilations.

The maintained Python API is `generate_encoder_cpp(compile_schema(xml),
namespace="owned_feed")`; the CLI is `python -m devtools_mcp.sbe compile schema.xml
--output encoder.h --expect-sha256 HASH --encoder`. Omitting `--encoder` keeps
decoder generation. Both generated headers may coexist under the same supplied
namespace. Include the owned Bolt headers, including `bolt/wire/sbe_writer.h`.

For each message, the encoder declares `message_<Name>_input`,
`measure_<Name>(input, version, bytes)` and
`encode_<Name>(input, version, output, capacity, written)`. Inputs use fixed
scalar/array fields; composite names flatten with underscores. Groups borrow
`Span<entry_input>{pointer,count}` and variable data borrows `ByteView`.
Optional scalars use `present_<field>` (false by default) plus `value_<field>`;
absence writes the null sentinel, while a present null is rejected. Fixed
character arrays permit NUL padding. Version arguments are checked before
narrowing; booleans and floating versions are refused.

Measurement validates active fields and tails, primitive domains, enum/set
choices, declared scalar and group/data-prefix bounds, schema versions and
compiler budgets. Encoding also checks capacity and output aliases against the
entire active borrowed input graph before writing; on refusal, output bytes and
`written` remain unchanged. Caller-owned inputs and spans must be valid and
immutable throughout the call. Successful fixed blocks have deterministic zero
padding. No runtime XML, allocation or ownership enters the generated code.

The compiler supports its existing strict IR subset, including nested groups
and variable data. Unsupported constructs fail explicitly: optional composite
and array fields, versioned composite wire members, custom headers/dimensions,
constrained common-header scalars and constrained variable-data payload elements
are examples. Declared variable-data **length** bounds are supported. Deprecation
metadata does not remove fields from the wire; fields absent in the selected
version are ignored. Larger generated functions or output exceed explicit
budgets and are refused.

The byte-oracle tests use owned synthetic XML. This is generic encoder/compiler
support, not CME qualification. Current official CME XML acquisition, exact
schema provenance and venue-specific conformance evidence remain separate work.
