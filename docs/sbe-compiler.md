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
and array fields, versioned composite wire members, custom headers,
constrained common-header scalars and constrained variable-data payload elements
are examples. Declared variable-data **length** bounds are supported. Deprecation
metadata does not remove fields from the wire; fields absent in the selected
version are ignored. Larger generated functions or output exceed explicit
budgets and are refused.

The byte-oracle tests use owned synthetic XML. This is generic encoder/compiler
support, not CME qualification. Exact schema provenance and venue-specific conformance evidence remain separate
requirements; successful cold generation alone does not establish them.

The official legacy namespace `http://www.fixprotocol.org/ns/simple/1.0` and
root `xsi:schemaLocation` metadata are allowlisted; schema locations are never
fetched. Field/group/enum-value versions inherit their enclosing version when
omitted. Enum primitive aliases may be nullable, with explicit optional inputs.
Group dimensions retain both scalar offsets and their full composite size,
including padded headers such as uint16 block length at offset 0 and uint8
count at offset 7. The writer zeroes dimension padding; the reader uses declared
offsets. Large fixed-field and tail writers are split into bounded helpers.

The pinned CME production-path XML acquired on 2026-10-16 declares schema 1,
version 13, description 20230411 (SHA-256
`f45ebadf65ccb1a2d416f00ee755480072ecff030970cc170a78540541d78d60`).
Both cold code-generation paths can emit its 31 templates. That evidence does
not establish current live-channel applicability, generated C++ conformance,
actual listed security definitions or full CME venue qualification.

## Exact template selection

Both generation APIs accept optional `template_ids=[47, 48]` (an exact list or
tuple of exact integers). CLI selection repeats `--template-id 47 --template-id
48`; MCP uses `"template_ids": [47, 48]` with either `emit` mode. Omission keeps
the original full-header output and receipt. A provided selection adds
`template_ids` to the receipt and makes `message_count` the emitted count.
Output and receipt IDs follow original schema order, independent of request
order. The XML SHA-256, schema ID and version remain those of the complete
unmodified source. The entire XML is still parsed/validated before selection;
invalid unselected definitions do not disappear from compiler diagnostics.

Empty selections, duplicates, unknown IDs, bools, noninteger IDs, IDs outside
uint16 and selections exceeding the compiler message budget are refused. The
MCP budget admits at most 64 selected IDs and uses strict integers without
coercing strings/floats/bools. Generation errors publish no header; CLI refusal
leaves an existing output file unchanged. Selection does not rewrite XML, fetch
files, register runtime schemas or qualify a venue adapter.
