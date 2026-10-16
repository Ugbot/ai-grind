"""Emit zero-allocation typed Bolt views and schema-ordered bounded traversal."""

import re

from .ir import Data, Field, Layout, SchemaError, SchemaIR
from .parser import PRIMITIVES, structural_properties


def cpp_literal(value, primitive):
    cpp = PRIMITIVES[primitive][0]
    if value == "NaN":
        return f"std::numeric_limits<{cpp}>::quiet_NaN()"
    if primitive in ("float", "double"):
        return f"static_cast<{cpp}>({value})"
    if value == "-9223372036854775808":
        return "std::numeric_limits<std::int64_t>::min()"
    suffix = "ULL" if primitive.startswith("u") else "LL"
    return f"static_cast<{cpp}>({value}{suffix})"


def leaves(fields, work, limit, prefix="", base=0, since=0):
    for f in fields:
        work[0] += 1
        if work[0] > limit:
            raise SchemaError("expanded emission budget exceeded")
        path = prefix + f.name
        if f.encoding.kind == "composite":
            if f.presence != "required":
                raise SchemaError("codegen requires required composites")
            yield from leaves(f.encoding.members, work, limit, path + "_", base + f.offset, max(since, f.since))
        else:
            yield (
                path,
                Field(
                    path, f.id, f.encoding, base + f.offset, max(since, f.since), f.deprecated, f.presence, f.constant
                ),
            )


class _Generator:
    def __init__(self, ir):
        self.ir, self.lines, self.symbols = ir, [], set()
        self.work = [0]
        self.properties = {}
        self.output_bytes = 0

    def emit(self, *lines):
        for line in lines:
            size = len(line.encode("utf-8")) + int(bool(self.lines))
            if self.output_bytes + size > self.ir.budgets.header_bytes:
                raise SchemaError("generated header output budget exceeded")
            self.output_bytes += size
            self.lines.append(line)

    def claim(self, symbol):
        if symbol in self.symbols:
            raise SchemaError(f"C++ generated symbol collision: {symbol}")
        self.symbols.add(symbol)

    def field(self, owner, path, f):
        enc, method = f.encoding, "get_" + path
        cpp = PRIMITIVES[enc.primitive][0]
        enum = owner + "_" + path + "_enum"
        if enc.kind == "enum":
            cpp = enum
        argument = f"{cpp}& out"
        if enc.length != 1:
            argument = "std::uint32_t index, " + argument
        deprecated = f.deprecated if f.deprecated is not None else "UINT32_MAX"
        self.emit(
            f"    static constexpr std::uint32_t {path}_offset = {f.offset};",
            f"    static constexpr std::uint16_t {path}_since = {f.since};",
            f"    static constexpr std::uint32_t {path}_deprecated = {deprecated};",
            f"    Status {method}({argument}) const noexcept {{",
            f"        assert({path}_offset <= UINT32_MAX);",
            f"        assert({path}_since <= schema_version);",
        )
        if f.presence == "constant":
            value = cpp_literal(f.constant, enc.primitive)
            if enc.kind == "enum":
                value = f"static_cast<{cpp}>({value})"
            self.emit(
                f"        if (block.acting_version < {f.since}) return Status::Absent;",
                f"        out = {value};",
                "        return Status::Ok;",
                "    }",
            )
            return
        offset = str(f.offset)
        if enc.length != 1:
            self.emit(f"        if (index >= {enc.length}) return Status::InvalidInput;")
            offset += f" + index * {PRIMITIVES[enc.primitive][1]}"
        if enc.kind == "enum":
            raw = PRIMITIVES[enc.primitive][0]
            self.emit(f"        {raw} value{{}};")
            target = "value"
        else:
            target = "out"
        reader = "read_optional" if f.presence == "optional" else "read_field"
        extra = f", {cpp_literal(enc.null, enc.primitive)}" if f.presence == "optional" else ""
        call = f"sbe::{reader}<order>(block, {offset}, {f.since}{extra}, {target})"
        if enc.kind != "enum":
            self.emit(f"        return {call};", "    }")
            return
        self.emit(
            f"        const auto status = {call};",
            "        if (status != Status::Ok) return status;",
            "        switch (value) {",
        )
        for value in enc.values:
            if len(enc.values) > 16:
                self.emit(
                    f"        case {cpp_literal(value.literal, enc.primitive)}: "
                    f"if (block.acting_version < {value.since}) return Status::InvalidInput; "
                    f"out = static_cast<{cpp}>(value); return Status::Ok;"
                )
            else:
                self.emit(
                    f"        case {cpp_literal(value.literal, enc.primitive)}:",
                    f"            if (block.acting_version < {value.since}) return Status::InvalidInput;",
                    f"            out = static_cast<{cpp}>(value); return Status::Ok;",
                )
        self.emit("        default: return Status::InvalidInput;", "        }", "    }")

    def view(self, layout, owner):
        self.claim(owner + "_view")
        limit = self.ir.budgets.expanded_nodes
        required = 0
        for f in layout.fields:
            _, _, expanded = structural_properties(f.encoding, self.properties, limit)
            if expanded > limit - self.work[0] - required:
                raise SchemaError("expanded emission budget exceeded")
            required += expanded
        flat = list(leaves(layout.fields, self.work, limit))
        seen = set()
        for path, f in flat:
            if path in seen:
                raise SchemaError("flattened composite getter collision")
            seen.add(path)
            if f.encoding.kind == "enum":
                symbol = owner + "_" + path + "_enum"
                self.claim(symbol)
                self.emit(f"enum class {symbol} : {PRIMITIVES[f.encoding.primitive][0]} {{")
                for v in f.encoding.values:
                    self.emit(f"    value_{v.name} = {cpp_literal(v.literal, f.encoding.primitive)},")
                self.emit("};")
        self.emit(f"struct {owner}_view {{", "    sbe::Block block{};")
        for path, f in flat:
            self.field(owner, path, f)
            if f.encoding.kind == "set":
                for v in f.encoding.values:
                    self.emit(
                        f"    Status has_{path}_{v.name}(bool& out) const noexcept {{",
                        "        assert(schema_id <= UINT16_MAX);",
                        "        assert(schema_version <= UINT16_MAX);",
                        f"        if (block.acting_version < {max(f.since, v.since)}) return Status::Absent;",
                        f"        {PRIMITIVES[f.encoding.primitive][0]} value{{}};",
                        f"        const auto status = get_{path}(value);",
                        "        if (status != Status::Ok) return status;",
                        f"        out = (value & (std::uint64_t{{1}} << {v.literal})) != 0;",
                        "        return Status::Ok;",
                        "    }",
                    )
        self.emit(
            "};",
            f"inline std::uint32_t {owner}_minimum_block(std::uint16_t version) noexcept {{",
            "    assert(version <= schema_version);",
            "    assert(schema_version <= UINT16_MAX);",
            "    std::uint32_t size = 0;",
        )
        for _, f in flat:
            if f.size:
                self.emit(f"    if (version >= {f.since}) size = std::max(size, std::uint32_t{{{f.offset + f.size}}});")
        self.emit(f"    if (version == schema_version) size = {layout.block_length};", "    return size;", "}")

    def padded_group(self, layout, owner, depth):
        length, count = (PRIMITIVES[p][0] for p in layout.dimension)
        lo, co = layout.dimension_offsets
        self.emit(
            f"inline Status {owner}_open(bolt::wire::ByteReader& reader, "
            f"std::uint16_t version, sbe::Group& out) noexcept {{",
            "    assert(bolt::wire::byte_reader_valid(reader));",
            "    assert(version <= schema_version);",
            f"    if ({depth} > {self.ir.budgets.nesting}) return Status::LimitExceeded;",
            "    auto next = reader; bolt::wire::ByteView dimensions{};",
            f"    if (!bolt::wire::read_bytes(next, {layout.dimension_size}, dimensions)) return Status::Truncated;",
            f"    bolt::wire::ByteReader lr{{dimensions.data, dimensions.size, {lo}}};",
            f"    bolt::wire::ByteReader cr{{dimensions.data, dimensions.size, {co}}};",
            f"    {length} length{{}}; {count} count{{}};",
            "    if (!sbe::read_scalar<order>(lr, length) || !sbe::read_scalar<order>(cr, count)) "
            "return Status::Truncated;",
        )
        if self.ir.budgets.group_entries < (1 << (PRIMITIVES[layout.dimension[1]][1] * 8)) - 1:
            self.emit(f"    if (count > {self.ir.budgets.group_entries}) return Status::LimitExceeded;")
        self.emit(
            f"    if (length < {owner}_minimum_block(version)) return Status::InvalidInput;",
            "    if (std::uint64_t{length} * count > next.size - next.offset) return Status::Truncated;",
            "    reader = next; out = {length, count, version}; return Status::Ok;",
            "}",
        )

    def walk(self, layout, owner, depth):
        self.view(layout, owner)
        for tail in layout.tails:
            if isinstance(tail, Layout):
                self.walk(tail, owner + "_" + tail.name, depth + 1)
        visitor_arg = " visitor" if layout.tails else ""
        self.emit(
            "template <typename Visitor>",
            f"inline Status {owner}_tail(bolt::wire::ByteReader& reader, "
            f"std::uint16_t version, Visitor&{visitor_arg}) noexcept {{",
            "    if (!bolt::wire::byte_reader_valid(reader)) return Status::InvalidInput;",
            "    if (version > schema_version) return Status::UnsupportedVersion;",
            "    assert(bolt::wire::byte_reader_valid(reader));",
            "    assert(version <= schema_version);",
        )
        # Keep emitted functions bounded in source size: each tail is delegated.
        for tail in layout.tails:
            child = owner + "_" + tail.name
            if isinstance(tail, Layout):
                self.emit(
                    f"    if (version >= {tail.since}) {{",
                    f"        const auto status = {child}_group(reader, version, visitor);",
                    "        if (status != Status::Ok) return status;",
                    "    }",
                )
            elif isinstance(tail, Data):
                self.emit(
                    f"    if (version >= {tail.since}) {{",
                    "        bolt::wire::ByteView value{};",
                    f"        const auto status = sbe::read_data<order, "
                    f"{PRIMITIVES[tail.length_primitive][0]}, {self.ir.budgets.data_bytes}>(reader, value);",
                    "        if (status != Status::Ok) return status;",
                    f"        const auto callback = visitor.on_{child}(value);",
                    "        if (callback != Status::Ok) return callback;",
                    "    }",
                )
            else:
                raise SchemaError("unsupported tail IR")
        self.emit("    return Status::Ok;", "}")
        if layout.dimension:
            length, count = (PRIMITIVES[p][0] for p in layout.dimension)
            padded = layout.dimension_offsets and (
                layout.dimension_offsets != (0, PRIMITIVES[layout.dimension[0]][1])
                or layout.dimension_size != sum(PRIMITIVES[p][1] for p in layout.dimension)
            )
            if padded:
                self.padded_group(layout, owner, depth)
            opening = (
                f"    auto status = {owner}_open(reader, version, group);"
                if padded
                else f"    auto status = sbe::open_group<order, {length}, {count}, "
                f"{self.ir.budgets.group_entries}, {self.ir.budgets.nesting}>"
                f"(reader, {owner}_minimum_block(version), version, {depth}, group);"
            )
            self.emit(
                "template <typename Visitor>",
                f"inline Status {owner}_group(bolt::wire::ByteReader& reader, "
                "std::uint16_t version, Visitor& visitor) noexcept {",
                "    assert(bolt::wire::byte_reader_valid(reader));",
                "    assert(version <= schema_version);",
                "    sbe::Group group{};",
                opening,
                "    if (status != Status::Ok) return status;",
                f"    for (std::uint32_t i = 0; i < {self.ir.budgets.group_entries} && group.remaining != 0; ++i) {{",
                "        sbe::Block block{};",
                "        status = sbe::next_entry(reader, group, block);",
                "        if (status != Status::Ok) return status;",
                f"        status = visitor.on_{owner}({owner}_view{{block}});",
                "        if (status != Status::Ok) return status;",
                f"        status = {owner}_tail(reader, version, visitor);",
                "        if (status != Status::Ok) return status;",
                "    }",
                "    return group.remaining == 0 ? Status::Ok : Status::LimitExceeded;",
                "}",
            )

    def message(self, message):
        owner = "message_" + message.name
        self.walk(message, owner, 0)
        self.claim("decode_" + message.name)
        self.emit(
            "template <typename Visitor>",
            f"inline Status decode_{message.name}(bolt::wire::ByteView bytes, Visitor& visitor) noexcept {{",
            "    assert(schema_id <= UINT16_MAX);",
            "    assert(schema_version <= UINT16_MAX);",
            f"    const sbe::Expected expected{{schema_id, {message.id}, 0, {message.since}, schema_version}};",
            "    sbe::Message message{};",
            f"    auto status = sbe::open_message<order, {self.ir.budgets.message_bytes}>(bytes, expected, message);",
            "    if (status != Status::Ok) return status;",
            f"    if (message.root.bytes.size < {owner}_minimum_block(message.header.version)) "
            "return Status::InvalidInput;",
            f"    status = visitor.on_{owner}({owner}_view{{message.root}});",
            "    if (status != Status::Ok) return status;",
            f"    status = {owner}_tail(message.tail, message.header.version, visitor);",
            "    if (status != Status::Ok) return status;",
            "    return sbe::finish_message(message.tail);",
            "}",
        )


def generate_cpp(ir: SchemaIR, *, namespace="sbe_generated") -> str:
    """Emit borrowed view getters; visitor callbacks return sbe::Status.

    Traversal enforces structure, enum domains are checked by typed getters.
    Caller callbacks may have effects before a later failure; stage externally
    when all-or-nothing application semantics are required.
    """
    if len(namespace) > 128:
        raise SchemaError("C++ namespace exceeds bounded length")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*(::[A-Za-z][A-Za-z0-9_]*)*", namespace):
        raise SchemaError("invalid C++ namespace")
    keywords = {
        "alignas",
        "alignof",
        "and",
        "and_eq",
        "asm",
        "auto",
        "bitand",
        "bitor",
        "bool",
        "break",
        "case",
        "catch",
        "char",
        "char8_t",
        "char16_t",
        "char32_t",
        "class",
        "compl",
        "concept",
        "const",
        "consteval",
        "constexpr",
        "constinit",
        "const_cast",
        "continue",
        "co_await",
        "co_return",
        "co_yield",
        "decltype",
        "default",
        "delete",
        "do",
        "double",
        "dynamic_cast",
        "else",
        "enum",
        "explicit",
        "export",
        "extern",
        "false",
        "float",
        "for",
        "friend",
        "goto",
        "if",
        "inline",
        "int",
        "long",
        "mutable",
        "namespace",
        "new",
        "noexcept",
        "not",
        "not_eq",
        "nullptr",
        "operator",
        "or",
        "or_eq",
        "private",
        "protected",
        "public",
        "register",
        "reinterpret_cast",
        "requires",
        "return",
        "short",
        "signed",
        "sizeof",
        "static",
        "static_assert",
        "static_cast",
        "struct",
        "switch",
        "template",
        "this",
        "thread_local",
        "throw",
        "true",
        "try",
        "typedef",
        "typeid",
        "typename",
        "union",
        "unsigned",
        "using",
        "virtual",
        "void",
        "volatile",
        "wchar_t",
        "while",
        "xor",
        "xor_eq",
    }
    if any(part in keywords or "__" in part for part in namespace.split("::")):
        raise SchemaError("reserved C++ namespace")
    if not re.fullmatch("[0-9a-f]{64}", ir.sha256):
        raise SchemaError("invalid IR provenance")
    generator = _Generator(ir)
    order = "Little" if ir.byte_order == "littleEndian" else "Big"
    generator.emit(
        "// Generated by devtools_mcp.sbe; DO NOT EDIT.",
        f"// Exact XML SHA-256: {ir.sha256}",
        "#pragma once",
        '#include "bolt/wire/sbe_fields.h"',
        '#include "bolt/wire/sbe_groups.h"',
        "#include <algorithm>",
        "#include <limits>",
        f"namespace {namespace} {{",
        "namespace sbe = bolt::wire::sbe;",
        "using Status = sbe::Status;",
        f'inline constexpr char schema_sha256[] = "{ir.sha256}";',
        f"inline constexpr std::uint16_t schema_id = {ir.schema_id};",
        f"inline constexpr std::uint16_t schema_version = {ir.version};",
        f"inline constexpr auto order = sbe::ByteOrder::{order};",
    )
    for message in ir.messages:
        generator.message(message)
    generator.emit(f"}} // namespace {namespace}", "")
    # Very large valid schemas must be split rather than emitting oversized hot
    # functions. Refuse honestly instead of silently omitting fields/callbacks.
    output = "\n".join(generator.lines)
    depth, count = 0, 0
    for line in generator.lines:
        if "noexcept {" in line and depth == 0:
            count = 0
            depth = 1
            continue
        if depth:
            count += 1
            depth += line.count("{") - line.count("}")
            if count > 68:
                raise SchemaError("generated function exceeds 70-line limit; split schema blocks")
    return output
