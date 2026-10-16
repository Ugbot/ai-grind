"""Cold generation of caller-owned, preflighted native SBE encoders.

No XML or heap ownership enters generated code. Inputs must remain immutable
through measure/encode; concurrent mutation is a caller programming error.
"""

from .codegen import _Generator, cpp_literal, generate_cpp, leaves
from .ir import Layout, SchemaError, SchemaIR
from .parser import PRIMITIVES


class _Encoder(_Generator):
    def flat(self, layout):
        return list(leaves(layout.fields, self.work, self.ir.budgets.expanded_nodes))

    def layout(self, layout, owner, depth=0):
        for tail in layout.tails:
            if isinstance(tail, Layout):
                self.layout(tail, owner + "_" + tail.name, depth + 1)
        flat = self.flat(layout)
        for _, field in flat:
            if field.presence == "constant":
                convert = float if field.encoding.primitive in ("float", "double") else int
                value = convert(field.constant)
                if (field.encoding.minimum and not value >= convert(field.encoding.minimum)) or (
                    field.encoding.maximum and not value <= convert(field.encoding.maximum)
                ):
                    raise SchemaError("constant outside declared scalar bounds")
        self.claim(owner + "_input")
        self.emit(f"struct {owner}_input {{")
        for path, f in flat:
            if f.presence == "constant":
                continue
            cpp = PRIMITIVES[f.encoding.primitive][0]
            length = f.encoding.length
            initial = cpp_literal(f.encoding.null, f.encoding.primitive) if f.presence == "optional" else ""
            if f.presence == "optional":
                self.emit(f"    bool present_{path} = false;")
            self.emit(f"    {cpp} value_{path}" + (f"[{length}]{{}};" if length != 1 else f"{{{initial}}};"))
        for tail in layout.tails:
            child = owner + "_" + tail.name
            self.emit(
                f"    Span<{child}_input> tail_{tail.name}{{}};"
                if isinstance(tail, Layout)
                else f"    bolt::wire::ByteView tail_{tail.name}{{}};"
            )
        self.emit("};")
        self.emit(
            f"inline std::uint32_t {owner}_block(std::uint16_t version) noexcept {{",
            "    assert(version <= schema_version);",
            "    assert(schema_version <= UINT16_MAX);",
            "    std::uint32_t n = 0;",
        )
        for _, f in flat:
            if f.size:
                self.emit(f"    if (version >= {f.since}) n = std::max(n, std::uint32_t{{{f.offset + f.size}}});")
        self.emit(f"    if (version == schema_version) n = {layout.block_length};", "    return n;", "}")
        for path, f in flat:
            if f.presence != "constant":
                self.validator(owner, path, f)
        self.measure(layout, owner, flat)
        self.write(layout, owner, flat, depth)

    def validator(self, owner, path, f):
        enc = f.encoding
        cpp = PRIMITIVES[enc.primitive][0]
        self.emit(
            f"inline Status {owner}_check_{path}({cpp} value, std::uint16_t version, bool present) noexcept {{",
            "    assert(version <= schema_version);",
            "    assert(schema_version <= UINT16_MAX);",
            "    (void)value; (void)version; (void)present;",
        )
        if f.presence == "optional":
            self.emit("    if (!present) return Status::Ok;")
        if enc.kind == "type" and not (enc.primitive == "char" and enc.length != 1) or f.presence == "optional":
            null_test = "value != value" if enc.null == "NaN" else f"value == {cpp_literal(enc.null, enc.primitive)}"
            self.emit(f"    if ({null_test}) return Status::InvalidInput;")
        if enc.primitive in ("float", "double"):
            self.emit("    if (!std::isfinite(value)) return Status::InvalidInput;")
        if enc.minimum:
            self.emit(f"    if (!(value >= {cpp_literal(enc.minimum, enc.primitive)})) return Status::InvalidInput;")
        if enc.maximum:
            self.emit(f"    if (!(value <= {cpp_literal(enc.maximum, enc.primitive)})) return Status::InvalidInput;")
        if enc.kind == "enum":
            self.emit("    switch (value) {")
            for v in enc.values:
                self.emit(
                    f"    case {cpp_literal(v.literal, enc.primitive)}: return version >= {v.since} ? "
                    f"Status::Ok : Status::InvalidInput;"
                )
            self.emit("    default: return Status::InvalidInput;", "    }")
        elif enc.kind == "set":
            self.emit("    std::uint64_t mask = 0;")
            for v in enc.values:
                self.emit(f"    if (version >= {v.since}) mask |= std::uint64_t{{1}} << {v.literal};")
            self.emit(
                "    return (static_cast<std::uint64_t>(value) & ~mask) == 0 ? Status::Ok : Status::InvalidInput;"
            )
        else:
            self.emit("    return Status::Ok;")
        self.emit("}")

    def measure(self, layout, owner, flat):
        self.emit(
            f"inline Status {owner}_measure(const {owner}_input& input, Measure& m) noexcept {{",
            "    assert(m.version <= schema_version);",
            "    assert(m.total <= max_message_bytes);",
            "    if (overlap(&input, sizeof(input), m.output, m.capacity)) return Status::InvalidInput;",
            f"    if (!add(m, {owner}_block(m.version))) return Status::LimitExceeded;",
        )
        for path, f in flat:
            if f.presence == "constant":
                continue
            value = f"input.value_{path}"
            present = f"input.present_{path}" if f.presence == "optional" else "true"
            self.emit(f"    if (m.version >= {f.since}) {{")
            if f.encoding.length != 1:
                self.emit(
                    f"        for (std::uint32_t i = 0; i < {f.encoding.length}; ++i)",
                    f"            if ({owner}_check_{path}({value}[i], m.version, {present}) != Status::Ok) "
                    f"return Status::InvalidInput;",
                )
            else:
                self.emit(
                    f"        if ({owner}_check_{path}({value}, m.version, {present}) != Status::Ok) return "
                    f"Status::InvalidInput;"
                )
            self.emit("    }")
        for tail in layout.tails:
            child, value = owner + "_" + tail.name, "input.tail_" + tail.name
            self.emit(f"    if (m.version >= {tail.since}) {{")
            if isinstance(tail, Layout):
                lp, cp = tail.dimension
                max_count = min(self.ir.budgets.group_entries, (1 << (PRIMITIVES[cp][1] * 8)) - 1)
                max_block = (1 << (PRIMITIVES[lp][1] * 8)) - 1
                bounds = tail.dimension_bounds or (("", ""), ("", ""))
                for expression, primitive, (minimum, maximum) in zip(
                    (f"{child}_block(m.version)", f"{value}.count"), tail.dimension, bounds, strict=True
                ):
                    if minimum:
                        self.emit(
                            f"        if ({expression} < {cpp_literal(minimum, primitive)}) return "
                            f"Status::InvalidInput;"
                        )
                    if maximum:
                        self.emit(
                            f"        if ({expression} > {cpp_literal(maximum, primitive)}) return "
                            f"Status::InvalidInput;"
                        )
                self.emit(
                    f"        if ({value}.count > {max_count} || ({value}.count && !{value}.data) || "
                    f"{child}_block(m.version) > {max_block}) return Status::InvalidInput;",
                    f"        if (!add(m, {PRIMITIVES[lp][1] + PRIMITIVES[cp][1]})) return Status::LimitExceeded;",
                    f"        for (std::uint32_t i = 0; i < {value}.count; ++i) {{",
                    f"            const auto status = {child}_measure({value}.data[i], m);",
                    "            if (status != Status::Ok) return status;",
                    "        }",
                )
            else:
                max_bytes = min(self.ir.budgets.data_bytes, (1 << (PRIMITIVES[tail.length_primitive][1] * 8)) - 1)
                if tail.length_minimum:
                    self.emit(
                        f"        if ({value}.size < "
                        f"{cpp_literal(tail.length_minimum, tail.length_primitive)}) return Status::InvalidInput;"
                    )
                if tail.length_maximum:
                    self.emit(
                        f"        if ({value}.size > "
                        f"{cpp_literal(tail.length_maximum, tail.length_primitive)}) return Status::InvalidInput;"
                    )
                self.emit(
                    f"        if ({value}.size > {max_bytes} || ({value}.size && !{value}.data) || "
                    f"overlap({value}.data, {value}.size, m.output, m.capacity)) return Status::InvalidInput;",
                    f"        if (!add(m, {PRIMITIVES[tail.length_primitive][1]} + "
                    f"std::uint64_t{{{value}.size}})) return Status::LimitExceeded;",
                )
            self.emit("    }")
        self.emit("    return Status::Ok;", "}")

    def write(self, layout, owner, flat, depth):
        self.emit(
            f"inline Status {owner}_write(const {owner}_input& input, std::uint16_t version, sbe::Writer& "
            f"writer) noexcept {{",
            "    assert(version <= schema_version);",
            "    assert(writer.offset <= writer.size);",
            "    (void)input;",
            "    sbe::WriteBlock block{};",
            f"    auto status = sbe::open_write_block(writer, {owner}_block(version), version, block);",
            "    if (status != Status::Ok) return status;",
        )
        for path, f in flat:
            if f.presence == "constant":
                continue
            cpp = PRIMITIVES[f.encoding.primitive][0]
            value = f"input.value_{path}"
            self.emit(f"    if (version >= {f.since}) {{")
            if f.encoding.length != 1:
                self.emit(
                    f"        for (std::uint32_t i = 0; i < {f.encoding.length}; ++i) {{",
                    f"            status = sbe::write_field<order, {cpp}>(block, {f.offset} + i * "
                    f"{PRIMITIVES[f.encoding.primitive][1]}, {f.since}, {value}[i]);",
                    "            if (status != Status::Ok) return status;",
                    "        }",
                )
            else:
                call = (
                    f"sbe::write_optional<order, {cpp}>(block, {f.offset}, {f.since}, input.present_{path}, "
                    f"{value}, {cpp_literal(f.encoding.null, f.encoding.primitive)})"
                    if f.presence == "optional"
                    else f"sbe::write_field<order, {cpp}>(block, {f.offset}, {f.since}, {value})"
                )
                self.emit(f"        status = {call};", "        if (status != Status::Ok) return status;")
            self.emit("    }")
        for tail in layout.tails:
            child, value = owner + "_" + tail.name, "input.tail_" + tail.name
            self.emit(f"    if (version >= {tail.since}) {{")
            if isinstance(tail, Layout):
                lp, cp = (PRIMITIVES[p][0] for p in tail.dimension)
                self.emit(
                    f"        status = sbe::write_group_dimensions<order, {lp}, {cp}, "
                    f"{self.ir.budgets.group_entries}, {self.ir.budgets.nesting}>(writer, "
                    f"{child}_block(version), {value}.count, {depth + 1});",
                    "        if (status != Status::Ok) return status;",
                    f"        for (std::uint32_t i = 0; i < {value}.count; ++i) {{",
                    f"            status = {child}_write({value}.data[i], version, writer);",
                    "            if (status != Status::Ok) return status;",
                    "        }",
                )
            else:
                self.emit(
                    f"        status = sbe::write_data<order, {PRIMITIVES[tail.length_primitive][0]}, "
                    f"{self.ir.budgets.data_bytes}>(writer, {value});",
                    "        if (status != Status::Ok) return status;",
                )
            self.emit("    }")
        self.emit("    return Status::Ok;", "}")

    def public(self, message):
        owner = "message_" + message.name
        self.emit(
            "template <typename Version>",
            f"inline Status measure_{message.name}(const {owner}_input& input, Version supplied_version, "
            f"std::uint32_t& bytes) noexcept {{",
            "    assert(schema_id <= UINT16_MAX);",
            "    assert(schema_version <= UINT16_MAX);",
            "    std::uint16_t version = 0;",
            "    const auto version_status = version_value(supplied_version, version);",
            "    if (version_status != Status::Ok) return version_status;",
            f"    if (version < {message.since} || version > schema_version) return Status::UnsupportedVersion;",
            "    if (overlap(&input, sizeof(input), &bytes, sizeof(bytes))) return Status::InvalidInput;",
            "    Measure m{8, version, reinterpret_cast<std::uint8_t*>(&bytes), sizeof(bytes)};",
            f"    const auto status = {owner}_measure(input, m);",
            "    if (status != Status::Ok) return status;",
            "    bytes = static_cast<std::uint32_t>(m.total); return Status::Ok;",
            "}",
            "template <typename Version>",
            f"inline Status encode_{message.name}(const {owner}_input& input, Version supplied_version, "
            f"std::uint8_t* output, std::uint32_t capacity, std::uint32_t& written) noexcept {{",
            "    assert(schema_id <= UINT16_MAX);",
            "    assert(schema_version <= UINT16_MAX);",
            "    std::uint16_t version = 0;",
            "    const auto version_status = version_value(supplied_version, version);",
            "    if (version_status != Status::Ok) return version_status;",
            f"    if (version < {message.since} || version > schema_version) return Status::UnsupportedVersion;",
            "    if (!output || overlap(output, capacity, &written, sizeof(written))) return Status::InvalidInput;",
            "    Measure m{8, version, output, capacity};",
            f"    auto status = {owner}_measure(input, m);",
            "    if (status != Status::Ok) return status;",
            "    // Validate the result reference against the entire borrowed input graph too.",
            "    Measure alias{8, version, reinterpret_cast<std::uint8_t*>(&written), sizeof(written)};",
            f"    status = {owner}_measure(input, alias);",
            "    if (status != Status::Ok) return status;",
            "    if (m.total > capacity) return Status::LimitExceeded;",
            "    sbe::Writer writer{output, capacity, 0};",
            f"    const sbe::Header header{{static_cast<std::uint16_t>({owner}_block(version)), "
            f"{message.id}, schema_id, version}};",
            "    status = sbe::write_header<order>(writer, header);",
            "    if (status != Status::Ok) return status;",
            f"    status = {owner}_write(input, version, writer);",
            "    if (status != Status::Ok) return status;",
            "    assert(writer.offset == m.total);",
            "    written = writer.offset; return Status::Ok;",
            "}",
        )


def generate_encoder_cpp(ir: SchemaIR, *, namespace="sbe_generated") -> str:
    """Emit standalone encoder in ``namespace::encoder``; decoder output unchanged.

    Flattened composite fields retain their schema names joined with underscores.
    Optional scalars have an explicit present flag, false by default; absence
    writes the schema null sentinel and present sentinel collisions are refused.
    Declared scalar and group/data-prefix bounds are retained and validated.
    Fields/tails absent in the version are ignored. Deprecation is metadata,
    not removal from the wire. All input graphs must be immutable during calls.
    """
    generate_cpp(ir, namespace=namespace)  # Reuse strict IR/name/work validation.
    g = _Encoder(ir)
    g.emit(
        "// Generated native SBE encoder; synthetic fixtures do not qualify a venue schema.",
        f"// Exact XML SHA-256: {ir.sha256}",
        "#pragma once",
        '#include "bolt/wire/sbe_writer.h"',
        "#include <algorithm>",
        "#include <limits>",
        "#include <type_traits>",
        "#include <cmath>",
        f"namespace {namespace}::encoder {{",
        "namespace sbe = bolt::wire::sbe;",
        "using Status = sbe::Status;",
        f"inline constexpr std::uint16_t schema_id = {ir.schema_id}, schema_version = {ir.version};",
        f"inline constexpr std::uint64_t max_message_bytes = {ir.budgets.message_bytes};",
        f"inline constexpr auto order = sbe::ByteOrder::{'Little' if ir.byte_order == 'littleEndian' else 'Big'};",
        "template <typename Version>",
        "inline Status version_value(Version source, std::uint16_t& out) noexcept {",
        "    assert(schema_version <= UINT16_MAX); assert(sizeof(out) == 2);",
        "    if constexpr (!std::is_integral_v<Version> || std::is_same_v<Version, bool> || sizeof(Version) > 8) {",
        "        (void)source; (void)out; return Status::InvalidInput;",
        "    } else {",
        "        if constexpr (std::is_signed_v<Version>) if (source < 0) return Status::UnsupportedVersion;",
        "        if (static_cast<std::uint64_t>(source) > UINT16_MAX) return Status::UnsupportedVersion;",
        "        out = static_cast<std::uint16_t>(source); return Status::Ok;",
        "    }",
        "}",
        "template <typename T> struct Span { const T* data = nullptr; std::uint32_t count = 0; };",
        "inline bool overlap(const void* a, std::uint64_t na, const void* b, std::uint64_t nb) noexcept {",
        "    assert(na <= UINT64_MAX); assert(nb <= UINT64_MAX);",
        "    if (!na || !nb) return false;",
        "    if (!a || !b) return true;",
        "    const auto x = reinterpret_cast<std::uintptr_t>(a), y = reinterpret_cast<std::uintptr_t>(b);",
        "    return x <= y ? std::uint64_t{y-x} < na : std::uint64_t{x-y} < nb;",
        "}",
        "struct Measure { std::uint64_t total; std::uint16_t version; std::uint8_t* output; std::uint32_t capacity; };",
        "inline bool add(Measure& m, std::uint64_t n) noexcept {",
        "    assert(m.total <= max_message_bytes); assert(max_message_bytes <= UINT32_MAX);",
        "    if (n > max_message_bytes - m.total) return false;",
        "    m.total += n; return true;",
        "}",
    )
    if ir.budgets.message_bytes < 8:
        raise SchemaError("encoder message budget cannot contain SBE header")
    for message in ir.messages:
        g.layout(message, "message_" + message.name)
        g.public(message)
    g.emit(f"}} // namespace {namespace}::encoder", "")
    # Prevent unexpectedly huge inline functions; callers can split a schema.
    depth = count = 0
    for line in g.lines:
        if "noexcept {" in line and depth == 0:
            depth, count = 1, 0
        elif depth:
            count += 1
            depth += line.count("{") - line.count("}")
            if count > 68:
                raise SchemaError("generated encoder function exceeds 70-line limit; split schema blocks")
    return "\n".join(g.lines)
