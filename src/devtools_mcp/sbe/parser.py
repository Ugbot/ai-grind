"""Strict subset compiler for FIX SBE v1: bounded local bytes, never includes."""

import hashlib
import math
import re

from lxml import etree

from .ir import CompilerBudgets, Data, Encoding, Field, Layout, SchemaError, SchemaIR, Value

PRIMITIVES = {
    "char": ("char", 1),
    **{f"{sign}int{bits}": (f"std::{sign}int{bits}_t", bits // 8) for sign in ("", "u") for bits in (8, 16, 32, 64)},
    "float": ("float", 4),
    "double": ("double", 8),
}
NAMESPACES = {"", "http://fixprotocol.io/2016/sbe", "http://fixprotocol.io/2017/sbe"}
DEFAULT_BUDGETS = CompilerBudgets()
META = {"description", "semanticType", "sinceVersion", "deprecated"}
ATTRS = {
    "messageSchema": {"package", "id", "version", "semanticVersion", "byteOrder", "headerType", "description"},
    "types": set(),
    "type": META
    | {
        "name",
        "primitiveType",
        "presence",
        "length",
        "offset",
        "nullValue",
        "minValue",
        "maxValue",
        "characterEncoding",
        "epoch",
        "timeUnit",
    },
    "composite": META | {"name", "offset"},
    "ref": META | {"name", "type", "offset"},
    "enum": META | {"name", "encodingType", "offset", "nullValue", "presence"},
    "set": META | {"name", "encodingType", "offset"},
    "validValue": META | {"name"},
    "choice": META | {"name"},
    "message": META | {"name", "id", "blockLength"},
    "group": META | {"name", "id", "blockLength", "dimensionType"},
    "field": META | {"name", "id", "type", "offset", "presence", "valueRef"},
    "data": META | {"name", "id", "type"},
}


def tag(node):
    q = etree.QName(node)
    if (q.namespace or "") not in NAMESPACES:
        raise SchemaError(f"unsupported XML namespace {q.namespace!r}; includes are forbidden")
    return q.localname


def number(node, key, default=None, maximum=65535):
    value = node.get(key, default)
    if value is None or not re.fullmatch(r"[0-9]+", str(value)):
        raise SchemaError(f"{tag(node)} requires unsigned {key}")
    digits = str(value).lstrip("0") or "0"
    if len(digits) > len(str(maximum)):
        raise SchemaError(f"{key} exceeds limit {maximum}")
    result = int(digits)
    if result > maximum:
        raise SchemaError(f"{key} exceeds limit {maximum}")
    return result


def name(node):
    result = node.get("name", "")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", result) or len(result) > 128:
        raise SchemaError(f"invalid bounded symbolic name {result!r}")
    return result


def literal(text, primitive):
    text = text.strip()
    if primitive == "char":
        if len(text) != 1 or ord(text) > 127:
            raise SchemaError("char scalar requires one ASCII character")
        return str(ord(text))
    if primitive in ("float", "double"):
        try:
            result = float(text)
        except ValueError as error:
            raise SchemaError("invalid floating literal") from error
        limit = 3.4028234663852886e38 if primitive == "float" else 1.7976931348623157e308
        if math.isinf(result) or (math.isfinite(result) and abs(result) > limit):
            raise SchemaError("floating literal out of range")
        return "NaN" if math.isnan(result) else repr(result)
    try:
        result = int(text, 10)
    except ValueError as error:
        raise SchemaError("invalid integer literal") from error
    bits = PRIMITIVES[primitive][1] * 8
    low, high = (0, (1 << bits) - 1) if primitive.startswith("u") else (-(1 << (bits - 1)), (1 << (bits - 1)) - 1)
    if not low <= result <= high:
        raise SchemaError("integer literal out of range")
    return str(result)


def default_null(primitive):
    if primitive in ("float", "double"):
        return "NaN"
    if primitive == "char":
        return "0"
    bits = PRIMITIVES[primitive][1] * 8
    return str((1 << bits) - 1 if primitive.startswith("u") else -(1 << (bits - 1)))


class _Compiler:
    def __init__(self, root, budgets):
        self.root, self.budgets = root, budgets
        self.version = number(root, "version", 0)
        self.nodes: dict[str, etree._Element] = {}
        self.resolved: dict[str, Encoding] = {}
        self.active: set[str] = set()
        self.properties: dict[int, tuple[int, bool, int]] = {}
        self.expanded_count = 0
        self.field_count = 0
        self.group_count = 0
        for types in root:
            if tag(types) == "types":
                for node in types:
                    key = name(node)
                    if key in self.nodes or key in PRIMITIVES:
                        raise SchemaError("duplicate or primitive-shadowing type")
                    self.nodes[key] = node
        if not self.nodes or len(self.nodes) > budgets.types:
            raise SchemaError("type budget exceeded or missing types")

    def versions(self, node):
        since = number(node, "sinceVersion", 0, self.version)
        deprecated = number(node, "deprecated", maximum=self.version) if node.get("deprecated") else None
        if deprecated is not None and deprecated < since:
            raise SchemaError("deprecated precedes sinceVersion")
        return since, deprecated

    def resolve(self, key, depth=0):
        if key in self.resolved:
            return self.resolved[key]
        if key in PRIMITIVES:
            return Encoding(key, "type", key, size=PRIMITIVES[key][1], null=default_null(key))
        if depth > self.budgets.nesting or key in self.active:
            raise SchemaError("type nesting budget exceeded or cyclic reference")
        if key not in self.nodes:
            raise SchemaError(f"undefined encoding {key!r}")
        self.active.add(key)
        result = self.encoding(self.nodes[key], depth)
        self.active.remove(key)
        self.resolved[key] = result
        return result

    def encoding(self, node, depth):
        kind, key = tag(node), name(node)
        since, deprecated = self.versions(node)
        presence = node.get("presence", "required")
        if presence not in ("required", "optional", "constant"):
            raise SchemaError("invalid presence")
        common = dict(
            name=key,
            kind=kind,
            since=since,
            deprecated=deprecated,
            presence=presence,
            semantic=node.get("semanticType", ""),
        )
        if kind == "type":
            primitive = node.get("primitiveType", "")
            if primitive not in PRIMITIVES or len(node):
                raise SchemaError("unsupported primitive or child of simple type")
            length = number(node, "length", 1, self.budgets.array_elements)
            if presence == "optional" and length != 1:
                raise SchemaError("optional arrays unsupported")
            if node.get("nullValue") is not None and presence != "optional":
                raise SchemaError("nullValue requires optional presence")
            null = (
                literal(node.get("nullValue"), primitive)
                if node.get("nullValue") is not None
                else default_null(primitive)
            )
            minimum = literal(node.get("minValue"), primitive) if node.get("minValue") is not None else ""
            maximum = literal(node.get("maxValue"), primitive) if node.get("maxValue") is not None else ""
            if minimum == "NaN" or maximum == "NaN":
                raise SchemaError("NaN minValue/maxValue unsupported")
            if node.get("minValue") and node.get("maxValue"):
                minimum = literal(node.get("minValue"), primitive)
                maximum = literal(node.get("maxValue"), primitive)
                if primitive in ("float", "double"):
                    invalid_range = float(minimum) > float(maximum)
                else:
                    invalid_range = int(minimum) > int(maximum)
                if invalid_range:
                    raise SchemaError("minValue exceeds maxValue")
            constant = ""
            if presence == "constant":
                if length != 1:
                    raise SchemaError("constant arrays unsupported")
                constant = literal(node.text or "", primitive)
            elif (node.text or "").strip():
                raise SchemaError("unexpected simple type content")
            size = 0 if presence == "constant" else PRIMITIVES[primitive][1] * length
            if size > self.budgets.block_bytes:
                raise SchemaError("encoding exceeds block budget")
            return Encoding(
                **common,
                primitive=primitive,
                length=length,
                size=size,
                null=null,
                constant=constant,
                minimum=minimum,
                maximum=maximum,
            )
        if kind in ("enum", "set"):
            base = self.resolve(node.get("encodingType", ""), depth + 1)
            if base.kind != "type" or base.length != 1 or base.presence != "required":
                raise SchemaError("enum/set requires required scalar encoding")
            if not base.primitive.startswith("u") and not (kind == "enum" and base.primitive == "char"):
                raise SchemaError("enum/set requires unsigned integer (enum also char)")
            if presence == "constant" or (kind == "set" and presence != "required"):
                raise SchemaError("unsupported enum/set presence")
            values, names, numbers = [], set(), set()
            for child in node:
                if tag(child) != ("validValue" if kind == "enum" else "choice"):
                    raise SchemaError("invalid enum/set child")
                item = name(child)
                value = literal(child.text or "", base.primitive) if kind == "enum" else str(number_text(child.text))
                if kind == "set" and int(value) >= base.size * 8:
                    raise SchemaError("set bit outside encoding")
                if item in names or value in numbers:
                    raise SchemaError("duplicate enum/set value")
                names.add(item)
                numbers.add(value)
                v_since, v_deprecated = self.versions(child)
                if v_since < since:
                    raise SchemaError("enum value precedes encoding")
                values.append(Value(item, value, v_since, v_deprecated))
            if not values:
                raise SchemaError("empty enum/set")
            null = literal(node.get("nullValue"), base.primitive) if node.get("nullValue") is not None else base.null
            if node.get("nullValue") is not None and presence != "optional":
                raise SchemaError("enum nullValue requires optional presence")
            if kind == "enum" and null in numbers:
                raise SchemaError("enum value conflicts with null sentinel")
            return Encoding(
                **common,
                primitive=base.primitive,
                size=base.size,
                null=null,
                values=tuple(values),
                minimum=base.minimum,
                maximum=base.maximum,
            )
        if kind != "composite" or depth > self.budgets.nesting:
            raise SchemaError("unsupported encoding or composite nesting")
        fields, end, seen = [], 0, set()
        for child in node:
            self.field_count += 1
            if self.field_count > self.budgets.fields:
                raise SchemaError("field budget exceeded")
            child_name = name(child)
            if child_name in seen:
                raise SchemaError("duplicate composite member")
            seen.add(child_name)
            enc = (
                self.resolve(child.get("type", ""), depth + 1)
                if tag(child) == "ref"
                else self.encoding(child, depth + 1)
            )
            offset = number(child, "offset", end, self.budgets.block_bytes)
            if offset < end:
                raise SchemaError("overlapping composite members")
            c_since, c_deprecated = self.versions(child)
            if max(c_since, enc.since) > since and enc.size:
                raise SchemaError("versioned composite wire members unsupported; version the whole field")
            c_since = max(c_since, enc.since, since)
            fields.append(Field(child_name, 0, enc, offset, c_since, c_deprecated, enc.presence, enc.constant))
            end = offset + enc.size
            if enc.length == 0 and child is not node[-1]:
                raise SchemaError("variable composite member must be last")
        if len(fields) < 2 or end > self.budgets.block_bytes:
            raise SchemaError("composite requires >=2 members within block budget")
        return Encoding(**common, size=end, members=tuple(fields))

    def field(self, node, end):
        enc = self.resolve(node.get("type", ""))
        since, deprecated = self.versions(node)
        if since < enc.since:
            raise SchemaError("field precedes encoding version")
        if enc.semantic and node.get("semanticType") and enc.semantic != node.get("semanticType"):
            raise SchemaError("semanticType mismatch")
        presence = node.get("presence", enc.presence)
        if presence not in ("required", "optional", "constant"):
            raise SchemaError("invalid field presence")
        if enc.presence != "required" and presence != enc.presence:
            raise SchemaError("field/type presence mismatch")
        if presence == "optional" and (enc.kind == "composite" or enc.length != 1 or enc.kind == "set"):
            raise SchemaError("optional composites/arrays/sets unsupported")
        constant = enc.constant
        if node.get("valueRef"):
            ref = node.get("valueRef").split(".")
            if presence != "constant" or len(ref) != 2 or ref[0] != enc.name or enc.kind != "enum":
                raise SchemaError("invalid valueRef")
            value = next((v for v in enc.values if v.name == ref[1]), None)
            if value is None or value.since > since:
                raise SchemaError("undefined or newer constant enum value")
            constant = value.literal
        if presence == "constant" and not constant:
            raise SchemaError("constant field missing value")
        _, variable, expanded = structural_properties(enc, self.properties, self.budgets.expanded_nodes)
        if expanded > self.budgets.expanded_nodes - self.expanded_count:
            raise SchemaError("expanded traversal budget exceeded")
        self.expanded_count += expanded
        if variable:
            raise SchemaError("variable encoding cannot be fixed field")
        offset = number(node, "offset", end, self.budgets.block_bytes)
        if offset < end:
            raise SchemaError("overlapping field offset")
        return Field(name(node), number(node, "id"), enc, offset, since, deprecated, presence, constant)

    def packed(self, key, names):
        enc = self.resolve(key)
        if enc.kind != "composite" or tuple(f.name for f in enc.members) != names:
            raise SchemaError(f"requires packed composite {names}")
        end, result = 0, []
        for f in enc.members:
            if f.offset != end or f.presence != "required" or f.since != 0 or f.encoding.kind != "type":
                raise SchemaError("custom/versioned dimensions/header/data prefix unsupported")
            if f.encoding.length != 1 and f.name != "varData":
                raise SchemaError("header/dimension/data length requires scalar encoding")
            result.append(f.encoding.primitive)
            end += f.encoding.size
        return enc, tuple(result)

    def layout(self, node, depth=0):
        if depth > self.budgets.nesting:
            raise SchemaError("group nesting budget exceeded")
        since, deprecated = self.versions(node)
        fields, tails, end, phase, names, ids = [], [], 0, 0, set(), set()
        last_since = since
        for child in node:
            self.field_count += 1
            if self.field_count > self.budgets.fields:
                raise SchemaError("field budget exceeded")
            kind, child_name, child_id = tag(child), name(child), number(child, "id")
            if child_name in names or child_id in ids:
                raise SchemaError("duplicate member name/id")
            names.add(child_name)
            ids.add(child_id)
            next_phase = {"field": 0, "group": 1, "data": 2}.get(kind)
            if next_phase is None or next_phase < phase:
                raise SchemaError("members must be fields, groups, data in wire order")
            phase = next_phase
            child_since, child_deprecated = self.versions(child)
            if child_since < since or (kind == "field" and child_since < last_since):
                raise SchemaError("versioned additions must append within each block/tail")
            if kind == "field":
                last_since = child_since
            if kind == "field":
                f = self.field(child, end)
                fields.append(f)
                end = f.offset + f.size
            elif kind == "group":
                tails.append(self.layout(child, depth + 1))
            else:
                enc, primitives = self.packed(child.get("type", ""), ("length", "varData"))
                length, data = enc.members
                if not primitives[0].startswith("u") or length.encoding.length != 1:
                    raise SchemaError("data length must be unsigned scalar")
                if data.encoding.length != 0 or primitives[1] not in ("char", "uint8", "int8"):
                    raise SchemaError("data payload must be variable byte/char array")
                if data.encoding.minimum or data.encoding.maximum:
                    raise SchemaError("constrained variable data payload unsupported")
                tails.append(
                    Data(
                        child_name,
                        child_id,
                        primitives[0],
                        child_since,
                        child_deprecated,
                        length.encoding.minimum,
                        length.encoding.maximum,
                    )
                )
        block = number(node, "blockLength", end, self.budgets.block_bytes)
        if block < end:
            raise SchemaError("blockLength smaller than fixed layout")
        dimension = ()
        dimension_bounds: tuple[tuple[str, str], ...] = ()
        if tag(node) == "group":
            self.group_count += 1
            if self.group_count > self.budgets.groups:
                raise SchemaError("schema group budget exceeded")
            dimensions, dimension = self.packed(
                node.get("dimensionType", "groupSizeEncoding"), ("blockLength", "numInGroup")
            )
            dimension_bounds = tuple((f.encoding.minimum, f.encoding.maximum) for f in dimensions.members)
            if any(p not in ("uint8", "uint16", "uint32") for p in dimension):
                raise SchemaError("group dimensions require unsigned <=32-bit scalars")
            if block >= 1 << (PRIMITIVES[dimension[0]][1] * 8):
                raise SchemaError("group blockLength cannot fit dimension encoding")
        return Layout(
            name(node),
            number(node, "id"),
            block,
            since,
            deprecated,
            tuple(fields),
            tuple(tails),
            dimension,
            dimension_bounds,
        )


def number_text(text):
    if text is None or not re.fullmatch(r"[0-9]+", text.strip()):
        raise SchemaError("set choice requires unsigned bit index")
    digits = text.strip().lstrip("0") or "0"
    if len(digits) > 2 or int(digits) > 63:
        raise SchemaError("set bit outside encoding")
    return int(digits)


def structural_properties(enc, cache, limit):
    """Memoize by identity: hashing a recursive frozen DAG itself expands it."""
    key = id(enc)
    if key in cache:
        return cache[key]
    depth, variable, expanded = 0, enc.length == 0, 1
    for member in enc.members:
        child_depth, child_variable, child_expanded = structural_properties(member.encoding, cache, limit)
        depth = max(depth, child_depth + 1)
        variable = variable or child_variable
        if child_expanded > limit - expanded:
            raise SchemaError("expanded traversal budget exceeded")
        expanded += child_expanded
    result = (depth, variable, expanded)
    cache[key] = result
    return result


def compile_schema(xml: bytes, *, budgets=DEFAULT_BUDGETS, expect_sha256: str | None = None) -> SchemaIR:
    """Compile one self-contained schema; no files, URLs, entities, DTD or XInclude."""
    if not isinstance(xml, bytes) or len(xml) > budgets.file_bytes:
        raise SchemaError("schema bytes required within file budget")
    digest = hashlib.sha256(xml).hexdigest()
    if expect_sha256 is not None and digest != expect_sha256:
        raise SchemaError("schema SHA-256 mismatch")
    parser = etree.XMLParser(
        resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False, remove_comments=True, remove_pis=True
    )
    try:
        root = etree.fromstring(xml, parser)
    except etree.XMLSyntaxError as error:
        raise SchemaError(f"invalid XML: {error}") from error
    if root.getroottree().docinfo.doctype:
        raise SchemaError("DTD/entities forbidden")
    for count, node in enumerate(root.iter(), 1):
        if count > budgets.xml_nodes:
            raise SchemaError("XML node budget exceeded")
        kind = tag(node)
        if kind not in ATTRS or set(node.attrib) - ATTRS[kind]:
            raise SchemaError(f"unsupported node/attribute: {kind}")
        allowed_children = {
            "messageSchema": {"types", "message"},
            "types": {"type", "composite", "enum", "set"},
            "composite": {"type", "composite", "enum", "set", "ref"},
            "enum": {"validValue"},
            "set": {"choice"},
            "message": {"field", "group", "data"},
            "group": {"field", "group", "data"},
        }.get(kind, set())
        if any(tag(child) not in allowed_children for child in node):
            raise SchemaError(f"unsupported child of {kind}")
    if tag(root) != "messageSchema" or any(tag(n) not in ("types", "message") for n in root):
        raise SchemaError("expected messageSchema with types/messages")
    order = root.get("byteOrder", "littleEndian")
    if order not in ("littleEndian", "bigEndian"):
        raise SchemaError("invalid byteOrder")
    compiler = _Compiler(root, budgets)
    for key in compiler.nodes:
        enc = compiler.resolve(key)
        depth, _, _ = structural_properties(enc, compiler.properties, budgets.expanded_nodes)
        if depth > budgets.nesting:
            raise SchemaError("type nesting budget exceeded")
    header, primitives = compiler.packed(
        root.get("headerType", "messageHeader"), ("blockLength", "templateId", "schemaId", "version")
    )
    if header.size != 8 or primitives != ("uint16",) * 4:
        raise SchemaError("only common four-uint16 message header supported")
    if any(f.encoding.minimum or f.encoding.maximum for f in header.members):
        raise SchemaError("constrained message header scalars unsupported")
    messages, names, ids = [], set(), set()
    for node in root:
        if tag(node) != "message":
            continue
        message = compiler.layout(node)
        if message.name in names or message.id in ids:
            raise SchemaError("duplicate message name/id")
        names.add(message.name)
        ids.add(message.id)
        messages.append(message)
    if not messages or len(messages) > budgets.messages:
        raise SchemaError("message budget exceeded or no messages")
    return SchemaIR(
        number(root, "id"),
        compiler.version,
        order,
        root.get("package", ""),
        digest,
        tuple(compiler.resolved.values()),
        tuple(messages),
        budgets,
    )
