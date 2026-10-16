"""Immutable, bounded cold-path SBE compiler contract; no runtime XML in Bolt."""

from dataclasses import dataclass


class SchemaError(ValueError):
    """Invalid or explicitly unsupported SBE schema; compilation emits nothing."""


@dataclass(frozen=True)
class CompilerBudgets:
    file_bytes: int = 2 * 1024 * 1024
    xml_nodes: int = 32768
    types: int = 4096
    messages: int = 1024
    groups: int = 4096
    fields: int = 16384
    nesting: int = 8
    array_elements: int = 65535
    block_bytes: int = 65535
    message_bytes: int = 16 * 1024 * 1024
    group_entries: int = 65535
    data_bytes: int = 1024 * 1024
    expanded_nodes: int = 65536
    header_bytes: int = 4 * 1024 * 1024

    def __post_init__(self):
        if any(not isinstance(value, int) or value <= 0 for value in vars(self).values()):
            raise SchemaError("all compiler budgets must be positive integers")
        if self.expanded_nodes > 1024 * 1024 or self.header_bytes > 16 * 1024 * 1024:
            raise SchemaError("expanded work/output budgets exceed supported caps")
        if self.block_bytes > 65535 or self.message_bytes > 0xFFFFFFFF:
            raise SchemaError("budgets exceed Bolt wire integer representation")
        if self.group_entries > 0xFFFFFFFF or self.data_bytes > self.message_bytes or self.nesting > 64:
            raise SchemaError("wire/traversal budgets exceed supported representation")


@dataclass(frozen=True)
class Value:
    name: str
    literal: str
    since: int = 0
    deprecated: int | None = None


@dataclass(frozen=True)
class Encoding:
    name: str
    kind: str
    primitive: str = ""
    length: int = 1
    size: int = 0
    presence: str = "required"
    null: str = ""
    constant: str = ""
    since: int = 0
    deprecated: int | None = None
    semantic: str = ""
    members: tuple["Field", ...] = ()
    values: tuple[Value, ...] = ()
    minimum: str = ""
    maximum: str = ""


@dataclass(frozen=True)
class Field:
    name: str
    id: int
    encoding: Encoding
    offset: int
    since: int
    deprecated: int | None
    presence: str
    constant: str = ""

    @property
    def size(self):
        return 0 if self.presence == "constant" else self.encoding.size


@dataclass(frozen=True)
class Data:
    name: str
    id: int
    length_primitive: str
    since: int
    deprecated: int | None
    length_minimum: str = ""
    length_maximum: str = ""


@dataclass(frozen=True)
class Layout:
    name: str
    id: int
    block_length: int
    since: int
    deprecated: int | None
    fields: tuple[Field, ...]
    tails: tuple["Layout | Data", ...]
    dimension: tuple[str, ...] = ()
    dimension_bounds: tuple[tuple[str, str], ...] = ()
    dimension_offsets: tuple[int, ...] = ()
    dimension_size: int = 0


@dataclass(frozen=True)
class SchemaIR:
    schema_id: int
    version: int
    byte_order: str
    package: str
    sha256: str
    encodings: tuple[Encoding, ...]
    messages: tuple[Layout, ...]
    budgets: CompilerBudgets
