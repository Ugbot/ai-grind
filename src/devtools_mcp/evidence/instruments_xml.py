"""Private bounded UTF-8 xctrace XML reader and per-file reference validator."""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from pathlib import Path
from xml.etree.ElementTree import Element
from xml.parsers import expat

from .artifacts import fingerprint_file, verify_binding
from .benchmark import EvidenceParseError

MAX_DEPTH = 64
U64 = (1 << 64) - 1


def require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceParseError(message)


def uint(text: str | None) -> int:
    require(text is not None and 0 < len(text) <= 20 and text.isascii() and text.isdecimal(), "expected uint64")
    assert text is not None
    value = int(text)
    require(value <= U64, "uint64 overflow")
    return value


class Document:
    def __init__(self, data: bytes, remaining_nodes: int, remaining_rows: int):
        self.ids: dict[str, Element] = {}
        self.process_labels: dict[int, str] = {}
        self.nodes = 0
        self.rows = 0
        stack: list[Element] = []
        roots: list[Element] = []
        parser = expat.ParserCreate(encoding="UTF-8")

        def start(tag: str, attrs: dict[str, str]) -> None:
            self.nodes += 1
            self.rows += tag == "row"
            require(self.nodes <= remaining_nodes and self.rows <= remaining_rows, "XML node/row cap exceeded")
            require(len(stack) < MAX_DEPTH, "XML depth cap exceeded")
            element = Element(tag, attrs)
            if stack:
                stack[-1].append(element)
            else:
                roots.append(element)
            stack.append(element)
            if "id" in attrs:
                uint(attrs["id"])
                require(attrs["id"] not in self.ids, "duplicate XML id")
                self.ids[attrs["id"]] = element

        def end(tag: str) -> None:
            require(bool(stack) and stack[-1].tag == tag, "XML nesting mismatch")
            stack.pop()

        def text(value: str) -> None:
            if stack:
                stack[-1].text = (stack[-1].text or "") + value

        def forbidden(*_args: object) -> None:
            raise EvidenceParseError("DTD/entities are forbidden")

        def declaration(_version: str, encoding: str | None, _standalone: int) -> None:
            require(encoding is None or encoding.lower() == "utf-8", "XML must declare UTF-8")

        parser.XmlDeclHandler = declaration
        parser.StartElementHandler = start
        parser.EndElementHandler = end
        parser.CharacterDataHandler = text
        parser.StartDoctypeDeclHandler = forbidden
        parser.EntityDeclHandler = forbidden
        try:
            data.decode("utf-8", errors="strict")
            parser.Parse(data, True)
        except (expat.ExpatError, UnicodeError) as exc:
            raise EvidenceParseError("malformed UTF-8 XML") from exc
        require(len(roots) == 1 and roots[0].tag == "trace-query-result", "unexpected XML root")
        self.root = roots[0]
        self._validate_references()

    def _validate_references(self) -> None:
        # DAG edges include both containment and ref links, not merely ref chains.
        state: dict[Element, int] = {}
        pending = [(self.root, False, 0)]
        while pending:
            node, leaving, depth = pending.pop()
            if leaving:
                state[node] = 2
                continue
            require(depth < MAX_DEPTH, "XML reference depth cap exceeded")
            require(state.get(node) != 1, "cyclic XML reference")
            if state.get(node) == 2:
                continue
            state[node] = 1
            pending.append((node, True, depth))
            children = list(node)
            if "ref" in node.attrib:
                require(
                    set(node.attrib) == {"ref"} and not children and not (node.text or "").strip(), "invalid ref node"
                )
                uint(node.attrib["ref"])
                target = self.ids.get(node.attrib["ref"])
                require(target is not None and target.tag == node.tag, "missing or tag-mismatched XML ref")
                assert target is not None
                children.append(target)
            for child in reversed(children):
                pending.append((child, False, depth + 1))

    def resolve(self, element: Element) -> Element:
        for _ in range(MAX_DEPTH):
            if "ref" not in element.attrib:
                return element
            element = self.ids[element.attrib["ref"]]
        raise EvidenceParseError("XML reference depth cap exceeded")


def read_document(path: str | Path, max_bytes: int, nodes: int, rows: int) -> tuple[Document, dict]:
    binding = fingerprint_file(Path(path), max_bytes=max_bytes)
    with Path(binding.path).open("rb") as stream:
        data = stream.read(binding.size_bytes + 1)
    require(
        len(data) == binding.size_bytes and hashlib.sha256(data).hexdigest() == binding.sha256,
        "XML artifact changed during read",
    )
    verify_binding(binding, max_bytes=max_bytes)
    return Document(data, nodes, rows), asdict(binding)
