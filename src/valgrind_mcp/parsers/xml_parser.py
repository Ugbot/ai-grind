"""Parser for Valgrind XML Protocol 4 output (memcheck, helgrind, drd)."""

from __future__ import annotations

from lxml import etree

from valgrind_mcp.models import (
    MemcheckError,
    MemcheckResult,
    StackFrame,
    ThreadCheckResult,
    ThreadError,
    ValgrindRun,
)


def _text(elem: etree._Element, tag: str) -> str:
    """Get text content of a child element, empty string if missing."""
    child = elem.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return ""


def _int(elem: etree._Element, tag: str, default: int = 0) -> int:
    """Get integer content of a child element."""
    text = _text(elem, tag)
    if text:
        try:
            return int(text)
        except ValueError:
            return default
    return default


def _parse_frame(frame_elem: etree._Element) -> StackFrame:
    """Parse a single <frame> element into a StackFrame."""
    return StackFrame(
        ip=_text(frame_elem, "ip"),
        obj=_text(frame_elem, "obj") or None,
        fn=_text(frame_elem, "fn") or None,
        dir=_text(frame_elem, "dir") or None,
        file=_text(frame_elem, "file") or None,
        line=_int(frame_elem, "line") or None,
    )


def _parse_stack(stack_elem: etree._Element | None) -> list[StackFrame]:
    """Parse a <stack> element into a list of StackFrames."""
    if stack_elem is None:
        return []
    return [_parse_frame(f) for f in stack_elem.findall("frame")]


def parse_memcheck_xml(xml_path: str, run_base: ValgrindRun) -> MemcheckResult:
    """Parse memcheck XML output into a MemcheckResult.

    Uses iterparse with element clearing for memory efficiency on large files.
    """
    errors: list[MemcheckError] = []
    error_counts: dict[str, int] = {}
    leak_summary: dict[str, int] = {}

    try:
        context = etree.iterparse(xml_path, events=("end",), tag=("error", "errorcounts", "suppcounts"))
    except (etree.XMLSyntaxError, OSError) as e:
        # Return empty result if XML is malformed or missing
        return MemcheckResult(
            **run_base.model_dump(),
            errors=[],
            leak_summary={},
            error_summary={},
        )

    for event, elem in context:
        if elem.tag == "error":
            kind = _text(elem, "kind")

            # Get description from <what> or <xwhat>/<text>
            what = _text(elem, "what")
            if not what:
                xwhat = elem.find("xwhat")
                if xwhat is not None:
                    what = _text(xwhat, "text")

            # Leak info from <xwhat>
            bytes_leaked = None
            blocks_leaked = None
            xwhat = elem.find("xwhat")
            if xwhat is not None:
                bl = _text(xwhat, "leakedbytes")
                if bl:
                    bytes_leaked = int(bl)
                bk = _text(xwhat, "leakedblocks")
                if bk:
                    blocks_leaked = int(bk)

            # Parse stacks
            stacks = elem.findall("stack")
            main_stack = _parse_stack(stacks[0]) if stacks else []
            aux_stack = _parse_stack(stacks[1]) if len(stacks) > 1 else None

            # Aux description
            auxwhat = _text(elem, "auxwhat") or None

            error = MemcheckError(
                unique_id=_text(elem, "unique"),
                kind=kind,
                what=what,
                bytes_leaked=bytes_leaked,
                blocks_leaked=blocks_leaked,
                stack=main_stack,
                auxstack=aux_stack,
                auxwhat=auxwhat,
            )
            errors.append(error)

            # Count by kind
            error_counts[kind] = error_counts.get(kind, 0) + 1

            # Track leak bytes by kind
            if bytes_leaked is not None and "Leak" in kind:
                leak_key = kind.replace("Leak_", "").lower()
                leak_summary[leak_key] = leak_summary.get(leak_key, 0) + bytes_leaked

            # Clear element to free memory
            elem.clear()

        elif elem.tag == "errorcounts":
            # Parse error count pairs
            for pair in elem.findall("pair"):
                count_kind = _text(pair, "name")
                count_val = _int(pair, "count")
                if count_kind:
                    error_counts[count_kind] = count_val
            elem.clear()

    return MemcheckResult(
        **run_base.model_dump(),
        errors=errors,
        leak_summary=leak_summary,
        error_summary=error_counts,
    )


def parse_threadcheck_xml(xml_path: str, run_base: ValgrindRun, tool: str = "helgrind") -> ThreadCheckResult:
    """Parse helgrind/drd XML output into a ThreadCheckResult.

    Same XML Protocol 4 format as memcheck but with thread-specific error kinds.
    """
    errors: list[ThreadError] = []
    error_counts: dict[str, int] = {}

    try:
        context = etree.iterparse(xml_path, events=("end",), tag=("error", "errorcounts"))
    except (etree.XMLSyntaxError, OSError) as e:
        return ThreadCheckResult(
            **run_base.model_dump(),
            errors=[],
            error_summary={},
        )

    for event, elem in context:
        if elem.tag == "error":
            kind = _text(elem, "kind")

            what = _text(elem, "what")
            if not what:
                xwhat = elem.find("xwhat")
                if xwhat is not None:
                    what = _text(xwhat, "text")

            # Thread ID
            tid = _int(elem, "tid") or None

            # Parse stacks
            stacks = elem.findall("stack")
            main_stack = _parse_stack(stacks[0]) if stacks else []
            aux_stack = _parse_stack(stacks[1]) if len(stacks) > 1 else None

            auxwhat = _text(elem, "auxwhat") or None

            error = ThreadError(
                unique_id=_text(elem, "unique"),
                kind=kind,
                what=what,
                stack=main_stack,
                auxstack=aux_stack,
                auxwhat=auxwhat,
                thread_id=tid,
            )
            errors.append(error)
            error_counts[kind] = error_counts.get(kind, 0) + 1
            elem.clear()

        elif elem.tag == "errorcounts":
            for pair in elem.findall("pair"):
                count_kind = _text(pair, "name")
                count_val = _int(pair, "count")
                if count_kind:
                    error_counts[count_kind] = count_val
            elem.clear()

    return ThreadCheckResult(
        **run_base.model_dump(),
        errors=errors,
        error_summary=error_counts,
    )
