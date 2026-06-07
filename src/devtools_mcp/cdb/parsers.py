"""Parsers for batch CDB output: backtraces, !analyze -v, registers."""

from __future__ import annotations

import re

from devtools_mcp.cdb.models import CdbStackFrame, CdbThread

_THREAD_HDR = re.compile(r"^\s*(?P<idx>\d+)\s+Id:\s*(?P<tid>[0-9a-fA-F]+\.[0-9a-fA-F]+)")
_FRAME = re.compile(r"^(?P<idx>[0-9a-fA-F]{2})\s+(?P<rest>.+)$")
_ADDR = re.compile(r"^[0-9a-fA-F`]+$")
_SRC = re.compile(r"\[(?P<file>.+?)\s+@\s+(?P<line>\d+)\]\s*$")
_CALL = re.compile(r"^(?P<mod>[^!\s]+)!(?P<fn>[^+\s]+)(?P<off>\+0x[0-9a-fA-F]+)?")
_ANALYZE_KEYS = {
    "SYMBOL_NAME", "MODULE_NAME", "IMAGE_NAME", "FAILURE_BUCKET_ID", "EXCEPTION_CODE",
    "EXCEPTION_CODE_STR", "FAULTING_IP", "PROCESS_NAME", "STACK_COMMAND", "BUGCHECK_CODE",
}
_REG = re.compile(r"\b([a-z]{2,3}[0-9]?)=([0-9a-fA-F`]+)")
MAX_LINES = 200_000


def _parse_call_site(rest: str, idx: int) -> CdbStackFrame | None:
    """Strip leading address columns, then parse `module!func+off [file @ line]`."""
    src = _SRC.search(rest)
    file_, line_ = (src.group("file"), int(src.group("line"))) if src else (None, None)
    if src:
        rest = rest[: src.start()].rstrip()
    tokens = rest.split()
    while tokens and _ADDR.match(tokens[0]):
        tokens.pop(0)
    if not tokens:
        return None
    call = " ".join(tokens)
    m = _CALL.match(call)
    if m:
        return CdbStackFrame(index=idx, module=m.group("mod"), function=m.group("fn"),
                             offset=m.group("off") or "", file=file_, line=line_)
    return CdbStackFrame(index=idx, function=call, file=file_, line=line_)


def parse_stacks(text: str) -> list[CdbThread]:
    """Parse `k` / `~*k` backtraces into threads."""
    assert isinstance(text, str), "cdb text must be str"
    threads: list[CdbThread] = []
    current: CdbThread | None = None
    for line in text.splitlines()[:MAX_LINES]:
        hdr = _THREAD_HDR.match(line)
        if hdr:
            current = CdbThread(index=int(hdr.group("idx")), tid=hdr.group("tid"))
            threads.append(current)
            continue
        if "ChildEBP" in line or "Child-SP" in line or "Call Site" in line:
            continue
        fm = _FRAME.match(line)
        if not fm:
            continue
        frame = _parse_call_site(fm.group("rest"), int(fm.group("idx"), 16))
        if frame is None:
            continue
        if current is None:
            current = CdbThread(index=0)
            threads.append(current)
        current.frames.append(frame)
    return threads


def parse_analyze(text: str) -> tuple[dict[str, str], str]:
    """Extract key !analyze -v fields and the exception summary."""
    assert isinstance(text, str), "cdb text must be str"
    fields: dict[str, str] = {}
    for line in text.splitlines()[:MAX_LINES]:
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        if key in _ANALYZE_KEYS and val.strip():
            fields[key] = val.strip()
    exception = fields.get("EXCEPTION_CODE_STR") or fields.get("EXCEPTION_CODE", "")
    return fields, exception


def parse_registers(text: str) -> dict[str, str]:
    """Parse a `r` register dump into name->value."""
    assert isinstance(text, str), "cdb text must be str"
    regs: dict[str, str] = {}
    for name, val in _REG.findall(text):
        regs[name] = val
    return regs
