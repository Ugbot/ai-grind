"""Test fixtures with generated sample data for all Valgrind output formats."""

from __future__ import annotations

import os
import random
import string
import tempfile
import uuid

import pytest


def _random_hex(n: int = 8) -> str:
    return "0x" + "".join(random.choices("0123456789ABCDEF", k=n))


def _random_fn() -> str:
    prefixes = ["alloc", "init", "process", "handle", "compute", "parse", "read", "write"]
    suffixes = ["_data", "_buffer", "_node", "_entry", "_block", "_chunk", ""]
    return random.choice(prefixes) + random.choice(suffixes)


def _random_file() -> str:
    names = ["main.c", "utils.c", "parser.c", "buffer.c", "network.c", "thread.c", "alloc.c"]
    return random.choice(names)


@pytest.fixture
def memcheck_xml_file() -> str:
    """Generate a valid memcheck XML file with randomized errors."""
    num_errors = random.randint(3, 8)
    leak_kinds = ["Leak_DefinitelyLost", "Leak_PossiblyLost", "Leak_IndirectlyLost"]
    error_kinds = ["InvalidRead", "InvalidWrite", "UninitValue", "UninitCondition"]
    all_kinds = leak_kinds + error_kinds

    errors_xml = []
    for i in range(num_errors):
        kind = random.choice(all_kinds)
        fn = _random_fn()
        src_file = _random_file()
        line = random.randint(10, 500)
        unique_id = str(uuid.uuid4().int % 100000)

        if kind.startswith("Leak_"):
            leaked_bytes = random.randint(16, 65536)
            leaked_blocks = random.randint(1, 100)
            what_xml = f"""<xwhat>
        <text>{leaked_bytes} bytes in {leaked_blocks} blocks are {kind.replace('Leak_', '').lower()}</text>
        <leakedbytes>{leaked_bytes}</leakedbytes>
        <leakedblocks>{leaked_blocks}</leakedblocks>
      </xwhat>"""
        else:
            size = random.choice([1, 2, 4, 8])
            what_xml = f"<what>{kind} of size {size}</what>"

        error_xml = f"""  <error>
    <unique>{unique_id}</unique>
    <tid>1</tid>
    <kind>{kind}</kind>
    {what_xml}
    <stack>
      <frame>
        <ip>{_random_hex()}</ip>
        <obj>/usr/lib/libc.so</obj>
        <fn>{fn}</fn>
        <dir>/home/user/src</dir>
        <file>{src_file}</file>
        <line>{line}</line>
      </frame>
      <frame>
        <ip>{_random_hex()}</ip>
        <fn>main</fn>
        <dir>/home/user/src</dir>
        <file>main.c</file>
        <line>{random.randint(10, 200)}</line>
      </frame>
    </stack>
  </error>"""
        errors_xml.append(error_xml)

    xml_content = f"""<?xml version="1.0"?>
<valgrindoutput>
<protocolversion>4</protocolversion>
<protocoltool>memcheck</protocoltool>
<preamble>
  <line>Memcheck, a memory error detector</line>
</preamble>
<pid>{random.randint(1000, 99999)}</pid>
<tool>memcheck</tool>
<args>
  <vargv><exe>/usr/bin/valgrind</exe></vargv>
  <argv><exe>./test_binary</exe></argv>
</args>
<status><state>RUNNING</state></status>
{"".join(errors_xml)}
<status><state>FINISHED</state></status>
</valgrindoutput>"""

    fd, path = tempfile.mkstemp(suffix=".xml", prefix="memcheck-test-")
    os.write(fd, xml_content.encode())
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def helgrind_xml_file() -> str:
    """Generate a valid helgrind XML file with randomized thread errors."""
    num_errors = random.randint(2, 5)
    kinds = ["Race", "LockOrder", "UnlockUnlocked", "UnlockForeign"]

    errors_xml = []
    for i in range(num_errors):
        kind = random.choice(kinds)
        fn = _random_fn()
        tid = random.randint(1, 8)
        unique_id = str(uuid.uuid4().int % 100000)

        error_xml = f"""  <error>
    <unique>{unique_id}</unique>
    <tid>{tid}</tid>
    <kind>{kind}</kind>
    <what>Possible data race during {random.choice(['read', 'write'])} of size {random.choice([1,4,8])}</what>
    <stack>
      <frame>
        <ip>{_random_hex()}</ip>
        <fn>{fn}</fn>
        <dir>/home/user/src</dir>
        <file>thread.c</file>
        <line>{random.randint(10, 300)}</line>
      </frame>
    </stack>
  </error>"""
        errors_xml.append(error_xml)

    xml_content = f"""<?xml version="1.0"?>
<valgrindoutput>
<protocolversion>4</protocolversion>
<protocoltool>helgrind</protocoltool>
<pid>{random.randint(1000, 99999)}</pid>
<tool>helgrind</tool>
{"".join(errors_xml)}
</valgrindoutput>"""

    fd, path = tempfile.mkstemp(suffix=".xml", prefix="helgrind-test-")
    os.write(fd, xml_content.encode())
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def callgrind_file() -> str:
    """Generate a callgrind output file with randomized profiling data."""
    num_functions = random.randint(5, 15)
    events = ["Ir", "Dr", "Dw"]

    lines_out = [
        "# callgrind format",
        "version: 1",
        f"creator: test-generator",
        f"pid: {random.randint(1000, 99999)}",
        "cmd: ./test_binary",
        "",
        f"events: {' '.join(events)}",
        "",
    ]

    total_costs = {e: 0 for e in events}
    functions = []

    for _ in range(num_functions):
        fn_name = _random_fn() + "_" + "".join(random.choices(string.ascii_lowercase, k=3))
        src_file = _random_file()
        functions.append((fn_name, src_file))

        lines_out.append(f"fl={src_file}")
        lines_out.append(f"fn={fn_name}")

        num_cost_lines = random.randint(1, 5)
        for _ in range(num_cost_lines):
            line_num = random.randint(1, 500)
            costs = [random.randint(100, 1000000) for _ in events]
            for idx, c in enumerate(costs):
                total_costs[events[idx]] += c
            lines_out.append(f"{line_num} {' '.join(str(c) for c in costs)}")

        lines_out.append("")

    # Add some call relationships
    if len(functions) > 2:
        for i in range(min(5, len(functions) - 1)):
            caller_fn, caller_file = functions[i]
            callee_fn, callee_file = functions[i + 1]
            call_count = random.randint(1, 10000)
            costs = [random.randint(100, 50000) for _ in events]

            lines_out.append(f"fl={caller_file}")
            lines_out.append(f"fn={caller_fn}")
            lines_out.append(f"cfn={callee_fn}")
            lines_out.append(f"calls={call_count} {random.randint(1, 100)}")
            lines_out.append(f"{random.randint(1, 200)} {' '.join(str(c) for c in costs)}")
            lines_out.append("")

    lines_out.append(f"totals: {' '.join(str(total_costs[e]) for e in events)}")

    fd, path = tempfile.mkstemp(suffix=".out", prefix="callgrind-test-")
    os.write(fd, "\n".join(lines_out).encode())
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def cachegrind_file() -> str:
    """Generate a cachegrind output file with randomized data."""
    events = ["Ir", "I1mr", "ILmr", "Dr", "D1mr", "DLmr", "Dw", "D1mw", "DLmw"]
    num_functions = random.randint(3, 10)

    lines_out = [
        "# cachegrind format",
        f"events: {' '.join(events)}",
        "",
    ]

    total_costs = {e: 0 for e in events}

    for _ in range(num_functions):
        fn_name = _random_fn()
        src_file = _random_file()
        lines_out.append(f"fl={src_file}")
        lines_out.append(f"fn={fn_name}")

        num_lines = random.randint(2, 8)
        for _ in range(num_lines):
            line_num = random.randint(1, 500)
            ir = random.randint(1000, 500000)
            # Cache misses should be much smaller than refs
            costs = [
                ir,
                random.randint(0, ir // 100),    # I1mr
                random.randint(0, ir // 1000),   # ILmr
                random.randint(100, ir // 2),    # Dr
                random.randint(0, ir // 200),    # D1mr
                random.randint(0, ir // 2000),   # DLmr
                random.randint(50, ir // 4),     # Dw
                random.randint(0, ir // 300),    # D1mw
                random.randint(0, ir // 3000),   # DLmw
            ]
            for idx, c in enumerate(costs):
                total_costs[events[idx]] += c
            lines_out.append(f"{line_num} {' '.join(str(c) for c in costs)}")

        lines_out.append("")

    lines_out.append(f"summary: {' '.join(str(total_costs[e]) for e in events)}")

    fd, path = tempfile.mkstemp(suffix=".out", prefix="cachegrind-test-")
    os.write(fd, "\n".join(lines_out).encode())
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def massif_file() -> str:
    """Generate a massif output file with randomized heap snapshots."""
    num_snapshots = random.randint(10, 30)
    peak_idx = random.randint(num_snapshots // 2, num_snapshots - 2)

    lines_out = [
        "desc: --massif-out-file=massif.out --stacks=yes",
        "cmd: ./test_binary",
        "time_unit: i",
    ]

    heap_size = 0
    for idx in range(num_snapshots):
        # Simulate growing then shrinking heap
        if idx <= peak_idx:
            heap_size += random.randint(1000, 50000)
        else:
            heap_size = max(0, heap_size - random.randint(500, 30000))

        heap_extra = random.randint(100, heap_size // 4 + 1)
        stacks = random.randint(100, 5000)
        time_val = idx * random.randint(100000, 500000)

        is_peak = (idx == peak_idx)
        is_detailed = is_peak or (idx % 10 == 0)

        lines_out.append("#-----------")
        lines_out.append(f"snapshot={idx}")
        lines_out.append("#-----------")
        lines_out.append(f"time={time_val}")
        lines_out.append(f"mem_heap_B={heap_size}")
        lines_out.append(f"mem_heap_extra_B={heap_extra}")
        lines_out.append(f"mem_stacks_B={stacks}")

        if is_peak:
            lines_out.append("heap_tree=peak")
            # Generate a simple allocation tree
            fn1 = _random_fn()
            fn2 = _random_fn()
            alloc1 = random.randint(heap_size // 4, heap_size // 2)
            alloc2 = heap_size - alloc1
            lines_out.append(f"n2: {heap_size} (heap allocation functions) malloc/new/new[], --alloc-fns, etc.")
            lines_out.append(f" n0: {alloc1} {_random_hex()}: {fn1} ({_random_file()}:{random.randint(10,200)})")
            lines_out.append(f" n0: {alloc2} {_random_hex()}: {fn2} ({_random_file()}:{random.randint(10,200)})")
        elif is_detailed:
            lines_out.append("heap_tree=detailed")
            fn1 = _random_fn()
            lines_out.append(f"n1: {heap_size} (heap allocation functions) malloc/new/new[], --alloc-fns, etc.")
            lines_out.append(f" n0: {heap_size} {_random_hex()}: {fn1} ({_random_file()}:{random.randint(10,200)})")
        else:
            lines_out.append("heap_tree=empty")

    fd, path = tempfile.mkstemp(suffix=".out", prefix="massif-test-")
    os.write(fd, "\n".join(lines_out).encode())
    os.close(fd)
    yield path
    os.unlink(path)
