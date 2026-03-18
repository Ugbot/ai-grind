"""End-to-end tests for the MCP server tool endpoints.

Uses the MCP SDK's in-memory client/server session to test each tool
without needing a real stdio transport. All data is generated via the
existing conftest fixtures + randomized factories.
"""

from __future__ import annotations

import random
import string
import uuid

import polars as pl
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from devtools_mcp.models import RunBase
from devtools_mcp.registry import InstalledTool, ToolRegistry, get_backend
from devtools_mcp.server import mcp
from devtools_mcp.valgrind.models import (
    CallgrindFunction,
    CallgrindResult,
    CachegrindLine,
    CachegrindResult,
    MassifResult,
    MassifSnapshot,
    MemcheckError,
    MemcheckResult,
    StackFrame,
    ThreadCheckResult,
    ThreadError,
)
from devtools_mcp.workspace import AppContext


# ---------------------------------------------------------------------------
# Helpers: randomized data factories
# ---------------------------------------------------------------------------


def _rand_id() -> str:
    return str(uuid.uuid4())


def _rand_fn() -> str:
    prefixes = ["alloc", "init", "process", "handle", "compute", "parse", "read", "write"]
    suffixes = ["_data", "_buffer", "_node", "_entry", "_block", "_chunk", ""]
    return random.choice(prefixes) + random.choice(suffixes) + "_" + "".join(random.choices(string.ascii_lowercase, k=3))


def _rand_file() -> str:
    names = ["main.c", "utils.c", "parser.c", "buffer.c", "network.c", "thread.c", "alloc.c"]
    return random.choice(names)


def _make_memcheck_result(run_id: str | None = None, num_errors: int | None = None) -> MemcheckResult:
    """Generate a MemcheckResult with randomized errors."""
    rid = run_id or _rand_id()
    n = num_errors or random.randint(3, 8)
    leak_kinds = ["Leak_DefinitelyLost", "Leak_PossiblyLost", "Leak_IndirectlyLost"]
    error_kinds = ["InvalidRead", "InvalidWrite", "UninitValue"]
    all_kinds = leak_kinds + error_kinds

    errors = []
    error_summary: dict[str, int] = {}
    leak_summary: dict[str, int] = {}

    for _ in range(n):
        kind = random.choice(all_kinds)
        fn = _rand_fn()
        src = _rand_file()
        line = random.randint(10, 500)
        leaked = random.randint(16, 65536) if kind.startswith("Leak_") else None

        errors.append(
            MemcheckError(
                unique_id=_rand_id(),
                kind=kind,
                what=f"{kind} of size {random.randint(1, 8)}" if not leaked else f"{leaked} bytes leaked",
                bytes_leaked=leaked,
                blocks_leaked=random.randint(1, 50) if leaked else None,
                stack=[
                    StackFrame(ip=f"0x{random.randint(0, 0xFFFFFFFF):08X}", fn=fn, file=src, line=line),
                    StackFrame(ip=f"0x{random.randint(0, 0xFFFFFFFF):08X}", fn="main", file="main.c", line=random.randint(10, 200)),
                ],
            )
        )
        error_summary[kind] = error_summary.get(kind, 0) + 1
        if leaked:
            leak_summary[kind] = leak_summary.get(kind, 0) + leaked

    return MemcheckResult(
        run_id=rid,
        suite="valgrind",
        tool="memcheck",
        binary="./test_binary",
        errors=errors,
        error_summary=error_summary,
        leak_summary=leak_summary,
        exit_code=0,
        duration_seconds=random.uniform(0.1, 5.0),
    )


def _make_callgrind_result(run_id: str | None = None) -> CallgrindResult:
    """Generate a CallgrindResult with randomized profiling data."""
    rid = run_id or _rand_id()
    events = ["Ir", "Dr", "Dw"]
    num_fns = random.randint(5, 15)

    functions = []
    totals = {e: 0 for e in events}

    for _ in range(num_fns):
        fn_name = _rand_fn()
        costs = {e: random.randint(1000, 1_000_000) for e in events}
        for e, v in costs.items():
            totals[e] += v
        functions.append(
            CallgrindFunction(
                name=fn_name,
                file=_rand_file(),
                self_cost=costs,
                inclusive_cost={e: v + random.randint(0, v) for e, v in costs.items()},
            )
        )

    return CallgrindResult(
        run_id=rid,
        suite="valgrind",
        tool="callgrind",
        binary="./test_binary",
        events=events,
        functions=functions,
        totals=totals,
        exit_code=0,
        duration_seconds=random.uniform(0.5, 10.0),
    )


def _make_massif_result(run_id: str | None = None) -> MassifResult:
    """Generate a MassifResult with randomized snapshots."""
    rid = run_id or _rand_id()
    num_snaps = random.randint(10, 25)
    peak_idx = random.randint(num_snaps // 2, num_snaps - 2)
    snapshots = []
    heap = 0

    for i in range(num_snaps):
        if i <= peak_idx:
            heap += random.randint(1000, 50000)
        else:
            heap = max(0, heap - random.randint(500, 30000))
        snapshots.append(
            MassifSnapshot(
                index=i,
                time=i * random.randint(100000, 500000),
                heap_bytes=heap,
                heap_extra_bytes=random.randint(100, max(heap // 4, 200)),
                stacks_bytes=random.randint(100, 5000),
                is_peak=(i == peak_idx),
                is_detailed=(i == peak_idx or i % 10 == 0),
            )
        )

    return MassifResult(
        run_id=rid,
        suite="valgrind",
        tool="massif",
        binary="./test_binary",
        snapshots=snapshots,
        peak_snapshot_index=peak_idx,
        command="./test_binary",
        time_unit="i",
        exit_code=0,
        duration_seconds=random.uniform(0.1, 3.0),
    )


def _make_threadcheck_result(run_id: str | None = None) -> ThreadCheckResult:
    """Generate a ThreadCheckResult with randomized thread errors."""
    rid = run_id or _rand_id()
    n = random.randint(2, 5)
    kinds = ["Race", "LockOrder", "UnlockUnlocked"]
    errors = []
    error_summary: dict[str, int] = {}

    for _ in range(n):
        kind = random.choice(kinds)
        errors.append(
            ThreadError(
                unique_id=_rand_id(),
                kind=kind,
                what=f"Possible data race during {random.choice(['read', 'write'])} of size {random.choice([1, 4, 8])}",
                thread_id=random.randint(1, 8),
                stack=[
                    StackFrame(ip=f"0x{random.randint(0, 0xFFFFFFFF):08X}", fn=_rand_fn(), file="thread.c", line=random.randint(10, 300)),
                ],
            )
        )
        error_summary[kind] = error_summary.get(kind, 0) + 1

    return ThreadCheckResult(
        run_id=rid,
        suite="valgrind",
        tool="helgrind",
        binary="./test_binary",
        errors=errors,
        error_summary=error_summary,
        exit_code=0,
        duration_seconds=random.uniform(0.1, 2.0),
    )


def _make_cachegrind_result(run_id: str | None = None) -> CachegrindResult:
    """Generate a CachegrindResult with randomized cache data."""
    rid = run_id or _rand_id()
    events = ["Ir", "I1mr", "ILmr", "Dr", "D1mr", "DLmr", "Dw", "D1mw", "DLmw"]
    num_lines = random.randint(5, 20)
    lines = []
    summary = {e: 0 for e in events}

    for _ in range(num_lines):
        ir = random.randint(1000, 500000)
        dr = random.randint(100, ir // 2)
        dw = random.randint(50, max(ir // 4, 51))
        vals = {
            "Ir": ir, "I1mr": random.randint(0, max(ir // 100, 1)), "ILmr": random.randint(0, max(ir // 1000, 1)),
            "Dr": dr, "D1mr": random.randint(0, max(dr // 10, 1)), "DLmr": random.randint(0, max(dr // 100, 1)),
            "Dw": dw, "D1mw": random.randint(0, max(dw // 10, 1)), "DLmw": random.randint(0, max(dw // 100, 1)),
        }
        for e in events:
            summary[e] += vals[e]
        lines.append(
            CachegrindLine(
                file=_rand_file(),
                function=_rand_fn(),
                line=random.randint(1, 500),
                ir=vals["Ir"], i1mr=vals["I1mr"], ilmr=vals["ILmr"],
                dr=vals["Dr"], d1mr=vals["D1mr"], dlmr=vals["DLmr"],
                dw=vals["Dw"], d1mw=vals["D1mw"], dlmw=vals["DLmw"],
            )
        )

    return CachegrindResult(
        run_id=rid,
        suite="valgrind",
        tool="cachegrind",
        binary="./test_binary",
        events=events,
        summary=summary,
        lines=lines,
        exit_code=0,
        duration_seconds=random.uniform(0.1, 2.0),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


async def _call_tool(name: str, arguments: dict | None = None) -> str:
    """Call an MCP tool via in-memory session and return the text result.

    Creates a fresh session per call to avoid anyio cancel-scope teardown
    issues with pytest-asyncio 1.x.
    """
    async with create_connected_server_and_client_session(mcp, raise_exceptions=True) as session:
        result = await session.call_tool(name, arguments or {})
        return result.content[0].text


async def _list_tools() -> set[str]:
    """List all tool names registered on the MCP server."""
    async with create_connected_server_and_client_session(mcp, raise_exceptions=True) as session:
        result = await session.list_tools()
        return {t.name for t in result.tools}


@pytest.fixture
def app_ctx() -> AppContext:
    """Create a standalone AppContext with a workspace for unit-level tests."""
    ctx = AppContext()
    ws = ctx.create_workspace("test")
    ctx.default_workspace_id = ws.workspace_id
    ctx.registry = ToolRegistry()
    return ctx


# ---------------------------------------------------------------------------
# Tests: Tool listing and server metadata
# ---------------------------------------------------------------------------


class TestServerMeta:
    """Verify the MCP server registers all expected tools."""

    async def test_lists_all_tools(self):
        tool_names = await _list_tools()
        expected = {
            "devtools_check", "devtools_run", "devtools_list", "devtools_raw",
            "devtools_analyze", "devtools_query", "devtools_compare",
            "devtools_search", "devtools_correlate",
            "debug_start", "debug", "debug_inspect", "debug_stop",
        }
        assert expected.issubset(tool_names), f"Missing tools: {expected - tool_names}"

    async def test_tool_count_at_least_11(self):
        tool_names = await _list_tools()
        assert len(tool_names) >= 11


# ---------------------------------------------------------------------------
# Tests: devtools_check
# ---------------------------------------------------------------------------


class TestDevtoolsCheck:
    """Test the devtools_check tool that probes for installed tools."""

    async def test_check_returns_string(self):
        text = await _call_tool("devtools_check")
        assert isinstance(text, str)
        assert len(text) > 0

    async def test_check_mentions_tool_suites(self):
        text = await _call_tool("devtools_check")
        assert any(s in text.lower() for s in ["valgrind", "lldb", "dtrace", "perf", "no tools"])


# ---------------------------------------------------------------------------
# Tests: devtools_list (empty workspace)
# ---------------------------------------------------------------------------


class TestDevtoolsList:
    """Test listing runs in a workspace."""

    async def test_list_empty_workspace(self):
        text = await _call_tool("devtools_list")
        assert "no runs" in text.lower() or "0 run" in text.lower() or "No runs" in text


# ---------------------------------------------------------------------------
# Tests: devtools_run (unavailable tool)
# ---------------------------------------------------------------------------


class TestDevtoolsRun:
    """Test running tools — we can test the unavailable path without requiring valgrind."""

    async def test_run_nonexistent_suite(self):
        text = await _call_tool("devtools_run", {
            "suite": "nonexistent",
            "tool": "fake",
            "binary": "/bin/ls",
        })
        assert "not available" in text.lower() or "unknown" in text.lower()

    async def test_run_unavailable_tool(self):
        text = await _call_tool("devtools_run", {
            "suite": "perf",
            "tool": "stat",
            "binary": "/bin/ls",
        })
        # On macOS perf won't be available
        assert isinstance(text, str) and len(text) > 0


# ---------------------------------------------------------------------------
# Tests: Analysis tools (error paths)
# ---------------------------------------------------------------------------


class TestDevtoolsAnalyze:
    """Test devtools_analyze error paths (no data injected through MCP)."""

    async def test_analyze_missing_run(self):
        text = await _call_tool("devtools_analyze", {"run_id": _rand_id()})
        assert "not found" in text.lower()

    async def test_query_schema_on_missing_run(self):
        text = await _call_tool("devtools_query", {
            "run_id": _rand_id(),
            "columns": ["schema"],
        })
        assert "not found" in text.lower()

    async def test_compare_missing_runs(self):
        text = await _call_tool("devtools_compare", {
            "run_id_a": _rand_id(),
            "run_id_b": _rand_id(),
        })
        assert "not found" in text.lower()


# ---------------------------------------------------------------------------
# Tests: Search tools (empty workspace)
# ---------------------------------------------------------------------------


class TestDevtoolsSearch:
    """Test devtools_search on empty workspaces and error paths."""

    async def test_search_empty_workspace(self):
        text = await _call_tool("devtools_search", {"query": "malloc"})
        assert "no runs" in text.lower() or "use" in text.lower()

    async def test_correlate_missing_runs(self):
        text = await _call_tool("devtools_correlate", {
            "run_id_a": _rand_id(),
            "run_id_b": _rand_id(),
        })
        assert "not found" in text.lower()


# ---------------------------------------------------------------------------
# Tests: Debug tools (error paths — no real LLDB needed)
# ---------------------------------------------------------------------------


class TestDebugTools:
    """Test debug tool error handling without a real LLDB session."""

    async def test_debug_no_session(self):
        text = await _call_tool("debug", {
            "session_id": _rand_id(),
            "action": "run",
        })
        assert "no active" in text.lower()

    async def test_debug_inspect_no_session(self):
        text = await _call_tool("debug_inspect", {
            "session_id": _rand_id(),
            "what": "backtrace",
        })
        assert "no active" in text.lower()

    async def test_debug_stop_no_session(self):
        text = await _call_tool("debug_stop", {
            "session_id": _rand_id(),
        })
        assert "no active" in text.lower()


# ---------------------------------------------------------------------------
# Tests: devtools_raw (error paths)
# ---------------------------------------------------------------------------


class TestDevtoolsRaw:
    """Test devtools_raw error handling."""

    async def test_raw_missing_run(self):
        text = await _call_tool("devtools_raw", {"run_id": _rand_id()})
        assert "not found" in text.lower() or "no raw" in text.lower()


# ---------------------------------------------------------------------------
# Unit tests: Workspace and AppContext (no MCP transport needed)
# ---------------------------------------------------------------------------


class TestWorkspaceOps:
    """Test workspace operations directly — store, retrieve, list, raw paths."""

    def test_store_and_retrieve_run(self, app_ctx):
        ws = app_ctx.get_workspace()
        result = _make_memcheck_result()
        ws.store_run(result)
        retrieved = ws.get_run(result.run_id)
        assert retrieved.run_id == result.run_id
        assert retrieved.suite == "valgrind"
        assert retrieved.tool == "memcheck"

    def test_store_with_raw_path(self, app_ctx, tmp_path):
        ws = app_ctx.get_workspace()
        result = _make_memcheck_result()
        raw_file = tmp_path / "raw.xml"
        raw_file.write_text("<valgrindoutput/>")
        ws.store_run(result, str(raw_file))
        assert ws.get_raw_path(result.run_id) == str(raw_file)

    def test_list_runs_populated(self, app_ctx):
        ws = app_ctx.get_workspace()
        n = random.randint(3, 7)
        stored_ids = set()
        for _ in range(n):
            r = _make_memcheck_result()
            ws.store_run(r)
            stored_ids.add(r.run_id)
        listing = ws.list_runs()
        assert len(listing) == n
        listed_ids = {entry["run_id"] for entry in listing}
        assert listed_ids == stored_ids

    def test_get_run_missing_raises(self, app_ctx):
        ws = app_ctx.get_workspace()
        with pytest.raises(KeyError, match="not found"):
            ws.get_run(_rand_id())

    def test_get_raw_path_missing_raises(self, app_ctx):
        ws = app_ctx.get_workspace()
        with pytest.raises(KeyError, match="not found"):
            ws.get_raw_path(_rand_id())

    def test_store_invalidates_index(self, app_ctx):
        ws = app_ctx.get_workspace()
        ws._index = pl.DataFrame({"x": [1]})
        r = _make_memcheck_result()
        ws.store_run(r)
        assert ws._index is None


class TestAppContext:
    """Test AppContext workspace management."""

    def test_create_multiple_workspaces(self):
        ctx = AppContext()
        ws1 = ctx.create_workspace("alpha")
        ws2 = ctx.create_workspace("beta")
        assert ws1.workspace_id != ws2.workspace_id
        assert ws1.name == "alpha"
        assert ws2.name == "beta"

    def test_get_workspace_missing_raises(self):
        ctx = AppContext()
        with pytest.raises(KeyError, match="not found"):
            ctx.get_workspace("nonexistent")

    def test_default_workspace(self):
        ctx = AppContext()
        ws = ctx.create_workspace("default")
        ctx.default_workspace_id = ws.workspace_id
        assert ctx.get_workspace().workspace_id == ws.workspace_id


# ---------------------------------------------------------------------------
# Unit tests: Backend registry
# ---------------------------------------------------------------------------


class TestBackendRegistry:
    """Test that all backends register correctly."""

    def test_valgrind_backend_registered(self):
        backend = get_backend("valgrind")
        assert backend.suite == "valgrind"
        assert "memcheck" in backend.tools
        assert "callgrind" in backend.tools
        assert "massif" in backend.tools

    def test_lldb_backend_registered(self):
        backend = get_backend("lldb")
        assert backend.suite == "lldb"

    def test_dtrace_backend_registered(self):
        backend = get_backend("dtrace")
        assert backend.suite == "dtrace"

    def test_perf_backend_registered(self):
        backend = get_backend("perf")
        assert backend.suite == "perf"

    def test_unknown_backend_raises(self):
        with pytest.raises(KeyError, match="Unknown suite"):
            get_backend("nonexistent")


# ---------------------------------------------------------------------------
# Unit tests: DataFrame builders via backends
# ---------------------------------------------------------------------------


class TestDataFrameBuilders:
    """Test that DataFrame builders produce valid output from generated data."""

    def test_memcheck_df_builder(self):
        backend = get_backend("valgrind")
        result = _make_memcheck_result()
        builder = backend.df_builders["memcheck"]
        df = builder(result)
        assert not df.is_empty()
        assert "kind" in df.columns
        assert len(df) == len(result.errors)

    def test_callgrind_df_builder(self):
        backend = get_backend("valgrind")
        result = _make_callgrind_result()
        builder = backend.df_builders["callgrind"]
        df = builder(result)
        assert not df.is_empty()
        assert "function" in df.columns

    def test_massif_df_builder(self):
        backend = get_backend("valgrind")
        result = _make_massif_result()
        builder = backend.df_builders["massif"]
        df = builder(result)
        assert not df.is_empty()
        assert "heap_bytes" in df.columns
        assert len(df) == len(result.snapshots)

    def test_cachegrind_df_builder(self):
        backend = get_backend("valgrind")
        result = _make_cachegrind_result()
        builder = backend.df_builders["cachegrind"]
        df = builder(result)
        assert not df.is_empty()
        assert "ir" in df.columns or "Ir" in df.columns

    def test_threadcheck_df_builder(self):
        backend = get_backend("valgrind")
        result = _make_threadcheck_result()
        builder = backend.df_builders["helgrind"]
        df = builder(result)
        assert not df.is_empty()
        assert "kind" in df.columns


# ---------------------------------------------------------------------------
# Unit tests: Format summary
# ---------------------------------------------------------------------------


class TestFormatSummary:
    """Test that format_summary produces readable output for each tool."""

    def test_memcheck_summary(self):
        backend = get_backend("valgrind")
        result = _make_memcheck_result()
        summary = backend.format_summary(result)
        assert "error" in summary.lower()
        assert result.run_id in summary

    def test_callgrind_summary(self):
        backend = get_backend("valgrind")
        result = _make_callgrind_result()
        summary = backend.format_summary(result)
        assert "hotspot" in summary.lower() or "profil" in summary.lower()

    def test_massif_summary(self):
        backend = get_backend("valgrind")
        result = _make_massif_result()
        summary = backend.format_summary(result)
        assert "snapshot" in summary.lower() or "heap" in summary.lower()

    def test_threadcheck_summary(self):
        backend = get_backend("valgrind")
        result = _make_threadcheck_result()
        summary = backend.format_summary(result)
        assert "error" in summary.lower()

    def test_cachegrind_summary(self):
        backend = get_backend("valgrind")
        result = _make_cachegrind_result()
        summary = backend.format_summary(result)
        assert isinstance(summary, str) and len(summary) > 0


# ---------------------------------------------------------------------------
# Unit tests: Unified index building and search
# ---------------------------------------------------------------------------


class TestUnifiedIndex:
    """Test the unified search index across stored runs."""

    def test_build_index_from_memcheck(self, app_ctx):
        from devtools_mcp.index import build_index

        ws = app_ctx.get_workspace()
        result = _make_memcheck_result()
        ws.store_run(result)
        index = build_index(ws)
        assert not index.is_empty()
        assert "run_id" in index.columns
        assert "suite" in index.columns
        assert "function" in index.columns

    def test_build_index_multiple_tools(self, app_ctx):
        from devtools_mcp.index import build_index

        ws = app_ctx.get_workspace()
        ws.store_run(_make_memcheck_result())
        ws.store_run(_make_callgrind_result())
        ws.store_run(_make_massif_result())
        index = build_index(ws)
        suites_in_index = index["suite"].unique().to_list()
        assert "valgrind" in suites_in_index

    def test_search_by_query(self, app_ctx):
        from devtools_mcp.index import build_index, search_index

        ws = app_ctx.get_workspace()
        result = _make_memcheck_result()
        ws.store_run(result)
        index = build_index(ws)
        # Search for any function that exists
        fn_name = result.errors[0].stack[0].fn
        found = search_index(index, query=fn_name)
        assert not found.is_empty()

    def test_search_by_suite_filter(self, app_ctx):
        from devtools_mcp.index import build_index, search_index

        ws = app_ctx.get_workspace()
        ws.store_run(_make_memcheck_result())
        ws.store_run(_make_callgrind_result())
        index = build_index(ws)
        # Filter to valgrind only — should still have rows
        found = search_index(index, suite="valgrind")
        assert not found.is_empty()
        assert all(s == "valgrind" for s in found["suite"].to_list())

    def test_search_by_kind_pattern(self, app_ctx):
        from devtools_mcp.index import build_index, search_index

        ws = app_ctx.get_workspace()
        result = _make_memcheck_result()
        ws.store_run(result)
        index = build_index(ws)
        # Search for a kind that exists
        kind = result.errors[0].kind
        found = search_index(index, kind_pattern=kind)
        assert not found.is_empty()

    def test_search_with_min_value(self, app_ctx):
        from devtools_mcp.index import build_index, search_index

        ws = app_ctx.get_workspace()
        ws.store_run(_make_callgrind_result())
        index = build_index(ws)
        # Use a very low threshold — should still find rows
        found = search_index(index, min_value=0.0)
        if not index.filter(pl.col("value").is_not_null()).is_empty():
            assert not found.is_empty()

    def test_search_empty_workspace(self, app_ctx):
        from devtools_mcp.index import build_index, search_index

        ws = app_ctx.get_workspace()
        index = build_index(ws)
        assert index.is_empty()
        found = search_index(index, query="anything")
        assert found.is_empty()

    def test_search_limit(self, app_ctx):
        from devtools_mcp.index import build_index, search_index

        ws = app_ctx.get_workspace()
        # Store enough data to have many rows
        for _ in range(5):
            ws.store_run(_make_memcheck_result(num_errors=8))
        index = build_index(ws)
        found = search_index(index, limit=3)
        assert len(found) <= 3


class TestCorrelateRuns:
    """Test cross-run correlation."""

    def test_correlate_memcheck_and_callgrind(self, app_ctx):
        from devtools_mcp.index import correlate_runs

        ws = app_ctx.get_workspace()
        # Create runs with overlapping function names
        mc = _make_memcheck_result()
        cg = _make_callgrind_result()

        # Force a shared function name
        shared_fn = "shared_function_" + _rand_id()[:8]
        mc.errors[0].stack[0].fn = shared_fn
        cg.functions[0].name = shared_fn

        ws.store_run(mc)
        ws.store_run(cg)

        result = correlate_runs(ws, mc.run_id, cg.run_id, join_on="function")
        # Should find at least the shared function
        assert not result.is_empty()
        assert "function" in result.columns

    def test_correlate_no_overlap(self, app_ctx):
        from devtools_mcp.index import correlate_runs

        ws = app_ctx.get_workspace()
        mc = _make_memcheck_result()
        cg = _make_callgrind_result()
        ws.store_run(mc)
        ws.store_run(cg)
        # With randomized names, overlap is unlikely but possible
        result = correlate_runs(ws, mc.run_id, cg.run_id, join_on="function")
        # Just verify it returns a DataFrame (may or may not be empty)
        assert isinstance(result, pl.DataFrame)


# ---------------------------------------------------------------------------
# Unit tests: ToolRegistry
# ---------------------------------------------------------------------------


class TestToolRegistry:
    """Test the ToolRegistry detection and query methods."""

    def test_is_available_false_for_undetected(self):
        reg = ToolRegistry()
        assert not reg.is_available("valgrind", "memcheck")

    def test_is_available_true_after_manual_add(self):
        reg = ToolRegistry()
        reg.tools["valgrind:memcheck"] = InstalledTool(
            suite="valgrind", name="memcheck", path="/usr/bin/valgrind", version="3.20.0", available=True
        )
        assert reg.is_available("valgrind", "memcheck")
        assert reg.is_available("valgrind")

    def test_list_available(self):
        reg = ToolRegistry()
        reg.tools["valgrind:memcheck"] = InstalledTool(
            suite="valgrind", name="memcheck", path="/usr/bin/valgrind", version="3.20.0", available=True
        )
        reg.tools["perf:stat"] = InstalledTool(
            suite="perf", name="stat", path="/usr/bin/perf", version="5.0", available=False
        )
        available = reg.list_available()
        assert len(available) == 1
        assert available[0].name == "memcheck"

    def test_format_check_empty(self):
        reg = ToolRegistry()
        output = reg.format_check()
        assert "no tools" in output.lower() or "detect" in output.lower()

    def test_format_check_with_tools(self):
        reg = ToolRegistry()
        reg.tools["valgrind:memcheck"] = InstalledTool(
            suite="valgrind", name="memcheck", path="/usr/bin/valgrind", version="3.20.0", available=True
        )
        output = reg.format_check()
        assert "valgrind" in output.lower()
        assert "memcheck" in output
        assert "3.20.0" in output


# ---------------------------------------------------------------------------
# Unit tests: Formatters
# ---------------------------------------------------------------------------


class TestFormatters:
    """Test the shared formatting utilities."""

    def test_format_dataframe_markdown_table(self):
        from devtools_mcp.formatters import format_dataframe

        df = pl.DataFrame({
            "function": [_rand_fn() for _ in range(5)],
            "cost": [random.randint(100, 10000) for _ in range(5)],
        })
        text = format_dataframe(df, title="Test Table")
        assert "Test Table" in text
        assert "|" in text
        assert "function" in text
        assert "cost" in text

    def test_format_dataframe_empty(self):
        from devtools_mcp.formatters import format_dataframe

        df = pl.DataFrame(schema={"a": pl.Utf8})
        text = format_dataframe(df, title="Empty")
        assert "no data" in text.lower()

    def test_format_dataframe_truncation(self):
        from devtools_mcp.formatters import format_dataframe

        df = pl.DataFrame({"x": list(range(100))})
        text = format_dataframe(df, max_rows=5)
        assert "5 of 100" in text

    def test_format_filtered_includes_filter_desc(self):
        from devtools_mcp.filters import build_filter_spec
        from devtools_mcp.formatters import format_filtered

        df = pl.DataFrame({"function": ["main", "foo"], "kind": ["error", "warn"]})
        spec = build_filter_spec(function_pattern="main")
        text = format_filtered(df, "Test", spec)
        assert "function" in text.lower()

    def test_human_bytes(self):
        from devtools_mcp.formatters import human_bytes

        assert "B" in human_bytes(100)
        assert "KB" in human_bytes(2048) or "KiB" in human_bytes(2048) or "K" in human_bytes(2048)


# ---------------------------------------------------------------------------
# Integration: Valgrind comparison (unit-level, no binary needed)
# ---------------------------------------------------------------------------


class TestComparison:
    """Test comparison logic for valgrind results."""

    def test_compare_memcheck_results(self):
        from devtools_mcp.valgrind.analysis.comparison import compare_memcheck

        a = _make_memcheck_result()
        b = _make_memcheck_result()
        df = compare_memcheck(a, b)
        assert isinstance(df, pl.DataFrame)
        assert not df.is_empty() or (len(a.errors) == 0 and len(b.errors) == 0)

    def test_compare_callgrind_results(self):
        from devtools_mcp.valgrind.analysis.comparison import compare_callgrind

        a = _make_callgrind_result()
        b = _make_callgrind_result()
        # Force shared function so comparison has data
        shared = "shared_" + _rand_id()[:6]
        a.functions[0].name = shared
        b.functions[0].name = shared
        df = compare_callgrind(a, b)
        assert isinstance(df, pl.DataFrame)
        delta_cols = [c for c in df.columns if "delta" in c]
        assert len(delta_cols) > 0

    def test_compare_massif_results(self):
        from devtools_mcp.valgrind.analysis.comparison import compare_massif

        a = _make_massif_result()
        b = _make_massif_result()
        info = compare_massif(a, b)
        assert isinstance(info, dict)
        assert "peak_bytes_a" in info or "peak_a" in info or len(info) > 0
