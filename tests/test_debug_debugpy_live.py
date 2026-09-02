"""Live integration test: DapSession driving a real debugpy adapter.

Skipped entirely unless debugpy is importable by the interpreter running
the tests (the adapter and the debuggee share it. See adapters/debugpy.py).
"""

from __future__ import annotations

import importlib.util
import sys
import textwrap

import pytest

debugpy_missing = importlib.util.find_spec("debugpy") is None
pytestmark = pytest.mark.skipif(debugpy_missing, reason="debugpy not importable by the test interpreter")

_TIMEOUT = 30.0

# Line 6 is `marker = total * 2`, the conditional breakpoint target.
_SCRIPT = textwrap.dedent("""\
    def loop() -> int:
        total = 0
        marker = -1
        for i in range(6):
            total = total + i
            marker = total * 2  # breakpoint here (line 6)
        return total


    result = loop()
    print("result:", result)
    """)
_BREAK_LINE = 6


async def test_debugpy_conditional_breakpoint_step_and_set_variable(tmp_path):
    from devtools_mcp.debug.adapters import get_adapter
    from devtools_mcp.debug.dap_session import DapSession
    from devtools_mcp.debug.models import BreakpointSpec, LaunchConfig, SessionState
    from devtools_mcp.debug.session import DebugSessionManager

    script = tmp_path / "loopy.py"
    script.write_text(_SCRIPT)

    manager = DebugSessionManager()
    session = DapSession("live-debugpy", get_adapter("debugpy"), manager)
    session.node = manager.register_root(session)
    try:
        assert session.add_watch("i * 10") is None
        # Conditional breakpoint set BEFORE launch, applied (and its
        # condition honoured) during the initialized-event dance.
        await session.set_breakpoints(
            str(script),
            [BreakpointSpec(source=str(script), line=_BREAK_LINE, condition="i == 3")],
        )
        await session.launch(
            LaunchConfig(
                program=str(script),
                cwd=str(tmp_path),
                stop_on_entry=False,
                extra={"python": sys.executable},
            )
        )
        assert session.capabilities.conditional_breakpoints

        # --- first stop: the condition fired on iteration i == 3 ---------
        state = await session.wait_until({SessionState.stopped, SessionState.terminated}, timeout=_TIMEOUT)
        assert (
            state == SessionState.stopped
        ), f"expected a breakpoint stop, got {state}; output:\n{session.output_tail()}"
        snap1 = session.last_snapshot
        assert snap1 is not None
        assert snap1.stop_reason == "breakpoint"
        confirmed = session.breakpoints[str(script)]
        assert confirmed and confirmed[0].verified

        locals1 = {v.path: v.value for v in snap1.variables if v.scope in ("locals", "local")}
        assert locals1.get("i") == "3", f"locals at stop: {locals1}"
        assert locals1.get("total") == "6"
        assert locals1.get("marker") == "6"  # still from the i == 2 iteration

        watch = next(w for w in snap1.watches if w.expression == "i * 10")
        assert not watch.error
        assert watch.value == "30"
        assert snap1.changes == []  # nothing to diff against on the first stop

        # --- step over the marker assignment: changes must be non-empty --
        await session.step("over")
        state = await session.wait_until({SessionState.stopped}, timeout=_TIMEOUT)
        assert state == SessionState.stopped
        snap2 = session.last_snapshot
        assert snap2 is not None and snap2.stop_seq == snap1.stop_seq + 1
        assert snap2.stop_reason == "step"
        assert snap2.changes, "second stop should report variable deltas"
        changed = {c.path: c for c in snap2.changes if c.kind == "changed"}
        assert "marker" in changed, f"changes: {[c.path for c in snap2.changes]}"
        assert changed["marker"].old == "6"
        assert changed["marker"].new == "12"

        # --- setVariable via a live threads → frames → scopes walk --------
        assert session.capabilities.set_variable
        thread_id = session.last_stop.thread_id
        assert thread_id is not None
        frames = await session.stack_trace(thread_id)
        assert frames and frames[0].function
        scopes = await session.scopes(frames[0].id)
        locals_scope = next(s for s in scopes if "local" in s.name.lower())
        variables = await session.variables(locals_scope.ref)
        assert any(v.name == "total" for v in variables)
        updated = await session.set_variable(locals_scope.ref, "total", "100")
        assert updated.value == "100"
        check = await session.evaluate("total", frame_id=frames[0].id)
        assert check.value == "100" and not check.error

        # --- run to completion --------------------------------------------
        await session.continue_()
        state = await session.wait_until({SessionState.terminated}, timeout=_TIMEOUT)
        assert state == SessionState.terminated, f"output:\n{session.output_tail()}"
    finally:
        await session.disconnect(terminate=True)
