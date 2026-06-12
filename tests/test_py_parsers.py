"""Tests for the Python backend: pstats parsing, py-spy dump, analysis."""

from __future__ import annotations

import cProfile
import os
import tempfile

from devtools_mcp.flamegraph.fold import parse_folded
from devtools_mcp.py.analysis import py_funcstats_df, py_hotspots_df, py_threads_df
from devtools_mcp.py.models import PyResult
from devtools_mcp.py.parsers import parse_pstats, parse_pyspy_dump


def _make_prof() -> str:
    """Generate a real cProfile .prof so we parse the actual binary format."""

    def inner(n):
        return sum(i * i for i in range(n))

    def outer():
        return sum(inner(500) for _ in range(50))

    fd, path = tempfile.mkstemp(suffix=".prof")
    os.close(fd)
    pr = cProfile.Profile()
    pr.enable()
    outer()
    pr.disable()
    pr.dump_stats(path)
    return path


class TestPstats:
    def test_parses_real_prof(self):
        path = _make_prof()
        try:
            rows = parse_pstats(path)
            assert rows
            assert any("inner" in r.function for r in rows)
            assert all(r.cumtime >= 0 for r in rows)
            # sorted by cumtime desc
            assert rows == sorted(rows, key=lambda r: r.cumtime, reverse=True)
        finally:
            os.unlink(path)

    def test_funcstats_df(self):
        path = _make_prof()
        try:
            df = py_funcstats_df(PyResult(run_id="r", tool="cprofile", binary="x.py", func_stats=parse_pstats(path)))
            assert "function" in df.columns and "value" in df.columns
            assert df.height > 0
        finally:
            os.unlink(path)


class TestPyspyDump:
    SAMPLE = """\
Process 4242: python app.py
Python v3.12.0 (/usr/bin/python3.12)

Thread 4242 (active): "MainThread"
    do_work (app.py:12)
    handler (app.py:30)
    <module> (app.py:50)
Thread 4243 (idle): "worker-1"
    wait (queue.py:171)
    run (threading.py:982)
"""

    def test_parses_threads(self):
        threads = parse_pyspy_dump(self.SAMPLE)
        assert len(threads) == 2
        assert threads[0].name == "MainThread"
        assert threads[0].state == "active"
        assert threads[0].frames[0].startswith("do_work (app.py:12)")
        assert threads[1].state == "idle"

    def test_threads_df(self):
        df = py_threads_df(PyResult(run_id="r", tool="threads", binary="4242", threads=parse_pyspy_dump(self.SAMPLE)))
        assert df.height == 2
        assert "state" in df.columns

    def test_empty(self):
        assert parse_pyspy_dump("") == []


class TestPyspyRecord:
    def test_folded_to_hotspots(self):
        folded = "app.py:<module>;app.py:main;app.py:work 80\napp.py:<module>;app.py:main;lib.py:io 20"
        samples = parse_folded(folded)
        df = py_hotspots_df(PyResult(run_id="r", tool="cpu", binary="1", stack_samples=samples))
        assert df.height > 0
        assert "function" in df.columns
        assert df["exclusive"].sum() == 100
