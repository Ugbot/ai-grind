"""Tests for JVM parsers: JFR JSON, jstack dumps, class histograms."""

from __future__ import annotations

import json
import random

from devtools_mcp.jvm.analysis import jvm_heap_df, jvm_hotspots_df, jvm_threads_df
from devtools_mcp.jvm.models import JvmResult
from devtools_mcp.jvm.parsers import parse_class_histogram, parse_jfr_json, parse_jstack


def _jfr_event(methods: list[tuple[str, str]]) -> dict:
    frames = [{"method": {"type": {"name": cls}, "name": name}, "lineNumber": 1} for cls, name in methods]
    return {"type": "jdk.ExecutionSample", "values": {"stackTrace": {"frames": frames}}}


def _jfr_json(n: int = 20) -> str:
    classes = ["com.app.Service", "com.app.Repo", "java.util.HashMap", "java.lang.String"]
    methods = ["process", "query", "get", "hashCode", "equals"]
    events = []
    for _ in range(n):
        depth = random.randint(1, 5)
        stack = [(random.choice(classes), random.choice(methods)) for _ in range(depth)]
        events.append(_jfr_event(stack))
    # a non-sample event that must be counted but not turned into a stack
    events.append({"type": "jdk.GCPhasePause", "values": {}})
    return json.dumps({"recording": {"events": events}})


class TestJfr:
    def test_parses_samples(self):
        samples, counts = parse_jfr_json(_jfr_json(15))
        assert sum(s.weight for s in samples) == 15  # 15 execution samples
        assert counts.get("jdk.ExecutionSample") == 15
        assert counts.get("jdk.GCPhasePause") == 1

    def test_frames_root_first(self):
        text = json.dumps({"events": [_jfr_event([("A", "leaf"), ("B", "mid"), ("C", "root")])]})
        samples, _ = parse_jfr_json(text)
        assert samples[0].frames[0] == "C.root"  # reversed to root-first
        assert samples[0].frames[-1] == "A.leaf"

    def test_aggregates_identical_stacks(self):
        ev = _jfr_event([("A", "a"), ("B", "b")])
        text = json.dumps({"events": [ev, ev, ev]})
        samples, _ = parse_jfr_json(text)
        assert len(samples) == 1
        assert samples[0].weight == 3

    def test_bad_json(self):
        assert parse_jfr_json("not json") == ([], {})

    def test_hotspots_df(self):
        samples, _ = parse_jfr_json(_jfr_json(20))
        df = jvm_hotspots_df(JvmResult(run_id="r", tool="jfr", binary="1", stack_samples=samples))
        assert "function" in df.columns and "value" in df.columns
        assert df.height > 0


class TestJstack:
    SAMPLE = '''\
"main" #1 prio=5 os_prio=0 cpu=10.5ms tid=0x00007f nid=0x1a03 runnable [0x00007ffe]
   java.lang.Thread.State: RUNNABLE
\tat com.app.Main.loop(Main.java:42)
\tat com.app.Main.main(Main.java:10)

"worker-1" #12 daemon prio=5 tid=0x00007a nid=0x2b04 waiting on condition [0x00007ffd]
   java.lang.Thread.State: WAITING (parking)
\tat jdk.internal.misc.Unsafe.park(Native Method)
\tat java.util.concurrent.locks.LockSupport.park(LockSupport.java:341)
'''

    def test_parses_threads(self):
        threads, deadlock = parse_jstack(self.SAMPLE)
        assert len(threads) == 2
        assert not deadlock

    def test_thread_fields(self):
        threads, _ = parse_jstack(self.SAMPLE)
        main = threads[0]
        assert main.name == "main"
        assert main.state == "RUNNABLE"
        assert main.frames[0].startswith("com.app.Main.loop")
        worker = threads[1]
        assert worker.daemon
        assert worker.state == "WAITING"

    def test_deadlock_detection(self):
        threads, deadlock = parse_jstack("Found one Java-level deadlock:\n" + self.SAMPLE)
        assert deadlock

    def test_threads_df(self):
        threads, _ = parse_jstack(self.SAMPLE)
        df = jvm_threads_df(JvmResult(run_id="r", tool="threads", binary="1", threads=threads))
        assert df.height == 2
        assert "state" in df.columns


class TestHeapHistogram:
    SAMPLE = '''\
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:        524288       33554432  [B (java.base@22)
   2:        131072        4194304  java.lang.String (java.base@22)
   3:         65536        2097152  java.util.HashMap$Node (java.base@22)
Total       720896       39845888
'''

    def test_parses_classes(self):
        classes, total = parse_class_histogram(self.SAMPLE)
        assert len(classes) == 3
        assert total == 39845888

    def test_class_fields(self):
        classes, _ = parse_class_histogram(self.SAMPLE)
        assert classes[0].instances == 524288
        assert classes[0].bytes == 33554432
        assert "[B" in classes[0].class_name

    def test_heap_df_sorted_and_aliased(self):
        classes, _ = parse_class_histogram(self.SAMPLE)
        df = jvm_heap_df(JvmResult(run_id="r", tool="heap", binary="1", heap_classes=classes))
        assert df["bytes"][0] == 33554432  # sorted desc
        assert "function" in df.columns

    def test_empty(self):
        classes, total = parse_class_histogram("")
        assert classes == [] and total == 0
