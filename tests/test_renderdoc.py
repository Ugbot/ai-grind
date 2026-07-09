"""Tests for the RenderDoc suite: bridge JSON parsing, frame builders, stacks,
formatters. All data is synthetic bridge-payload JSON; the real tool is never
needed (same approach as the ETW/JVM/CDB/VTune suites)."""

from __future__ import annotations

import json
import py_compile
import struct
import time
from pathlib import Path

import polars as pl
import pytest

from devtools_mcp.models import create_run_base
from devtools_mcp.registry import get_backend
from devtools_mcp.renderdoc.analysis import (
    rdoc_actions_df,
    rdoc_capture_df,
    rdoc_counters_df,
    rdoc_resources_df,
    rdoc_stack_samples,
    rdoc_thumb_df,
)
from devtools_mcp.renderdoc.formatters import format_renderdoc_summary
from devtools_mcp.renderdoc.models import (
    RenderdocCaptureResult,
    RenderdocReplayResult,
    RenderdocThumbResult,
)
from devtools_mcp.renderdoc.parsers import (
    bridge_to_replay_result,
    classify_bridge_error,
    find_new_rdcs,
    parse_bridge_json,
    parse_renderdoccmd_version,
)
from devtools_mcp.renderdoc.runner import _opt, _png_dimensions, run_renderdoc

BRIDGE_OK = {
    "schema_version": 1,
    "ok": True,
    "op": "replay",
    "api": "Vulkan",
    "frame_number": 120,
    "truncated": False,
    "actions": [
        {"eid": 1, "aid": 1, "parent_eid": 0, "depth": 0, "name": "Scene", "flags": "PushMarker"},
        {
            "eid": 2,
            "aid": 2,
            "parent_eid": 1,
            "depth": 1,
            "name": "vkCmdDrawIndexed(300)",
            "flags": "Drawcall|Indexed",
            "num_indices": 300,
            "num_instances": 2,
        },
        {
            "eid": 3,
            "aid": 3,
            "parent_eid": 1,
            "depth": 1,
            "name": "vkCmdDispatch(8,8,1)",
            "flags": "Dispatch",
            "dispatch": [8, 8, 1],
        },
        {"eid": 4, "aid": 4, "parent_eid": 0, "depth": 0, "name": "Present", "flags": "Present"},
    ],
    "resources": [
        {"id": "77", "name": "gbuffer0", "type": "Texture2D", "width": 1920, "height": 1080, "bytes": 8294400},
        {"id": "78", "name": "vbuf", "type": "Buffer", "bytes": 65536},
    ],
    "counters": [
        {"eid": 2, "counter": "GPU Duration", "unit": "us", "value": 42.5},
        {"eid": 3, "counter": "GPU Duration", "unit": "us", "value": 7.25},
    ],
    "stats": {"draws": 1, "dispatches": 1, "copies": 0, "markers": 1},
}

BRIDGE_ERR_GPU = {
    "schema_version": 1,
    "ok": False,
    "error": "Couldn't initialise replay device: no compatible GPU found",
    "stage": "replay",
}


def _replay(tool: str = "counters") -> RenderdocReplayResult:
    return bridge_to_replay_result(BRIDGE_OK, tool=tool, rdc_path="frame.rdc", duration_seconds=3.5)


class TestParseBridgeJson:
    def test_roundtrip(self):
        payload = parse_bridge_json(json.dumps(BRIDGE_OK))
        assert payload["ok"] is True
        assert payload["api"] == "Vulkan"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            parse_bridge_json("")

    def test_non_json_raises(self):
        with pytest.raises(ValueError, match="not JSON"):
            parse_bridge_json("qrenderdoc crashed\n")

    def test_wrong_schema_version_raises(self):
        with pytest.raises(ValueError, match="schema_version"):
            parse_bridge_json(json.dumps({"schema_version": 2, "ok": True}))

    def test_non_object_raises(self):
        with pytest.raises(ValueError, match="not an object"):
            parse_bridge_json("[1, 2]")


class TestClassifyBridgeError:
    def test_gpu_failure_mentions_session(self):
        msg = classify_bridge_error(BRIDGE_ERR_GPU)
        assert "GPU" in msg
        assert "session" in msg

    def test_open_failure_mentions_path(self):
        msg = classify_bridge_error({"ok": False, "error": "failed to open capture", "stage": "open"})
        assert ".rdc" in msg

    def test_version_mismatch(self):
        msg = classify_bridge_error({"ok": False, "error": "capture is incompatible", "stage": "open"})
        assert "version" in msg.lower()

    def test_no_frames(self):
        msg = classify_bridge_error({"ok": False, "error": "no frames captured", "stage": "capture"})
        assert "F12" in msg

    def test_unknown_falls_back_to_raw_hint(self):
        msg = classify_bridge_error({"ok": False, "error": "???", "stage": "x"}, stderr_tail="boom")
        assert "devtools_raw" in msg


class TestBridgeToReplayResult:
    def test_actions_mapped(self):
        result = _replay()
        assert len(result.actions) == 4
        draw = result.actions[1]
        assert draw.event_id == 2
        assert draw.num_indices == 300
        assert draw.flags == "Drawcall|Indexed"
        assert result.stats["draws"] == 1

    def test_durations_merged_by_event_id(self):
        result = _replay()
        assert result.actions[1].duration_us == 42.5
        assert result.actions[2].duration_us == 7.25
        assert result.actions[0].duration_us is None  # marker: no counter sample

    def test_resources_and_counters(self):
        result = _replay()
        assert result.resources[0].bytes == 8294400
        assert len(result.counters) == 2

    def test_bad_tool_asserts(self):
        with pytest.raises(AssertionError):
            bridge_to_replay_result(BRIDGE_OK, tool="capture", rdc_path="x.rdc")


class TestFrameBuilders:
    def test_actions_df_values(self):
        df = rdoc_actions_df(_replay())
        assert df.height == 4
        assert "function" in df.columns
        timed = df.filter(pl.col("event_id") == 2)
        assert timed["value"][0] == 42.5
        untimed = df.filter(pl.col("event_id") == 1)
        assert untimed["value"][0] == 0.0  # marker: no duration, 0 indices

    def test_actions_df_empty_schema(self):
        base = create_run_base(suite="renderdoc", tool="analyze", binary="x.rdc")
        empty = RenderdocReplayResult(**base.model_dump(), rdc_path="x.rdc")
        df = rdoc_actions_df(empty)
        assert df.height == 0
        assert df.schema["value"] == pl.Float64
        assert df.schema["function"] == pl.Utf8

    def test_resources_df(self):
        df = rdoc_resources_df(_replay())
        assert df.height == 2
        assert df.sort("value", descending=True)["function"][0] == "gbuffer0"

    def test_counters_df_joins_action_names(self):
        df = rdoc_counters_df(_replay())
        assert df.height == 2
        assert df.filter(pl.col("event_id") == 2)["function"][0] == "vkCmdDrawIndexed(300)"

    def test_empty_counter_and_resource_schemas(self):
        base = create_run_base(suite="renderdoc", tool="counters", binary="x.rdc")
        empty = RenderdocReplayResult(**base.model_dump(), rdc_path="x.rdc")
        assert rdoc_counters_df(empty).height == 0
        assert rdoc_resources_df(empty).height == 0

    def test_capture_df(self, tmp_path):
        rdc = tmp_path / "app_frame120.rdc"
        rdc.write_bytes(b"\x00" * 128)
        base = create_run_base(suite="renderdoc", tool="capture", binary="app.exe")
        result = RenderdocCaptureResult(**base.model_dump(), rdc_paths=[str(rdc)], frame_captured=120)
        df = rdoc_capture_df(result)
        assert df.height == 1
        assert df["bytes"][0] == 128
        assert df["frame"][0] == 120

    def test_thumb_df(self):
        base = create_run_base(suite="renderdoc", tool="thumb", binary="x.rdc")
        result = RenderdocThumbResult(**base.model_dump(), rdc_path="x.rdc", thumb_path="t.png", width=320, height=180)
        df = rdoc_thumb_df(result)
        assert df.height == 1
        assert df["value"][0] == 320.0 * 180.0


class TestStackSamples:
    def test_leaves_only_root_first(self):
        samples = rdoc_stack_samples(_replay())
        # Leaves: the draw, the dispatch, and Present (Scene is a parent).
        assert len(samples) == 3
        draw = next(s for s in samples if "vkCmdDrawIndexed" in s.frames[-1])
        assert draw.frames == ["Scene", "vkCmdDrawIndexed(300)"]
        assert draw.weight == 42

    def test_weight_defaults_to_one_without_counters(self):
        payload = dict(BRIDGE_OK, counters=[])
        result = bridge_to_replay_result(payload, tool="analyze", rdc_path="x.rdc")
        samples = rdoc_stack_samples(result)
        assert all(s.weight == 1 for s in samples)

    def test_non_replay_result_returns_empty(self):
        base = create_run_base(suite="renderdoc", tool="capture", binary="app.exe")
        result = RenderdocCaptureResult(**base.model_dump())
        assert rdoc_stack_samples(result) == []


class TestFormatters:
    def test_replay_summary_bounded_with_hints(self):
        text = format_renderdoc_summary(_replay())
        assert text.count("\n") < 80
        assert "Vulkan" in text
        assert "devtools_flamegraph" in text
        assert "devtools_analyze" in text

    def test_analyze_without_timings_hints_counters(self):
        payload = dict(BRIDGE_OK, counters=[])
        result = bridge_to_replay_result(payload, tool="analyze", rdc_path="frame.rdc")
        text = format_renderdoc_summary(result)
        assert 'tool="counters"' in text

    def test_capture_summary_next_step(self, tmp_path):
        rdc = tmp_path / "cap.rdc"
        rdc.write_bytes(b"\x00" * 64)
        base = create_run_base(suite="renderdoc", tool="capture", binary="app.exe")
        result = RenderdocCaptureResult(**base.model_dump(), rdc_paths=[str(rdc)], frame_captured=7)
        text = format_renderdoc_summary(result)
        assert 'tool="analyze"' in text
        assert "cap.rdc" in text

    def test_capture_summary_no_captures_explains(self):
        base = create_run_base(suite="renderdoc", tool="capture", binary="app.exe")
        result = RenderdocCaptureResult(**base.model_dump(), mode="launch-wait")
        text = format_renderdoc_summary(result)
        assert "F12" in text


class TestSmallParsers:
    def test_version(self):
        out = "renderdoccmd x64 v1.45 built from 2fc0bc04cb95499635f63986a55bc6f67849dd9f"
        assert parse_renderdoccmd_version(out) == "1.45"

    def test_version_missing(self):
        assert parse_renderdoccmd_version("garbage") == ""

    def test_find_new_rdcs(self, tmp_path):
        old = tmp_path / "old.rdc"
        old.write_bytes(b"0")
        cutoff = time.time() + 5  # everything so far is "old"
        assert find_new_rdcs(tmp_path, cutoff) == []
        assert find_new_rdcs(tmp_path, 0) == [old]

    def test_find_new_rdcs_missing_dir(self, tmp_path):
        assert find_new_rdcs(tmp_path / "nope", 0) == []


# --- Runner + bridge script ---

BRIDGE_PATH = Path(__file__).parent.parent / "src" / "devtools_mcp" / "renderdoc" / "scripts" / "bridge.py"


def _bridge_namespace() -> dict:
    """Exec bridge.py with the trailing main() call stripped (it sys.exit()s)."""
    source = BRIDGE_PATH.read_text(encoding="utf-8")
    assert source.rstrip().endswith("main()"), "bridge.py must self-run for qrenderdoc"
    ns: dict = {}
    exec(compile(source.rstrip()[: -len("main()")], str(BRIDGE_PATH), "exec"), ns)  # noqa: S102
    return ns


class TestBridgeScript:
    def test_compiles_as_py36(self):
        # py_compile under the current interpreter catches syntax errors; the
        # conservative-syntax rule (no walrus/match/X|Y) is enforced by review.
        py_compile.compile(str(BRIDGE_PATH), doraise=True)

    def test_flag_names(self):
        ns = _bridge_namespace()
        assert ns["flag_names"]("ActionFlags.Drawcall | ActionFlags.Indexed") == "Drawcall|Indexed"
        assert ns["flag_names"]("ActionFlags.NoFlags") == ""

    def test_flatten_actions_with_stub(self):
        ns = _bridge_namespace()

        class Action:
            def __init__(self, eid, name, children=()):
                self.eventId = eid
                self.actionId = eid
                self.flags = "ActionFlags.Drawcall"
                self.numIndices = 3
                self.numInstances = 1
                self.dispatchDimension = [0, 0, 0]
                self.children = list(children)
                self._name = name

            def GetName(self, sdfile):
                return self._name

        roots = [Action(1, "Scene", children=[Action(2, "draw_a"), Action(3, "draw_b")])]
        actions, truncated = ns["flatten_actions"](roots, None, max_actions=100)
        assert [a["eid"] for a in actions] == [1, 2, 3]
        assert actions[1]["parent_eid"] == 1
        assert actions[1]["depth"] == 1
        assert not truncated

    def test_flatten_actions_truncates(self):
        ns = _bridge_namespace()

        class Leaf:
            def __init__(self, eid):
                self.eventId = eid
                self.actionId = eid
                self.flags = ""
                self.numIndices = 0
                self.numInstances = 0
                self.dispatchDimension = [0, 0, 0]
                self.children = []

            def GetName(self, sdfile):
                return f"a{self.eventId}"

        actions, truncated = ns["flatten_actions"]([Leaf(i) for i in range(10)], None, max_actions=4)
        assert len(actions) == 4
        assert truncated

    def test_action_stats(self):
        ns = _bridge_namespace()
        stats = ns["action_stats"](
            [
                {"flags": "Drawcall|Indexed"},
                {"flags": "Dispatch"},
                {"flags": "Copy"},
                {"flags": "PushMarker"},
                {"flags": "Present"},
            ]
        )
        assert stats == {"draws": 1, "dispatches": 1, "copies": 1, "markers": 1}

    def test_read_request_rejects_bad_op(self, tmp_path, monkeypatch):
        ns = _bridge_namespace()
        req = tmp_path / "request.json"
        req.write_text(json.dumps({"schema_version": 1, "op": "explode"}))
        monkeypatch.setenv("DEVTOOLS_RDOC_REQUEST", str(req))
        with pytest.raises(RuntimeError, match="unknown op"):
            ns["read_request"]()

    def test_write_output_stamps_schema(self, tmp_path, monkeypatch):
        ns = _bridge_namespace()
        out = tmp_path / "output.json"
        monkeypatch.setenv("DEVTOOLS_RDOC_OUTPUT", str(out))
        ns["write_output"]({"ok": True})
        assert json.loads(out.read_text())["schema_version"] == 1


class TestRunner:
    async def test_unknown_tool(self):
        err, result, _ = await run_renderdoc(tool="explode", binary="x")
        assert result is None
        assert "Unknown renderdoc tool" in err

    async def test_missing_target(self):
        err, result, _ = await run_renderdoc(tool="analyze", binary="Z:/no/such/file.rdc")
        assert result is None
        assert "not found" in err

    async def test_replay_verb_rejects_exe(self, tmp_path):
        exe = tmp_path / "app.exe"
        exe.write_bytes(b"MZ")
        err, result, _ = await run_renderdoc(tool="analyze", binary=str(exe))
        assert result is None
        assert ".rdc" in err and "capture" in err

    async def test_capture_rejects_rdc(self, tmp_path):
        rdc = tmp_path / "frame.rdc"
        rdc.write_bytes(b"\x00")
        err, result, _ = await run_renderdoc(tool="capture", binary=str(rdc))
        assert result is None
        assert "analyze" in err

    async def test_capture_bad_mode(self, tmp_path):
        exe = tmp_path / "app.exe"
        exe.write_bytes(b"MZ")
        err, result, _ = await run_renderdoc(tool="capture", binary=str(exe), extra_args=["--mode", "psychic"])
        assert result is None
        assert "--mode" in err

    def test_opt_parsing(self):
        assert _opt(["--frame", "120", "--warmup", "5"], "--frame") == "120"
        assert _opt(["--frame"], "--frame") is None
        assert _opt(None, "--frame") is None

    def test_png_dimensions(self, tmp_path):
        png = tmp_path / "t.png"
        header = b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + struct.pack(">II", 320, 180) + b"\x00" * 5
        png.write_bytes(header)
        assert _png_dimensions(str(png)) == (320, 180)
        assert _png_dimensions(str(tmp_path / "missing.png")) == (0, 0)


class TestRegistration:
    def test_backend_registered_with_capabilities(self):
        import devtools_mcp.renderdoc.backend  # noqa: F401  (registers the suite)

        spec = get_backend("renderdoc")
        assert set(spec.tools) == {"capture", "analyze", "counters", "resources", "thumb"}
        assert spec.stacks is not None
        assert spec.install is not None
        assert "_default" in spec.df_builders
        caps = spec.capabilities()
        assert {"flamegraph", "install"} <= caps

    def test_install_spec_covers_windows_and_linux(self):
        import devtools_mcp.renderdoc.backend as backend_mod

        spec = backend_mod.RENDERDOC_INSTALL
        assert "windows" in spec.platforms
        assert "linux" in spec.platforms
        assert spec.platforms["windows"][0].argv[0] == "winget"
