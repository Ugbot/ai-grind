"""Tests for the RenderDoc suite: bridge JSON parsing, frame builders, stacks,
formatters. All data is synthetic bridge-payload JSON; the real tool is never
needed (same approach as the ETW/JVM/CDB/VTune suites)."""

from __future__ import annotations

import json
import time

import polars as pl
import pytest

from devtools_mcp.models import create_run_base
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
