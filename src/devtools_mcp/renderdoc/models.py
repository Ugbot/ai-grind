"""RenderDoc result models.

The replay verbs (analyze/counters/resources) parse the bridge JSON emitted by
scripts/bridge.py running inside qrenderdoc's embedded interpreter; capture and
thumb wrap renderdoccmd / target-control output.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase


class RdcAction(BaseModel):
    """One node of the frame's action (drawcall/dispatch/marker) tree."""

    event_id: int = 0
    action_id: int = 0
    parent_event_id: int = 0  # 0 = root
    depth: int = 0
    name: str = ""
    flags: str = ""  # "Drawcall|Indexed", "Dispatch", "PushMarker", ...
    num_indices: int = 0
    num_instances: int = 0
    dispatch: list[int] = Field(default_factory=lambda: [0, 0, 0])
    duration_us: float | None = None  # merged from GPU Duration counter


class RdcResource(BaseModel):
    """One GPU resource (texture/buffer/shader/...) referenced by the capture."""

    resource_id: str = ""
    name: str = ""
    type: str = ""  # "Texture2D", "Buffer", ...
    width: int = 0
    height: int = 0
    depth: int = 0
    mips: int = 0
    format: str = ""
    bytes: int = 0


class RdcCounter(BaseModel):
    """One GPU counter sample for one event."""

    event_id: int = 0
    counter: str = ""
    unit: str = ""
    value: float = 0.0


class RenderdocReplayResult(RunBase):
    """Replay analysis of an .rdc (tools: analyze, counters, resources)."""

    suite: str = "renderdoc"
    rdc_path: str = ""
    api: str = ""  # "Vulkan", "D3D11", ...
    frame_number: int = 0
    actions: list[RdcAction] = Field(default_factory=list)
    resources: list[RdcResource] = Field(default_factory=list)
    counters: list[RdcCounter] = Field(default_factory=list)
    stats: dict[str, int] = Field(default_factory=dict)  # draws/dispatches/copies/markers
    truncated: bool = False  # bridge hit the max-actions bound


class RenderdocCaptureResult(RunBase):
    """A capture run: app launched under RenderDoc, .rdc file(s) produced."""

    suite: str = "renderdoc"
    mode: str = "targetcontrol"  # or "launch-wait"
    rdc_paths: list[str] = Field(default_factory=list)
    frame_captured: int | None = None
    app_exit_code: int | None = None
    capture_log: str = ""  # bounded tail of launcher output


class RenderdocThumbResult(RunBase):
    """Thumbnail extraction from an .rdc via renderdoccmd thumb."""

    suite: str = "renderdoc"
    rdc_path: str = ""
    thumb_path: str = ""
    width: int = 0
    height: int = 0
