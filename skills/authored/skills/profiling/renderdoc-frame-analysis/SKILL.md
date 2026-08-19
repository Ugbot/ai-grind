---
name: renderdoc-frame-analysis
description: >
  Capture and analyze GPU frames via the devtools-mcp renderdoc backend
  (RenderDoc). Use when a graphics app (D3D/Vulkan/OpenGL) renders slowly or
  wrongly and you need the per-drawcall event tree, GPU timings per draw, the
  resource inventory, a frame thumbnail, or a GPU-time flame graph, captured
  headlessly, no RenderDoc GUI required. Covers the capture→analyze→counters
  workflow, the two capture modes, and the GPU/interactive-session prerequisite.
---

# GPU frame capture + replay analysis (devtools-mcp `renderdoc`)

The `renderdoc` backend drives **RenderDoc**: capture one frame of a running
graphics app into an `.rdc`, then replay it headlessly for the action tree,
GPU counter timings, and resources, all as bounded summaries plus queryable
Polars frames. The GUI (`qrenderdoc`) is never opened; replay runs through
`qrenderdoc --python` under the hood.

## Install

```
devtools_install(suite="renderdoc")          # shows the exact commands
winget install --id BaldurKarlsson.RenderDoc -e    # Windows
apt install renderdoc                              # Debian/Ubuntu
```

Non-default install location: set `$DEVTOOLS_RENDERDOCCMD` / `$DEVTOOLS_QRENDERDOC`.

## Prerequisites

- **Replay needs a GPU + interactive session.** analyze/counters/resources fail
  under session-0 services and headless containers, so run the MCP server in your
  user session for this suite. `thumb` needs no GPU.
- The capture and the installed RenderDoc should be the same version
  (a mismatch fails with "capture unsupported, recapture").
- UAC-elevated targets can't be injected.

## Capture

```
devtools_run(suite="renderdoc", tool="capture", binary="C:/path/app.exe", args=[...])
```

Default mode **targetcontrol**: launches + injects the app, waits `--warmup`
seconds (default 3), auto-triggers a capture of the next frame, and terminates
the app. `extra_args`:
- `--frame N` queues an exact frame number instead of "next frame".
- `--warmup S` and `--max-wait S` set the steady-state delay and the capture deadline.
- `--out DIR` is where the `.rdc` lands (default temp dir).
- `--mode launch-wait` is the fallback, plain `renderdoccmd capture`, where you trigger
  with F12/PrintScreen in-app (or the in-application RENDERDOC_API). Use for
  interactive sessions or when injection is blocked.

The summary prints the produced `.rdc` path and the exact analyze call.

## Analyze the .rdc

```
devtools_run(suite="renderdoc", tool="analyze",   binary="<file>.rdc")   # action tree, fast
devtools_run(suite="renderdoc", tool="counters",  binary="<file>.rdc")  # + GPU µs per action
devtools_run(suite="renderdoc", tool="resources", binary="<file>.rdc")  # textures/buffers by size
devtools_run(suite="renderdoc", tool="thumb",     binary="<file>.rdc")  # PNG of the frame
```

- `counters` replays the frame per counter pass, which takes minutes on big captures;
  it defaults to a 600s timeout, raise `timeout` if needed. Extra counters:
  `extra_args=["--counter", "SamplesPassed"]`.
- Huge frames: `--max-actions N` bounds the action walk (default 50k, summary
  says TRUNCATED when hit).

Then drill in without flooding context:

```
devtools_analyze(run_id, function_pattern="Draw", sort_by="duration_us")  # slow draws
devtools_analyze(run_id, group_by="flags")                                # cost by action type
devtools_flamegraph(run_id)          # GPU-time flame graph of the marker hierarchy
```

The actions frame: `event_id, parent_event_id, depth, function` (action name),
`flags, indices, instances, dispatch_x/y/z, duration_us, value` (µs when
counters ran, else indices). `devtools_raw(run_id)` returns the full bridge JSON.

## Gotchas

- No capture produced (targetcontrol): the app is too slow to reach steady state, so
  raise `--warmup`; or it presents no frames (offscreen compute), so use the
  in-application API with `--mode launch-wait`.
- **"replay needs a GPU + interactive session"**: the server is running as a
  service/session-0; restart it in your desktop session.
- Marker regions (`PushMarker`) give the flame graph its structure. Apps
  without debug markers produce a flat one-level graph.

See [[flamegraph-reading]] for flame-graph interpretation and
[[devtools-mcp-usage]] for the overall run→analyze→query workflow.
