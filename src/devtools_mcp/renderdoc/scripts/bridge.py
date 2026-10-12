# RenderDoc bridge: runs INSIDE qrenderdoc's embedded Python (3.6-era) via
# `qrenderdoc --python bridge.py`. Parameters arrive via environment variables
# (sys.argv does not exist in the embedded interpreter):
#   DEVTOOLS_RDOC_REQUEST, path to a request JSON file
#   DEVTOOLS_RDOC_OUTPUT, path to write the response JSON file
# The script ALWAYS writes a response and ALWAYS calls sys.exit() so the
# qrenderdoc UI never opens. Keep the syntax conservative (no walrus, no
# match, no `X | Y` annotations) and imports stdlib-only.
#
# Wire format: schema_version 1. See devtools_mcp/renderdoc/parsers.py.
import json
import os
import sys
import time

SCHEMA_VERSION = 1
MAX_ACTIONS_HARD = 100000
MAX_WALK = 1000000  # absolute bound on tree traversal steps


def read_request():
    path = os.environ.get("DEVTOOLS_RDOC_REQUEST", "")
    if not path or not os.path.isfile(path):
        raise RuntimeError("DEVTOOLS_RDOC_REQUEST missing or not a file: %r" % path)
    with open(path, "r") as f:
        req = json.load(f)
    if req.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError("request schema_version mismatch: %r" % req.get("schema_version"))
    if req.get("op") not in ("replay", "capture"):
        raise RuntimeError("unknown op: %r" % req.get("op"))
    return req


def write_output(payload):
    payload["schema_version"] = SCHEMA_VERSION
    path = os.environ.get("DEVTOOLS_RDOC_OUTPUT", "")
    if path:
        with open(path, "w") as f:
            json.dump(payload, f)


def flag_names(flags):
    # "ActionFlags.Drawcall | ActionFlags.Indexed" -> "Drawcall|Indexed"
    text = str(flags)
    parts = [p.strip().split(".")[-1] for p in text.split("|")]
    return "|".join([p for p in parts if p and p != "NoFlags"])


def flatten_actions(roots, sdfile, max_actions):
    # Iterative DFS with explicit stack; children visited in order.
    out = []  # type: list
    truncated = False
    stack = [(a, 0, 0) for a in reversed(list(roots))]
    steps = 0
    while stack and steps < MAX_WALK:
        steps += 1
        action, depth, parent_eid = stack.pop()
        if len(out) >= max_actions:
            truncated = True
            break
        dispatch = [0, 0, 0]
        dims = getattr(action, "dispatchDimension", None)
        if dims is not None:
            dispatch = [int(dims[0]), int(dims[1]), int(dims[2])]
        out.append(
            {
                "eid": int(action.eventId),
                "aid": int(action.actionId),
                "parent_eid": int(parent_eid),
                "depth": int(depth),
                "name": action.GetName(sdfile),
                "flags": flag_names(action.flags),
                "num_indices": int(getattr(action, "numIndices", 0)),
                "num_instances": int(getattr(action, "numInstances", 0)),
                "dispatch": dispatch,
            }
        )
        children = list(getattr(action, "children", []) or [])
        for child in reversed(children):
            stack.append((child, depth + 1, int(action.eventId)))
    return out, truncated


def action_stats(actions):
    stats = {"draws": 0, "dispatches": 0, "copies": 0, "markers": 0}
    for a in actions:
        flags = a["flags"]
        if "Drawcall" in flags or "MeshDispatch" in flags:
            stats["draws"] += 1
        elif "Dispatch" in flags:
            stats["dispatches"] += 1
        elif "Copy" in flags or "Resolve" in flags or "Clear" in flags:
            stats["copies"] += 1
        elif "Marker" in flags:
            stats["markers"] += 1
    return stats


def collect_resources(controller):
    rows = []
    textures = {}
    for tex in controller.GetTextures():
        textures[int(tex.resourceId)] = tex
    buffers = {}
    for buf in controller.GetBuffers():
        buffers[int(buf.resourceId)] = buf
    for res in controller.GetResources():
        rid = int(res.resourceId)
        row = {
            "id": str(rid),
            "name": res.name,
            "type": str(res.type).split(".")[-1],
            "width": 0,
            "height": 0,
            "depth": 0,
            "mips": 0,
            "format": "",
            "bytes": 0,
        }
        tex = textures.get(rid)
        if tex is not None:
            row["width"] = int(tex.width)
            row["height"] = int(tex.height)
            row["depth"] = int(tex.depth)
            row["mips"] = int(tex.mips)
            row["format"] = tex.format.Name()
            row["bytes"] = int(tex.byteSize)
        buf = buffers.get(rid)
        if buf is not None:
            row["bytes"] = int(buf.length)
        rows.append(row)
    return rows


def collect_counters(rd, controller, requested_names):
    # GPU Duration first; extra counters by (case-insensitive) description name.
    available = controller.EnumerateCounters()
    chosen = []
    descs = {}
    wanted = [n.lower() for n in requested_names]
    for counter in available:
        desc = controller.DescribeCounter(counter)
        descs[counter] = desc
        if counter == rd.GPUCounter.EventGPUDuration or desc.name.lower() in wanted:
            chosen.append(counter)
    if not chosen:
        return []
    rows = []
    for sample in controller.FetchCounters(chosen):
        desc = descs.get(sample.counter)
        name = desc.name if desc is not None else str(sample.counter)
        unit = str(desc.unit).split(".")[-1] if desc is not None else ""
        value = counter_value(sample, desc)
        if desc is not None and desc.unit == rd.CounterUnit.Seconds:
            value = value * 1e6
            unit = "us"
            if sample.counter == rd.GPUCounter.EventGPUDuration:
                name = "GPU Duration"
        rows.append({"eid": int(sample.eventId), "counter": name, "unit": unit, "value": value})
    return rows


def counter_value(sample, desc):
    # CounterValue is a union; pick the member by the counter's declared
    # result type (CompType) and byte width.
    v = sample.value
    if desc is None:
        return float(getattr(v, "d", 0.0))
    if desc.resultByteWidth == 8:
        if str(desc.resultType).endswith("Float"):
            return float(v.d)
        return float(v.u64)
    if str(desc.resultType).endswith("Float"):
        return float(v.f)
    return float(v.u32)


def op_replay(rd, req):
    rdc_path = req.get("rdc_path", "")
    if not os.path.isfile(rdc_path):
        raise RuntimeError("failed to open: no such file %r" % rdc_path)
    cap = rd.OpenCaptureFile()
    try:
        result = cap.OpenFile(rdc_path, "", None)
        if not result.OK():
            raise RuntimeError("failed to open capture: %s" % result.Message())
        if cap.LocalReplaySupport() != rd.ReplaySupport.Supported:
            raise RuntimeError("capture unsupported for local replay (ReplaySupport=%s)" % cap.LocalReplaySupport())
        result, controller = cap.OpenCapture(rd.ReplayOptions(), None)
        if controller is None:
            raise RuntimeError("replay device failed: %s" % result.Message())
        try:
            sdfile = controller.GetStructuredFile()
            max_actions = min(int(req.get("max_actions", 50000)), MAX_ACTIONS_HARD)
            actions, truncated = flatten_actions(controller.GetRootActions(), sdfile, max_actions)
            frame_info = controller.GetFrameInfo()
            payload = {
                "ok": True,
                "op": "replay",
                "api": cap.DriverName(),
                "frame_number": int(frame_info.frameNumber),
                "actions": actions,
                "resources": [],
                "counters": [],
                "stats": action_stats(actions),
                "truncated": truncated,
            }
            if req.get("want_resources"):
                payload["resources"] = collect_resources(controller)
            if req.get("want_counters"):
                payload["counters"] = collect_counters(rd, controller, req.get("counter_names", []))
            return payload
        finally:
            controller.Shutdown()
    finally:
        cap.Shutdown()


def op_capture(rd, req):
    exe = req.get("exe", "")
    if not os.path.isfile(exe):
        raise RuntimeError("failed to open: no such file %r" % exe)
    working_dir = req.get("working_dir", "") or os.path.dirname(exe)
    cmdline = req.get("cmdline", "")
    template = req.get("capture_file", "")
    warmup_s = float(req.get("warmup_s", 3.0))
    max_wait_s = float(req.get("max_wait_s", 60.0))
    frame = req.get("frame", None)

    opts = rd.CaptureOptions()
    exec_result = rd.ExecuteAndInject(exe, working_dir, cmdline, [], template, opts, False)
    ident = int(getattr(exec_result, "ident", 0))
    if ident == 0:
        raise RuntimeError("ExecuteAndInject failed: %s" % exec_result.result.Message())

    target = rd.CreateTargetControl("localhost", ident, "devtools-mcp", True)
    if target is None:
        raise RuntimeError("CreateTargetControl failed (ident=%d)" % ident)
    pid = 0
    rdc_paths = []
    frame_captured = None
    try:
        pid = int(target.GetPID())
        time.sleep(warmup_s)
        if frame is not None:
            target.QueueCapture(int(frame), 1)
        else:
            target.TriggerCapture(1)
        deadline = time.time() + max_wait_s
        while time.time() < deadline:
            msg = target.ReceiveMessage(None)
            if msg is None:
                time.sleep(0.1)
                continue
            if msg.type == rd.TargetControlMessageType.NewCapture:
                rdc_paths.append(msg.newCapture.path)
                frame_captured = int(msg.newCapture.frameNumber)
                break
            if msg.type == rd.TargetControlMessageType.Disconnected:
                break
    finally:
        target.Shutdown()
    if pid:
        try:
            os.kill(pid, 9)
        except OSError:
            pass
    return {
        "ok": True,
        "op": "capture",
        "rdc_paths": rdc_paths,
        "frame": frame_captured,
        "pid": pid,
    }


def main():
    payload = {"ok": False, "error": "bridge did not run", "stage": "start"}
    try:
        req = read_request()
        payload = {"ok": False, "error": "op did not complete", "stage": req["op"]}
        import renderdoc as rd

        if req["op"] == "replay":
            payload = op_replay(rd, req)
        else:
            payload = op_capture(rd, req)
    except Exception as exc:  # noqa: BLE001  # every failure becomes a payload
        stage = payload.get("stage", "start")
        payload = {"ok": False, "error": "%s: %s" % (type(exc).__name__, exc), "stage": stage}
    try:
        write_output(payload)
    finally:
        sys.exit(0 if payload.get("ok") else 1)


main()
