from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import anyio
import pytest

from devtools_mcp.market_feed import MarketFeedManager
from devtools_mcp.market_feed.config import MAX_MEMORY_MIB, HarnessLimits, arguments, configuration, control
from devtools_mcp.market_feed.manager import MAX_ACTIVE, MAX_RECEIPTS

SCRIPT = r"""
import json, signal, sys, time
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
seed = int(args.get('--seed', '1'))
def emit(obj):
    obj.setdefault('schema_version', 1)
    print(json.dumps(obj), flush=True)
if seed == 2:
    print('bad\n' * 8, end='', flush=True)
    time.sleep(10)
if seed == 3:
    print('x' * 65537, flush=True)
    time.sleep(10)
if seed == 7:
    time.sleep(10)
if seed in (6, 8, 9):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
emit({'type':'definition','symbol_count':int(args.get('--symbols','16'))})
emit({'type':'status','emitted_events':0,'argv':sys.argv[1:]})
sys.stderr.write('diagnostic' * 2000)
sys.stderr.flush()
revision = 0
for line in sys.stdin:
    command = line.rstrip('\n')
    if seed in (8, 9):
        if seed == 8:
            print('bad\n' * 8, end='', flush=True)
        else:
            print('x' * (1024 * 1024), flush=True)
        while True:
            time.sleep(1)
    if seed == 4 and command != 'stop':
        continue
    revision += 1
    emit({'type':'command','command':command,'revision':0 if seed == 5 else revision,
          'ok':command != 'burst 99','error':'refused' if command == 'burst 99' else ''})
    if command == 'status':
        emit({'type':'status','emitted_events':123})
    if command.startswith('book '):
        emit({'type':'book','id':int(command.split()[1]),'levels':[]})
    if command == 'stop':
        break
if seed == 6:
    while True:
        time.sleep(1)
emit({'type':'final','emitted_events':123})
"""


@pytest.fixture
async def harness(tmp_path: Path):
    # A literal semicolon/spaces in the executable pathname proves no shell
    # interprets it. Config fields are independently strict numeric values.
    executable = tmp_path / "native; not a shell command"
    executable.write_text("#!" + sys.executable + "\n" + SCRIPT)
    executable.chmod(0o755)
    manager = MarketFeedManager(executable, start_timeout=2.0, command_timeout=0.1, stop_timeout=0.2)
    yield manager
    await manager.close()
    assert all(run.process is None or run.process.returncode is not None for run in manager.runs.values())


def test_config_and_command_bounds():
    assert configuration(None)["seconds"] == 60
    assert "--rate" in arguments(configuration({"rate": 0}))
    for invalid in (
        {"seconds": 0},
        {"seed": True},
        {"symbols": 4294967296},
        {"memory_mib": 513},
        {"wat": 1},
        {"rate": "1;bad"},
    ):
        with pytest.raises(ValueError):
            configuration(invalid)
    for valid in (
        "rate 0",
        "ramp 1000 200",
        "burst 5",
        "pause",
        "resume",
        "unlimited",
        "book 16",
        "status",
        "stop",
    ):
        assert control(valid, 16) == valid
    for invalid in (
        "rate 1\nstop",
        "book 0",
        "book 17",
        "ramp 9 0",
        "ramp 0 10",
        "burst 0",
        "rate -1",
        "status extra",
    ):
        with pytest.raises(ValueError):
            control(invalid, 16)


async def test_real_process_lifecycle_and_no_shell(harness, monkeypatch):
    called = []
    spawn = asyncio.create_subprocess_exec

    async def spy(*args, **kwargs):
        called.append((args, kwargs))
        return await spawn(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spy)
    assert (await harness.start({"seed": "1;bad"}))["code"] == "invalid_config"
    assert not called
    start = await harness.start({"seconds": 10})
    assert start["status"] == "ok"
    run_id = start["data"]["run_id"]
    assert called[0][0][0] == str(harness.binary)
    assert "shell" not in called[0][1]
    assert start["data"]["mode"] == "synthetic_unpersisted_native_local"
    assert start["data"]["durable"] is False and start["data"]["platform_job"] is False
    reply = await harness.control(run_id, "rate 2500")
    assert reply["receipt"] == {
        "command": "rate 2500",
        "revision": 1,
        "ok": True,
        "error": "",
    }
    assert (await harness.control(run_id, "burst 99"))["code"] == "command_refused"
    assert (await harness.control(run_id, "book 3"))["status"] == "ok"
    await asyncio.sleep(0.01)
    assert harness.status(run_id)["data"]["book"]["id"] == 3
    for _ in range(12):
        assert (await harness.control(run_id, "status"))["status"] == "ok"
    snapshot = harness.status(run_id)["data"]
    assert len(snapshot["command_receipts"]) == MAX_RECEIPTS
    assert snapshot["dropped_diagnostics"] > 0 and len(snapshot["stderr_tail"].encode()) <= 4096
    stopped = await harness.stop(run_id)
    assert stopped["data"]["state"] == "finished" and stopped["data"]["final"]["emitted_events"] == 123
    assert stopped["data"]["exit_code"] == 0
    assert (await harness.stop(run_id))["data"]["state"] == "finished"


@pytest.mark.parametrize(
    "seed,reason",
    [
        (2, "protocol_malformed_output"),
        (3, "protocol_line_limit"),
        (7, "startup_timeout"),
    ],
)
async def test_bounded_bad_output_and_startup_timeout(harness, seed, reason):
    if seed == 7:
        harness.start_timeout = 0.15
    result = await harness.start({"seed": seed})
    assert result["status"] == "error"
    assert result["data"]["failure"] == reason
    assert result["data"]["exit_code"] is not None


async def test_command_timeout_keeps_unknown_identity(harness):
    run_id = (await harness.start({"seed": 4}))["data"]["run_id"]
    result = await harness.control(run_id, "rate 55")
    assert result["code"] == "command_timeout"
    assert result["data"]["pending_command"] == "rate 55"
    assert (await harness.control(run_id, "rate 66"))["code"] == "command_pending"
    assert (await harness.stop(run_id))["data"]["state"] == "finished"


async def test_protocol_revision_error_stops_process(harness):
    run_id = (await harness.start({"seed": 5}))["data"]["run_id"]
    result = await harness.control(run_id, "status")
    assert result["status"] == "error"
    await asyncio.wait_for(harness.runs[run_id].finished.wait(), 1)
    assert harness.status(run_id)["data"]["failure"] == "protocol_command_revision"
    assert harness.runs[run_id].process.returncode is not None


async def test_hung_graceful_stop_is_killed_and_reaped(harness):
    run_id = (await harness.start({"seed": 6}))["data"]["run_id"]
    result = await harness.stop(run_id)
    assert result["data"]["failure"] == "forced_stop"
    assert result["data"]["exit_code"] is not None


async def test_active_limit_and_shutdown(harness):
    for _ in range(MAX_ACTIVE):
        assert (await harness.start())["status"] == "ok"
    assert (await harness.start())["code"] == "active_limit"
    await harness.close()
    assert len(harness.list()["runs"]) == MAX_ACTIVE
    assert all(run.finished.is_set() for run in harness.runs.values())
    assert (await harness.start())["code"] == "closed"


async def test_retained_runs_are_bounded(harness, monkeypatch):
    monkeypatch.setattr("devtools_mcp.market_feed.manager.MAX_RETAINED", 2)
    old = []
    for _ in range(3):
        run_id = (await harness.start())["data"]["run_id"]
        old.append(run_id)
        await harness.stop(run_id)
    assert len(harness.list()["runs"]) == 2
    assert harness.status(old[0])["code"] == "not_found"


@pytest.mark.parametrize("cancelled", [False, True])
async def test_callable_registered_and_established_lifespan_reaps_native(harness, monkeypatch, cancelled):
    from devtools_mcp import server
    from devtools_mcp.tools import market_feed_tools as tools

    monkeypatch.setattr(tools, "_manager", harness)
    names = {tool.name for tool in await server.mcp.list_tools()}
    assert "devtools_market_feed" in names
    assert (await tools.devtools_market_feed("start", config={"seconds": 10}))["status"] == "ok"
    assert (await tools.devtools_market_feed("list"))["mode"] == "synthetic_unpersisted_native_local"
    assert (await tools.devtools_market_feed("control", run_id="not-an-owned-run", command="stop"))[
        "code"
    ] == "invalid_input"

    class Context:
        registry = None
        debug_manager = None
        cleaned = False

        def cleanup_all(self):
            self.cleaned = True

    class Registry:
        async def detect_all(self):
            return None

    context = Context()
    monkeypatch.setattr(server, "_EAGER_CTX", None)
    monkeypatch.setattr(server, "_bootstrap_ctx", lambda: context)
    monkeypatch.setattr(server, "ToolRegistry", Registry)
    monkeypatch.setattr(server, "_launch_dbos", lambda: None)
    monkeypatch.setattr(server, "_maybe_start_dashboard", lambda _: None)
    with anyio.CancelScope() as scope:
        async with server.app_lifespan(server.mcp):
            assert any(not run.finished.is_set() for run in harness.runs.values())
            if cancelled:
                scope.cancel()
    assert context.cleaned
    assert tools._manager is None
    assert all(run.finished.is_set() for run in harness.runs.values())


def test_large_universe_and_uint64_seed_reach_native_admission(monkeypatch):
    config = configuration({"symbols": 100_000, "seed": 0xFFFFFFFFFFFFFFFF, "rate": 1 << 60})
    assert configuration({"batch": 65_537})["batch"] == 65_537
    assert configuration({"batch": 0xFFFFFFFF})["batch"] == 0xFFFFFFFF
    with pytest.raises(ValueError):
        configuration({"batch": 0x100000000})
    argv = arguments(config)
    assert argv[argv.index("--symbols") + 1] == "100000"
    assert argv[argv.index("--seed") + 1] == "18446744073709551615"
    with pytest.raises(ValueError):
        configuration({"memory_mib": 1024})
    monkeypatch.setenv("DEVTOOLS_MARKET_FEED_MAX_MEMORY_MIB", "2048")
    limits = HarnessLimits.from_env()
    assert configuration({"memory_mib": 1024}, limits)["memory_mib"] == 1024
    assert control("book 99999", 100000) == "book 99999"
    assert control("rate 9223372036854775807", 100000).endswith("9223372036854775807")
    monkeypatch.setenv("DEVTOOLS_MARKET_FEED_MAX_MEMORY_MIB", str(MAX_MEMORY_MIB + 1))
    with pytest.raises(ValueError):
        HarnessLimits.from_env()


@pytest.mark.parametrize("seed,reason", [(8, "protocol_malformed_output"), (9, "protocol_line_limit")])
async def test_protocol_failure_immediately_kills_term_ignoring_process(harness, seed, reason):
    run_id = (await harness.start({"seed": seed, "seconds": 3600}))["data"]["run_id"]
    assert (await harness.control(run_id, "status"))["status"] == "error"
    await asyncio.wait_for(harness.runs[run_id].finished.wait(), 1)
    assert harness.runs[run_id].process.returncode is not None
    assert harness.status(run_id)["data"]["failure"] == reason


async def test_cancellation_during_creation_owns_and_reaps_established_child(harness, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    spawn = asyncio.create_subprocess_exec
    established = []

    async def delayed(*args, **kwargs):
        process = await spawn(*args, **kwargs)
        established.append(process)
        entered.set()
        await release.wait()  # Cancellation arrives after OS creation, before assignment.
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed)
    start = asyncio.create_task(harness.start())
    await asyncio.wait_for(entered.wait(), 2)
    start.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await start
    assert established[0].returncode is not None
    assert all(run.finished.is_set() for run in harness.runs.values())
    assert (await harness.start())["status"] == "ok"  # No leaked admission slot.
