"""dtrace argv construction: env reaches the -c child; convenience probes filter to the target."""

import shlex

from devtools_mcp.dtrace.runner import build_dtrace_cmd


def _prog(cmd):
    """The D program: the -n after "dtrace" (sudo has its own -n)."""
    d = cmd.index("dtrace")
    return cmd[cmd.index("-n", d) + 1]


def _c_arg(cmd):
    return shlex.split(cmd[cmd.index("-c") + 1])


def test_no_env_keeps_the_plain_command():
    cmd, _ = build_dtrace_cmd("cpu", binary="/b/bench", args=["drir"])
    assert _c_arg(cmd) == ["/b/bench", "drir"]


def test_cpu_with_c_filters_to_target():
    cmd, _ = build_dtrace_cmd("cpu", binary="/b/bench")
    prog = _prog(cmd)
    assert "/pid == $target/" in prog and prog.startswith("profile-97")


def test_syscall_with_c_filters_to_target():
    cmd, _ = build_dtrace_cmd("syscall", binary="/b/bench")
    assert "/pid == $target/" in _prog(cmd)


def test_cpu_with_pid_filters_to_pid_and_attaches():
    cmd, _ = build_dtrace_cmd("cpu", pid=4242)
    assert "/pid == 4242/" in _prog(cmd)
    assert cmd[cmd.index("-p") + 1] == "4242" and "-c" not in cmd


def test_system_wide_cpu_has_no_predicate():
    cmd, _ = build_dtrace_cmd("cpu")
    assert "/pid" not in _prog(cmd)


def test_trace_without_program_is_an_error():
    cmd, err = build_dtrace_cmd("trace")
    assert cmd == [] and "needs a D program" in err


def test_run_dtrace_with_env_spawns_target_and_attaches(monkeypatch):
    """With env, the target is launched by us (env intact) and dtrace attaches by pid."""
    import asyncio
    import sys

    import devtools_mcp.dtrace.runner as r

    calls = []
    real = asyncio.create_subprocess_exec

    async def fake_exec(*cmd, **kw):
        calls.append((cmd, kw))
        if cmd and cmd[0] == "sudo":
            # stand-in for dtrace: succeed immediately
            return await real(sys.executable, "-c", "print('')", stdout=kw.get("stdout"),
                              stderr=kw.get("stderr"))
        return await real(*cmd, **kw)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    err, _res, _raw = asyncio.run(r.run_dtrace(
        "cpu", binary=sys.executable, args=["-c", "import os,time; time.sleep(0.2)"],
        env={"QMF_PROBE": "1"}, timeout=20))
    target_cmd, target_kw = calls[0]
    assert target_cmd[0] == sys.executable
    assert target_kw["env"]["QMF_PROBE"] == "1"
    dt_cmd = calls[1][0]
    assert "-p" in dt_cmd and "-c" not in dt_cmd
    assert "/pid == " in _prog(list(dt_cmd))
