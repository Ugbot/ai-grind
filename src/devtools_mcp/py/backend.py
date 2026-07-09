"""Python backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.models import RunBase
from devtools_mcp.py.analysis import py_funcstats_df, py_hotspots_df, py_stack_samples, py_threads_df
from devtools_mcp.py.formatters import format_py_summary
from devtools_mcp.py.models import PyResult
from devtools_mcp.py.runner import check_py, run_py
from devtools_mcp.registry import BackendSpec, InstalledTool, InstallSpec, InstallStep, register_backend

PY_INSTALL = InstallSpec(
    platforms={
        "windows": [InstallStep(kind="pip", argv=["pip", "install", "py-spy"], description="py-spy sampler via pip")],
        "linux": [InstallStep(kind="pip", argv=["pip", "install", "py-spy"], description="py-spy sampler via pip")],
        "darwin": [InstallStep(kind="pip", argv=["pip", "install", "py-spy"], description="py-spy sampler via pip")],
    },
    note="cProfile is stdlib and always available; py-spy enables the cpu/threads verbs.",
    url="https://github.com/benfred/py-spy",
)


async def detect() -> list[InstalledTool]:
    """cProfile is always available (stdlib); cpu/threads need py-spy installed."""
    info = await check_py()
    pyspy = info.get("py-spy") or ""
    return [
        InstalledTool(suite="py", name="cprofile", path=info["python"], version="stdlib", available=True),
        InstalledTool(suite="py", name="cpu", path=pyspy or "py-spy", version="py-spy", available=bool(pyspy)),
        InstalledTool(suite="py", name="threads", path=pyspy or "py-spy", version="py-spy", available=bool(pyspy)),
    ]


async def run(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run a Python tool via the runner."""
    return await run_py(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, PyResult):
        return format_py_summary(result)
    return f"Unknown py result: {type(result)}"


_DF_BUILDERS = {
    "cpu": py_hotspots_df,
    "threads": py_threads_df,
    "cprofile": py_funcstats_df,
    "_default": py_funcstats_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="py",
            tools=["cpu", "threads", "cprofile"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
            stacks=py_stack_samples,
            install=PY_INSTALL,
        )
    )


_register()
