"""Tests for the VTune backend: CSV parsers, frame builder, stacks, registration.

All data is synthetic CSV in the shapes `vtune -report ... -format csv` emits;
the real tool is never needed (same approach as the ETW/JVM/CDB suites).
"""

from __future__ import annotations

import polars as pl

import devtools_mcp.vtune.backend  # noqa: F401  (registers the suite)
from devtools_mcp.models import create_run_base
from devtools_mcp.registry import get_backend
from devtools_mcp.vtune.analysis import vtune_functions_df, vtune_stack_samples
from devtools_mcp.vtune.formatters import format_vtune_summary
from devtools_mcp.vtune.models import VtuneResult
from devtools_mcp.vtune.parsers import _to_float, parse_function_csv, parse_topdown_csv
from devtools_mcp.vtune.runner import ANALYSES, _opt, _passthrough

HOTSPOTS_CSV = """\
Function,CPU Time,CPU Time:Effective Time,CPU Time:Spin Time,Module,Source File
compress_block,12.504,12.100,0.404,app.exe,compress.c
hash_lookup,3.210,3.210,0.000,app.exe,hash.c
memcpy,1.005,1.005,0.000,ucrtbase.dll,
[Outside any known module],0.120,0.120,0.000,,
"""

MEMORY_CSV = """\
Function,Loads,Stores,LLC Miss Count,Module,Source File
scan_rows,1250000,40000,9800,app.exe,scan.c
hash_lookup,310000,1200,150,app.exe,hash.c
"""

TOPDOWN_CSV = """\
Function Stack,CPU Time: Total,CPU Time: Self,Module
Total,100.0%,0s,
 main,98.5%,0.100s,app.exe
  run_pipeline,95.0%,0.500s,app.exe
   compress_block,80.0%,12.000s,app.exe
   hash_lookup,10.0%,3.200s,app.exe
  shutdown,1.0%,0.050s,app.exe
"""


def _result(**overrides) -> VtuneResult:
    base = create_run_base(suite="vtune", tool="cpu", binary="app.exe", args=[], duration_seconds=1.0)
    fields = dict(base.model_dump(), analysis_type="hotspots", result_dir="r-cpu")
    fields.update(overrides)
    return VtuneResult(**fields)


class TestToFloat:
    def test_variants(self):
        assert _to_float("1.234") == 1.234
        assert _to_float("12.5%") == 12.5
        assert _to_float("0.500s") == 0.5
        assert _to_float("1,250,000") == 1_250_000.0
        assert _to_float("") is None
        assert _to_float("n/a") is None


class TestFunctionCsv:
    def test_hotspots_columns(self):
        functions = parse_function_csv(HOTSPOTS_CSV)
        assert [f.function for f in functions][:2] == ["compress_block", "hash_lookup"]
        top = functions[0]
        assert top.module == "app.exe"
        assert top.source_file == "compress.c"
        assert top.metrics["cpu_time"] == 12.504
        assert top.metrics["cpu_time_spin_time"] == 0.404
        assert top.primary == 12.504  # first metric column is primary

    def test_memory_columns_differ(self):
        functions = parse_function_csv(MEMORY_CSV)
        assert functions[0].metrics == {"loads": 1_250_000.0, "stores": 40_000.0, "llc_miss_count": 9_800.0}
        assert functions[0].primary == 1_250_000.0

    def test_empty_and_garbage(self):
        assert parse_function_csv("") == []
        assert parse_function_csv("not,a,vtune\nreport,at,all\n") == []


class TestTopdownCsv:
    def test_folds_indentation(self):
        samples = parse_topdown_csv(TOPDOWN_CSV)
        by_leaf = {s.frames[-1]: s for s in samples}
        assert by_leaf["compress_block"].frames == ["main", "run_pipeline", "compress_block"]
        assert by_leaf["compress_block"].weight == 12_000  # 12.000s -> ms
        assert by_leaf["hash_lookup"].frames == ["main", "run_pipeline", "hash_lookup"]
        assert by_leaf["shutdown"].frames == ["main", "shutdown"]  # popped back to depth 2
        assert "Total" not in {f for s in samples for f in s.frames}

    def test_self_time_rows_only(self):
        samples = parse_topdown_csv(TOPDOWN_CSV)
        assert all(s.weight > 0 for s in samples)
        assert len(samples) == 5  # every row with self>0; Total (0s) excluded

    def test_no_stack_column(self):
        assert parse_topdown_csv("Function,CPU Time\nmain,1.0\n") == []


class TestFrameAndStacks:
    def test_dataframe_value_alias(self):
        result = _result(functions=parse_function_csv(HOTSPOTS_CSV))
        df = vtune_functions_df(result)
        assert df.columns[:4] == ["function", "module", "file", "value"]
        assert df["value"][0] == 12.504
        assert "cpu_time_spin_time" in df.columns

    def test_empty_frame_schema(self):
        df = vtune_functions_df(_result())
        assert df.is_empty()
        assert df.schema["value"] == pl.Float64

    def test_stacks_roundtrip(self):
        result = _result(stack_samples=parse_topdown_csv(TOPDOWN_CSV))
        samples = vtune_stack_samples(result)
        assert len(samples) == 5
        assert all(s.frames and s.weight > 0 for s in samples)


class TestSummary:
    def test_bounded_summary(self):
        result = _result(
            functions=parse_function_csv(HOTSPOTS_CSV),
            stack_samples=parse_topdown_csv(TOPDOWN_CSV),
            summary_text="Elapsed Time: 13.2s\nCPU Time: 16.7s\n" + "noise\n" * 100,
        )
        text = format_vtune_summary(result)
        assert "compress_block" in text
        assert "Elapsed Time" in text
        assert "more lines" in text  # summary is truncated, not dumped
        assert "devtools_flamegraph" in text
        assert text.count("\n") < 80


class TestRunnerHelpers:
    def test_opt_and_passthrough(self):
        extra = ["--pid", "1234", "-knob", "sampling-mode=hw", "--report-only", "--result-dir", "r000"]
        assert _opt(extra, "--pid") == "1234"
        assert _opt(extra, "--result-dir") == "r000"
        assert _passthrough(extra) == ["-knob", "sampling-mode=hw"]
        assert _passthrough(None) == []


class TestRegistration:
    def test_backend_registered(self):
        backend = get_backend("vtune")
        assert set(backend.tools) == set(ANALYSES)
        assert backend.stacks is not None
        assert "_default" in backend.df_builders
