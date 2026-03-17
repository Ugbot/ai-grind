"""Standalone CLI for running valgrind tools and analyzing output."""

from __future__ import annotations

import asyncio
import sys

import click

from valgrind_mcp import formatters
from valgrind_mcp.models import create_run_base
from valgrind_mcp.parsers import (
    parse_cachegrind,
    parse_callgrind,
    parse_massif,
    parse_memcheck_xml,
    parse_threadcheck_xml,
)
from valgrind_mcp.runner import check_valgrind, run_valgrind


@click.group()
def cli() -> None:
    """Valgrind tool suite CLI with structured output and analysis."""


@cli.command()
def check() -> None:
    """Check if valgrind is installed."""
    info = asyncio.run(check_valgrind())
    if info.get("installed") == "true":
        click.echo(f"Valgrind installed: {info['version']}")
    else:
        click.echo(f"Valgrind not found: {info.get('error', 'unknown')}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("binary")
@click.option("--args", "-a", multiple=True, help="Arguments for the binary")
@click.option("--timeout", "-t", default=300, help="Timeout in seconds")
@click.option("--valgrind-arg", "-v", multiple=True, help="Extra valgrind flags")
def memcheck(binary: str, args: tuple[str, ...], timeout: int, valgrind_arg: tuple[str, ...]) -> None:
    """Run memcheck on a binary."""
    result = asyncio.run(
        run_valgrind(
            tool="memcheck",
            binary=binary,
            binary_args=list(args),
            valgrind_args=list(valgrind_arg),
            timeout=timeout,
        )
    )
    if result.exit_code == -1:
        click.echo(f"Error: {result.stderr}", err=True)
        sys.exit(1)
    run_base = create_run_base("memcheck", binary, duration_seconds=result.duration_seconds, exit_code=result.exit_code)
    parsed = parse_memcheck_xml(result.output_path, run_base)
    click.echo(formatters.format_memcheck_summary(parsed))


@cli.command()
@click.argument("binary")
@click.option("--args", "-a", multiple=True)
@click.option("--timeout", "-t", default=300)
@click.option("--valgrind-arg", "-v", multiple=True)
def helgrind(binary: str, args: tuple[str, ...], timeout: int, valgrind_arg: tuple[str, ...]) -> None:
    """Run helgrind on a binary."""
    result = asyncio.run(
        run_valgrind(
            tool="helgrind",
            binary=binary,
            binary_args=list(args),
            valgrind_args=list(valgrind_arg),
            timeout=timeout,
        )
    )
    if result.exit_code == -1:
        click.echo(f"Error: {result.stderr}", err=True)
        sys.exit(1)
    run_base = create_run_base("helgrind", binary, duration_seconds=result.duration_seconds, exit_code=result.exit_code)
    parsed = parse_threadcheck_xml(result.output_path, run_base)
    click.echo(formatters.format_threadcheck_summary(parsed))


@cli.command()
@click.argument("binary")
@click.option("--args", "-a", multiple=True)
@click.option("--timeout", "-t", default=300)
@click.option("--valgrind-arg", "-v", multiple=True)
def callgrind(binary: str, args: tuple[str, ...], timeout: int, valgrind_arg: tuple[str, ...]) -> None:
    """Run callgrind on a binary."""
    result = asyncio.run(
        run_valgrind(
            tool="callgrind",
            binary=binary,
            binary_args=list(args),
            valgrind_args=list(valgrind_arg),
            timeout=timeout,
        )
    )
    if result.exit_code == -1:
        click.echo(f"Error: {result.stderr}", err=True)
        sys.exit(1)
    run_base = create_run_base(
        "callgrind",
        binary,
        duration_seconds=result.duration_seconds,
        exit_code=result.exit_code,
    )
    parsed = parse_callgrind(result.output_path, run_base)
    click.echo(formatters.format_callgrind_summary(parsed))


@cli.command()
@click.argument("binary")
@click.option("--args", "-a", multiple=True)
@click.option("--timeout", "-t", default=300)
@click.option("--valgrind-arg", "-v", multiple=True)
def cachegrind(binary: str, args: tuple[str, ...], timeout: int, valgrind_arg: tuple[str, ...]) -> None:
    """Run cachegrind on a binary."""
    result = asyncio.run(
        run_valgrind(
            tool="cachegrind",
            binary=binary,
            binary_args=list(args),
            valgrind_args=list(valgrind_arg),
            timeout=timeout,
        )
    )
    if result.exit_code == -1:
        click.echo(f"Error: {result.stderr}", err=True)
        sys.exit(1)
    run_base = create_run_base(
        "cachegrind",
        binary,
        duration_seconds=result.duration_seconds,
        exit_code=result.exit_code,
    )
    parsed = parse_cachegrind(result.output_path, run_base)
    click.echo(formatters.format_cachegrind_summary(parsed))


@cli.command()
@click.argument("binary")
@click.option("--args", "-a", multiple=True)
@click.option("--timeout", "-t", default=300)
@click.option("--valgrind-arg", "-v", multiple=True)
def massif(binary: str, args: tuple[str, ...], timeout: int, valgrind_arg: tuple[str, ...]) -> None:
    """Run massif on a binary."""
    result = asyncio.run(
        run_valgrind(
            tool="massif",
            binary=binary,
            binary_args=list(args),
            valgrind_args=list(valgrind_arg),
            timeout=timeout,
        )
    )
    if result.exit_code == -1:
        click.echo(f"Error: {result.stderr}", err=True)
        sys.exit(1)
    run_base = create_run_base("massif", binary, duration_seconds=result.duration_seconds, exit_code=result.exit_code)
    parsed = parse_massif(result.output_path, run_base)
    click.echo(formatters.format_massif_summary(parsed))


@cli.command()
@click.argument("file_path")
@click.option(
    "--tool",
    "-t",
    required=True,
    type=click.Choice(["memcheck", "helgrind", "drd", "callgrind", "cachegrind", "massif"]),
)
def parse(file_path: str, tool: str) -> None:
    """Parse an existing valgrind output file and show analysis."""
    run_base = create_run_base(tool, "parsed-file")

    if tool == "memcheck":
        parsed = parse_memcheck_xml(file_path, run_base)
        click.echo(formatters.format_memcheck_summary(parsed))
    elif tool in ("helgrind", "drd"):
        parsed_tc = parse_threadcheck_xml(file_path, run_base, tool=tool)
        click.echo(formatters.format_threadcheck_summary(parsed_tc))
    elif tool == "callgrind":
        parsed_cg = parse_callgrind(file_path, run_base)
        click.echo(formatters.format_callgrind_summary(parsed_cg))
    elif tool == "cachegrind":
        parsed_cache = parse_cachegrind(file_path, run_base)
        click.echo(formatters.format_cachegrind_summary(parsed_cache))
    elif tool == "massif":
        parsed_m = parse_massif(file_path, run_base)
        click.echo(formatters.format_massif_summary(parsed_m))


def main() -> None:
    """CLI entry point."""
    cli()


if __name__ == "__main__":
    main()
