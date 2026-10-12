#!/usr/bin/env python3
"""kb_mcp.py: MCP (stdio) server exposing kb.py as native agent tools.

Stdlib only. Newline-delimited JSON-RPC 2.0 on stdin/stdout, per the MCP
stdio transport. Each tool call runs the same code path as the kb.py CLI
(one implementation, two front doors) with its output captured as text.

Project root: $CLAUDE_PROJECT_DIR if set, else the git toplevel of the
working directory. Register in a project's .mcp.json:

    {"mcpServers": {"project-kb": {"command": "python3",
                                   "args": [".claude/tools/kb_mcp.py"]}}}
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import kb  # noqa: E402

SERVER_INFO = {"name": "project-kb", "version": "1.0.0"}
DEFAULT_PROTOCOL = "2025-06-18"
MAX_OUTPUT_CHARS = 60_000


def _s(desc: str, **props) -> dict:
    required = [k for k, v in props.items() if v.pop("required", False)]
    return {"description": desc,
            "inputSchema": {"type": "object", "properties": props, "required": required}}


TOOLS = {
    "kb_search": _s(
        "Search the project index (files + agent notes). BM25 by default; hybrid "
        "BM25+embeddings when embeddings are configured. Use BEFORE grep/glob.",
        query={"type": "string", "description": "words, \"phrase\", prefix*, a OR b", "required": True},
        k={"type": "integer", "description": "max results (default 10)"},
        kind={"type": "string", "enum": ["md", "code", "text", "note"]},
        path={"type": "string", "description": "SQL LIKE path filter, e.g. src/api/%"},
        mode={"type": "string", "enum": ["auto", "bm25", "semantic", "hybrid"]}),
    "kb_symbol": _s("Find definition sites of a function/class/type by name (substring ok).",
                    name={"type": "string", "required": True},
                    k={"type": "integer"}),
    "kb_notes": _s("Search agent-recorded notes only (stale ones are flagged).",
                   query={"type": "string", "required": True}),
    "kb_note_add": _s(
        "Record a verified, non-obvious finding so it is never re-researched. Name the "
        "files it depends on; the note is flagged STALE when they change.",
        topic={"type": "string", "required": True},
        body={"type": "string", "description": "the fact + how it was verified", "required": True},
        paths={"type": "array", "items": {"type": "string"}}),
    "kb_note_done": _s("Retire a note (e.g. after folding it into a CLAUDE.md).",
                       id={"type": "integer", "required": True}),
    "kb_index": _s("Refresh the index (incremental; full=true rebuilds).",
                   full={"type": "boolean"}),
    "kb_check": _s("Lint the project map: missing/stub area docs, broken links, size budgets, stale notes."),
    "kb_areas": _s("List candidate areas and whether each has its CLAUDE.md."),
    "kb_stats": _s("Index statistics."),
}


def _argv(name: str, a: dict) -> list[str]:
    if name == "kb_search":
        argv = ["search", a["query"], "-k", str(a.get("k", 10))]
        for flag in ("kind", "path", "mode"):
            if a.get(flag):
                argv += [f"--{flag}", str(a[flag])]
        return argv
    if name == "kb_symbol":
        return ["symbol", a["name"], "-k", str(a.get("k", 20))]
    if name == "kb_notes":
        return ["notes", a["query"]]
    if name == "kb_note_add":
        argv = ["note", "add", "--topic", a["topic"], "--body", a["body"]]
        return argv + (["--paths", *a["paths"]] if a.get("paths") else [])
    if name == "kb_note_done":
        return ["note", "done", str(int(a["id"]))]
    if name == "kb_index":
        return ["index"] + (["--full"] if a.get("full") else [])
    return {"kb_check": ["check"], "kb_areas": ["areas"], "kb_stats": ["stats"]}[name]


def project_root() -> str:
    env = os.environ.get("CLAUDE_PROJECT_DIR", "")
    # Guard against an unsubstituted "${CLAUDE_PROJECT_DIR}" from a config file.
    if env and "${" not in env and Path(env).is_dir():
        return str(Path(env).resolve())
    return str(kb.find_root(None))


def run_tool(name: str, args: dict) -> tuple[str, bool]:
    if name not in TOOLS:
        return f"unknown tool {name}", True
    try:
        argv = ["--root", project_root(), *_argv(name, args or {})]
    except KeyError as e:
        return f"missing argument {e}", True
    out, code = io.StringIO(), 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            kb.main(argv)
        except SystemExit as e:
            code = e.code if isinstance(e.code, int) else 1
        except Exception as e:  # a tool must answer, never kill the server
            print(f"kb internal error: {type(e).__name__}: {e}")
            code = 1
    text = out.getvalue()
    if len(text) > MAX_OUTPUT_CHARS:
        text = text[:MAX_OUTPUT_CHARS] + "\n… (truncated; narrow the query or lower k)"
    # `check` exits 1 when it finds errors; that is a result, not a tool failure.
    return text or "(no output)", code != 0 and name != "kb_check"


def handle(msg: dict) -> dict | None:
    method, mid = msg.get("method"), msg.get("id")
    if mid is None:
        return None  # notification (e.g. notifications/initialized)
    if method == "initialize":
        proto = (msg.get("params") or {}).get("protocolVersion") or DEFAULT_PROTOCOL
        result = {"protocolVersion": proto, "capabilities": {"tools": {}},
                  "serverInfo": SERVER_INFO}
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": [{"name": n, **spec} for n, spec in TOOLS.items()]}
    elif method == "tools/call":
        p = msg.get("params") or {}
        text, is_err = run_tool(p.get("name", ""), p.get("arguments") or {})
        result = {"content": [{"type": "text", "text": text}], "isError": is_err}
    else:
        return {"jsonrpc": "2.0", "id": mid,
                "error": {"code": -32601, "message": f"method not found: {method}"}}
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            reply = {"jsonrpc": "2.0", "id": None,
                     "error": {"code": -32700, "message": "parse error"}}
        else:
            reply = handle(msg)
        if reply is not None:
            sys.stdout.write(json.dumps(reply) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
