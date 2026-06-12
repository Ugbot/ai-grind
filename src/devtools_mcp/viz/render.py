"""Pure HTML rendering for the visualization terminal (no framework, dark theme).

Kept dependency-free and side-effect-free so it's unit-testable: every function
takes plain data and returns an HTML string.
"""

from __future__ import annotations

import html

import polars as pl

_CSS = """
:root{color-scheme:dark}
body{background:#0d1117;color:#c9d1d9;font:13px/1.5 Consolas,Menlo,monospace;margin:0}
a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}
header{background:#161b22;border-bottom:1px solid #30363d;padding:10px 16px;position:sticky;top:0}
header b{color:#e6edf3;font-size:15px}
nav{margin-left:14px;color:#8b949e}
.wrap{padding:16px}
table{border-collapse:collapse;width:100%;margin:8px 0}
th,td{border:1px solid #30363d;padding:4px 8px;text-align:left;white-space:nowrap}
th{background:#161b22;position:sticky;top:46px}
tr:hover td{background:#161b22}
pre{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;overflow:auto}
.pill{display:inline-block;background:#21262d;border:1px solid #30363d;border-radius:10px;
 padding:0 8px;color:#8b949e;font-size:11px}
.note{color:#8b949e;margin:6px 0}
.flame{background:#fff;border-radius:6px;overflow:auto;margin-top:8px}
"""


def _h(s: object) -> str:
    return html.escape(str(s if s is not None else ""))


def page(title: str, body: str, crumbs: str = "") -> str:
    """Wrap body in the shared shell."""
    return (
        f"<!doctype html><html><head><meta charset='utf-8'><title>{_h(title)}</title>"
        f"<style>{_CSS}</style></head><body>"
        f"<header><b>devtools-mcp</b><nav><a href='/'>dashboard</a>{crumbs}</nav></header>"
        f"<div class='wrap'>{body}</div></body></html>"
    )


def dashboard(rows: list[dict]) -> str:
    """Landing page: every run across all workspaces."""
    if not rows:
        return page("devtools-mcp", "<p class='note'>No runs yet. Use devtools_run, " "then refresh.</p>")
    out = [
        "<h2>Runs</h2><table><tr><th>run</th><th>suite:tool</th><th>target</th>"
        "<th>when</th><th>exit</th><th>views</th></tr>"
    ]
    for r in rows:
        views = [f"<a href='/run/{_h(r['run_id'])}'>data</a>"]
        if r.get("has_stacks"):
            views.append(f"<a href='/flame/{_h(r['run_id'])}'>flame</a>")
        views.append(f"<a href='/raw/{_h(r['run_id'])}'>raw</a>")
        out.append(
            f"<tr><td><span class='pill'>{_h(r['run_id'][:8])}</span></td>"
            f"<td>{_h(r['suite'])}:{_h(r['tool'])}</td><td>{_h(r['binary'])}</td>"
            f"<td>{_h(r['when'])}</td><td>{_h(r['exit'])}</td><td>{' · '.join(views)}</td></tr>"
        )
    out.append("</table>")
    return page("devtools-mcp", "".join(out))


def table_from_df(df: pl.DataFrame, max_rows: int = 200) -> str:
    """Render a Polars DataFrame as a bounded HTML table."""
    if df.is_empty():
        return "<p class='note'>(no rows)</p>"
    shown = df.head(max_rows)
    head = "".join(f"<th>{_h(c)}</th>" for c in shown.columns)
    body_rows = []
    for row in shown.iter_rows():
        body_rows.append("<tr>" + "".join(f"<td>{_h(v)}</td>" for v in row) + "</tr>")
    note = ""
    if df.height > max_rows:
        note = (
            f"<p class='note'>showing {max_rows} of {df.height:,} rows — " "query precisely with devtools_analyze</p>"
        )
    return f"<table><tr>{head}</tr>{''.join(body_rows)}</table>{note}"


def run_page(meta: dict, summary: str, table_html: str, has_stacks: bool) -> str:
    """A single run: summary + queryable table + view links."""
    rid = meta["run_id"]
    crumbs = f" / <a href='/run/{_h(rid)}'>{_h(meta['suite'])}:{_h(meta['tool'])}</a>"
    links = [f"<a href='/raw/{_h(rid)}'>raw output</a>"]
    if has_stacks:
        links.insert(0, f"<a href='/flame/{_h(rid)}'>flame graph</a>")
    body = (
        f"<h2>{_h(meta['suite'])}:{_h(meta['tool'])} <span class='pill'>{_h(rid[:8])}</span></h2>"
        f"<p class='note'>{_h(meta['binary'])} · {' · '.join(links)}</p>"
        f"<pre>{_h(summary)}</pre><h3>Data</h3>{table_html}"
    )
    return page(f"run {rid[:8]}", body, crumbs)


def flame_page(run_id: str, svg: str, total: int, focus_name: str | None) -> str:
    """Interactive flame graph: the SVG (click-to-zoom links) + breadcrumb."""
    crumbs = f" / <a href='/run/{_h(run_id)}'>run</a> / flame"
    reset = ""
    if focus_name:
        reset = f"<p class='note'>focused on <b>{_h(focus_name)}</b> — " f"<a href='/flame/{_h(run_id)}'>reset</a></p>"
    body = (
        f"<h2>Flame graph <span class='pill'>{_h(run_id[:8])}</span></h2>"
        f"<p class='note'>{total:,} samples · click any frame to zoom into its subtree</p>"
        f"{reset}<div class='flame'>{svg}</div>"
    )
    return page(f"flame {run_id[:8]}", body, crumbs)


def raw_page(run_id: str, text: str, truncated: bool) -> str:
    """Raw tool output / logs viewer."""
    crumbs = f" / <a href='/run/{_h(run_id)}'>run</a> / raw"
    note = "<p class='note'>truncated — full output via devtools_raw</p>" if truncated else ""
    body = f"<h2>Raw output <span class='pill'>{_h(run_id[:8])}</span></h2>{note}<pre>{_h(text)}</pre>"
    return page(f"raw {run_id[:8]}", body, crumbs)
