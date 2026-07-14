"""Pure HTML rendering for the visualization terminal (no framework, no CDN).

Dark design system rendered server-side: cards, badges, progress bars — all
plain CSS. Kept dependency-free and side-effect-free so it's unit-testable:
every function takes plain data and returns an HTML string.
"""

from __future__ import annotations

import html

import polars as pl

_CSS = """
:root{
  color-scheme:dark;
  --bg:#0a0d12; --surface:#10141b; --card:#131923; --card-hi:#182030;
  --border:#1f2733; --border-hi:#2c3850;
  --text:#d6dee8; --muted:#7d8a9a; --faint:#55606e;
  --accent:#5b8cff; --accent2:#39d0d8;
  --open:#7d8a9a; --progress:#e8b339; --blocked:#e5534b; --done:#3fb950; --cancelled:#6e7681;
  --mono:"Cascadia Mono",Consolas,Menlo,monospace;
}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--text);margin:0;
  font:14px/1.55 "Segoe UI",system-ui,-apple-system,sans-serif}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
code,pre,td,.mono{font-family:var(--mono);font-size:12.5px}
header{background:rgba(13,17,24,.85);backdrop-filter:blur(8px);border-bottom:1px solid var(--border);
  padding:10px 20px;position:sticky;top:0;z-index:5;display:flex;align-items:center;gap:14px}
.brand{display:flex;align-items:center;gap:9px;font-weight:600;color:#eef3f9;font-size:15px}
.dot{width:11px;height:11px;border-radius:3px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  box-shadow:0 0 10px rgba(91,140,255,.55)}
nav{display:flex;gap:4px}
nav a{color:var(--muted);padding:4px 12px;border-radius:7px;font-size:13px}
nav a:hover{background:var(--card);color:var(--text);text-decoration:none}
nav a.on{background:var(--card-hi);color:#eef3f9}
.crumbs{color:var(--faint);font-size:12.5px;margin-left:auto}
.crumbs a{color:var(--muted)}
.wrap{padding:20px;max-width:1500px;margin:0 auto}
h2{font-size:17px;margin:4px 0 14px;color:#eef3f9}
h3{font-size:14px;margin:18px 0 8px;color:#c5cfdb}
.note{color:var(--muted);margin:6px 0;font-size:13px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:12px 14px;
  transition:border-color .12s,transform .12s,box-shadow .12s}
.card:hover{border-color:var(--border-hi);transform:translateY(-1px);box-shadow:0 6px 18px rgba(0,0,0,.35)}
.card-hit{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:12px 14px;
  position:relative;transition:border-color .12s,transform .12s,box-shadow .12s}
.card-hit:hover{border-color:var(--border-hi);transform:translateY(-1px);box-shadow:0 6px 18px rgba(0,0,0,.35)}
.card-overlay{position:absolute;inset:0;z-index:1;border-radius:10px}
.card-body{position:relative;z-index:2;pointer-events:none}
.card-actions{position:relative;z-index:3;margin-top:8px;font-size:12px}
.card-actions a{pointer-events:auto;margin-right:8px}
.card-desc{color:var(--muted);font-size:12.5px;margin:6px 0;line-height:1.45;
  display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.card h4{margin:0 0 6px;font-size:13.5px;color:#e8eef6;font-weight:600}
.card .sub{color:var(--muted);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.row{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.spread{justify-content:space-between}
.badge{display:inline-block;border-radius:6px;padding:1px 8px;font-size:11px;font-weight:600;
  border:1px solid var(--border);background:var(--surface);color:var(--muted);white-space:nowrap}
.badge.suite{color:#9ec1ff;border-color:#27407a;background:#101b31}
.badge.kind-epic{color:#d2a8ff;border-color:#3d2a63;background:#1d1430}
.badge.kind-story{color:#79c0ff;border-color:#1d4273;background:#0e1f38}
.badge.kind-task{color:#a5d6a7;border-color:#2a4a2e;background:#10220f}
.badge.kind-subtask{color:#9ecbff;border-color:#2c3850;background:var(--surface)}
.badge.kind-spike,.badge.kind-test{color:#ffb77c;border-color:#5a3a1a;background:#2a1a08}
.st{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;color:var(--muted)}
.st::before{content:"";width:8px;height:8px;border-radius:50%;background:var(--open)}
.st.in_progress::before{background:var(--progress)}
.st.blocked::before{background:var(--blocked)}
.st.done::before{background:var(--done)}
.st.cancelled::before{background:var(--cancelled)}
.key{font-family:var(--mono);font-size:11.5px;color:var(--accent2)}
.tag{display:inline-block;font-size:10.5px;color:#8ab4f8;background:#0e1c33;border:1px solid #1d3b66;
  border-radius:10px;padding:0 7px;margin:1px 2px 1px 0}
.prio{letter-spacing:1px;font-size:10px;color:var(--progress)}
.bar{height:5px;border-radius:3px;background:var(--surface);overflow:hidden;margin-top:8px}
.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2))}
.board{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px;align-items:start}
.col{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:10px}
.col>h3{margin:2px 4px 10px;font-size:12.5px;text-transform:uppercase;letter-spacing:.7px;color:var(--muted)}
.col .card{margin-bottom:8px;padding:10px 12px}
.count{background:var(--card-hi);border-radius:9px;padding:0 7px;font-size:11px;color:var(--muted)}
table{border-collapse:collapse;width:100%;margin:8px 0;background:var(--card);border-radius:10px;overflow:hidden}
th,td{border-bottom:1px solid var(--border);padding:6px 10px;text-align:left;white-space:nowrap}
th{background:var(--surface);color:var(--muted);font-size:11.5px;text-transform:uppercase;letter-spacing:.5px;
  position:sticky;top:49px}
tr:hover td{background:var(--card-hi)}
pre{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px;overflow:auto}
.pill{display:inline-block;background:var(--surface);border:1px solid var(--border);border-radius:10px;
  padding:0 8px;color:var(--muted);font-size:11px;font-family:var(--mono)}
.flame{background:#1a2332;border-radius:10px;overflow:auto;margin-top:8px}
.flame.light{background:#fff}
.status-strip{color:var(--faint);font-size:12px;margin-left:auto}
.filter-form{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
.filter-form input,.filter-form select{background:var(--surface);border:1px solid var(--border);
  color:var(--text);padding:6px 10px;border-radius:7px;font-size:13px}
.filter-form button{background:var(--accent);color:#fff;border:none;padding:6px 14px;border-radius:7px;cursor:pointer}
.warn{color:var(--progress);font-size:12.5px;margin:8px 0}
.btn{display:inline-block;background:var(--surface);border:1px solid var(--border);border-radius:7px;
  padding:4px 10px;font-size:12px;color:var(--text);cursor:pointer}
.subnav{display:flex;gap:8px;margin:10px 0;flex-wrap:wrap}
.subnav a{font-size:12.5px}
.copy-btn{cursor:pointer;border:1px solid var(--border);background:var(--surface);color:var(--muted);
  border-radius:5px;padding:0 6px;font-size:11px}
.empty{border:1px dashed var(--border);border-radius:12px;padding:34px;text-align:center;color:var(--muted)}
.section{margin-top:22px}
.ready{border-left:3px solid var(--done)}
.stuck{border-left:3px solid var(--blocked)}
.waiting{border-left:3px solid var(--progress)}
.desc{color:#aeb9c6;font-size:13px;white-space:pre-wrap;margin:10px 0}
.check{list-style:none;padding:0;margin:6px 0}
.check li{padding:3px 0;color:var(--muted)}
.check b{color:var(--text);font-weight:500}
"""


def _h(s: object) -> str:
    return html.escape(str(s if s is not None else ""))


def _snippet(text: str, max_len: int = 160) -> str:
    """One-line preview for card bodies."""
    clean = " ".join((text or "").split())
    if not clean:
        return ""
    if len(clean) <= max_len:
        return _h(clean)
    return _h(clean[: max_len - 1]) + "…"


def _clickable_card(href: str, body: str, actions: str = "", extra_class: str = "", aria: str = "") -> str:
    """Card with full-surface click target; actions sit above the overlay."""
    aria_attr = f" aria-label='{_h(aria)}'" if aria else ""
    actions_html = f"<div class='card-actions'>{actions}</div>" if actions else ""
    return (
        f"<div class='card-hit {extra_class}'>"
        f"<a class='card-overlay' href='{href}'{aria_attr}></a>"
        f"<div class='card-body'>{body}</div>"
        f"{actions_html}</div>"
    )


def page(
    title: str,
    body: str,
    crumbs: str = "",
    active: str = "runs",
    status: dict | None = None,
    auto_refresh: bool = False,
) -> str:
    """Wrap body in the shared shell (header, nav, content column)."""
    tabs = [
        ("runs", "/", "Runs"),
        ("search", "/search", "Search"),
        ("tracker", "/tracker", "Tracker"),
        ("collab", "/collab", "Collab"),
        ("graph", "/graph", "Graph"),
        ("tools", "/tools", "Tools"),
    ]
    nav = "".join(f"<a href='{href}' class='{'on' if key == active else ''}'>{label}</a>" for key, href, label in tabs)
    status_html = ""
    if status:
        parts = []
        if status.get("runs") is not None:
            parts.append(f"{status['runs']} runs")
        if status.get("projects") is not None:
            parts.append(f"{status['projects']} projects")
        if status.get("mcp"):
            parts.append("MCP ok")
        status_html = f"<span class='status-strip'>{' · '.join(parts)}</span>"
    refresh = "<meta http-equiv='refresh' content='30'>" if auto_refresh else ""
    script = "<script>setInterval(()=>location.reload(),30000)</script>" if auto_refresh else ""
    return (
        f"<!doctype html><html><head><meta charset='utf-8'>{refresh}"
        f"<title>{_h(title)}</title><style>{_CSS}</style></head><body>"
        f"<header><span class='brand'><span class='dot'></span>devtools-mcp</span>"
        f"<nav>{nav}</nav>{status_html}<span class='crumbs'>{crumbs}</span></header>"
        f"<div class='wrap'>{body}</div>{script}</body></html>"
    )


# --- runs ---------------------------------------------------------------------


def dashboard(rows: list[dict], filters: dict | None = None, status: dict | None = None) -> str:
    """Landing page: every run across all workspaces, as cards."""
    filt = filters or {}
    form = (
        "<form class='filter-form' method='get' action='/'>"
        f"<input name='q' placeholder='binary or label' value='{_h(filt.get('q', ''))}'>"
        f"<input name='suite' placeholder='suite' value='{_h(filt.get('suite', ''))}'>"
        f"<input name='tool' placeholder='tool' value='{_h(filt.get('tool', ''))}'>"
        f"<input name='task_key' placeholder='task key' value='{_h(filt.get('task_key', ''))}'>"
        "<button type='submit'>Filter</button>"
        "<a class='btn' href='/'>Clear</a></form>"
    )
    if not rows:
        body = (
            "<h2>Runs</h2>" + form + "<div class='empty'>No runs yet. Start the service, then use "
            "<code>devtools_run</code> from Cursor or Claude. Runs persist across restarts.</div>"
        )
        return page("devtools-mcp", body, active="runs", status=status, auto_refresh=True)
    cards = []
    for r in rows:
        rid = r["run_id"]
        title = r.get("label") or r.get("binary") or "(no target)"
        actions = [f"<a href='/run/{_h(rid)}'>open</a>"]
        if r.get("has_stacks"):
            actions.append(f"<a href='/flame/{_h(rid)}'>flame</a>")
        actions.append(f"<a href='/raw/{_h(rid)}'>raw</a>")
        exit_note = "" if not r.get("exit") else f"<span class='badge'>exit {_h(r['exit'])}</span>"
        dur = r.get("duration", "")
        when = r.get("when_full") or r.get("when", "")
        task = ""
        if r.get("task_key"):
            task = f"<span class='key'>{_h(r['task_key'])}</span> "
        tags = "".join(f"<span class='tag'>{_h(t)}</span>" for t in (r.get("tag_list") or []))
        git = f"<span class='pill'>{_h(r.get('git_commit', ''))}</span>" if r.get("git_commit") else ""
        preview = r.get("preview") or ""
        desc_html = f"<div class='card-desc'>{_snippet(preview, 200)}</div>" if preview else ""
        body = (
            f"<div class='row spread'>"
            f"<span class='badge suite'>{_h(r['suite'])}:{_h(r['tool'])}</span>"
            f"<span class='pill'>{_h(rid[:8])}</span></div>"
            f"<h4 class='mono'>{_h(title)}</h4>"
            f"{desc_html}"
            f"<div class='row'>{task}{tags}{git}</div>"
            f"<div class='row spread'><span class='sub'>{_h(when)} {dur} {exit_note}</span></div>"
        )
        cards.append(
            _clickable_card(
                f"/run/{_h(rid)}",
                body,
                " · ".join(actions),
                aria=f"Open run {rid}",
            )
        )
    body = f"<h2>Runs <span class='count'>{len(rows)}</span></h2>{form}<div class='grid'>{''.join(cards)}</div>"
    return page("devtools-mcp", body, active="runs", status=status, auto_refresh=True)


def table_from_df(
    df: pl.DataFrame,
    max_rows: int = 200,
    offset: int = 0,
    total: int | None = None,
    page_url: str = "",
) -> str:
    """Render a Polars DataFrame as a bounded HTML table."""
    if df.is_empty():
        return "<p class='note'>(no rows)</p>"
    total_rows = total if total is not None else df.height
    shown = df.slice(offset, max_rows)
    head = "".join(f"<th>{_h(c)}</th>" for c in shown.columns)
    body_rows = []
    for row in shown.iter_rows():
        body_rows.append("<tr>" + "".join(f"<td>{_h(v)}</td>" for v in row) + "</tr>")
    note = ""
    end = offset + len(shown)
    if total_rows > end or offset > 0:
        note = f"<p class='note'>rows {offset + 1}-{end} of {total_rows:,}"
        if page_url:
            next_off = offset + max_rows
            if next_off < total_rows:
                note += f" · <a href='{_h(page_url)}&offset={next_off}'>next</a>"
            if offset > 0:
                prev = max(0, offset - max_rows)
                note += f" · <a href='{_h(page_url)}&offset={prev}'>prev</a>"
        note += "</p>"
    return f"<table><caption>data table</caption><tr>{head}</tr>{''.join(body_rows)}</table>{note}"


def run_page(
    meta: dict,
    summary: str,
    table_html: str,
    has_stacks: bool,
    status: dict | None = None,
) -> str:
    """A single run: metadata, summary + queryable table + view links."""
    rid = meta["run_id"]
    crumbs = f"<a href='/run/{_h(rid)}'>{_h(meta['suite'])}:{_h(meta['tool'])}</a>"
    links = [
        f"<a href='/raw/{_h(rid)}'>raw</a>",
        f"<a href='/compare?a={_h(rid)}'>compare</a>",
        f"<a href='/run/{_h(rid)}/export'>export</a>",
        f"<form method='post' action='/run/{_h(rid)}/delete' style='display:inline'>"
        "<button class='btn' type='submit'>delete</button></form>",
    ]
    if has_stacks:
        links.insert(0, f"<a href='/flame/{_h(rid)}'>flame</a>")
    meta_lines = [
        f"duration {meta.get('duration', '')}",
        f"exit {meta.get('exit_code', '')}",
        f"workspace {meta.get('workspace_name', '')}",
    ]
    if meta.get("task_key"):
        meta_lines.append(f"task <a href='/tracker/task/{_h(meta['task_key'])}'>{_h(meta['task_key'])}</a>")
    if meta.get("git_commit"):
        meta_lines.append(f"git {meta.get('git_commit')} ({meta.get('git_branch', '')})")
    if meta.get("tags"):
        meta_lines.append(f"tags {meta.get('tags')}")
    if meta.get("label"):
        meta_lines.append(f"label {meta.get('label')}")
    if meta.get("notes"):
        meta_lines.append(f"notes {meta.get('notes')}")
    body = (
        f"<h2>{_h(meta['suite'])}:{_h(meta['tool'])} "
        f"<span class='pill'>{_h(rid)}</span></h2>"
        f"<p class='note mono'>{_h(meta.get('binary', ''))}</p>"
        f"<p class='note'>{' · '.join(meta_lines)} · {' · '.join(links)}</p>"
        f"<pre>{_h(summary)}</pre><h3>Data</h3>{table_html}"
    )
    return page(f"run {rid[:8]}", body, crumbs, active="runs", status=status)


def flame_page(run_id: str, svg: str, total: int, focus_name: str | None, hotspots_html: str = "") -> str:
    """Interactive flame graph: the SVG (click-to-zoom links) + optional hotspot table."""
    crumbs = f"<a href='/run/{_h(run_id)}'>run</a> / flame"
    reset = ""
    if focus_name:
        reset = f"<p class='note'>focused on <b>{_h(focus_name)}</b> — " f"<a href='/flame/{_h(run_id)}'>reset</a></p>"
    body = (
        f"<h2>Flame graph <span class='pill'>{_h(run_id[:8])}</span></h2>"
        f"<p class='note'>{total:,} samples · click any frame to zoom into its subtree</p>"
        f"{reset}<div class='flame'>{svg}</div>{hotspots_html}"
    )
    return page(f"flame {run_id[:8]}", body, crumbs, active="runs")


def raw_page(run_id: str, text: str, truncated: bool, size: int = 0) -> str:
    """Raw tool output / logs viewer."""
    crumbs = f"<a href='/run/{_h(run_id)}'>run</a> / raw"
    note = ""
    if truncated:
        note = (
            f"<p class='note'>preview truncated ({size:,} bytes) — "
            f"<a href='/raw/{_h(run_id)}/download'>download full</a></p>"
        )
    body = f"<h2>Raw output <span class='pill'>{_h(run_id[:8])}</span></h2>{note}<pre>{_h(text)}</pre>"
    return page(f"raw {run_id[:8]}", body, crumbs, active="runs")


def search_page(results_html: str, query: dict, status: dict | None = None) -> str:
    form = (
        "<form class='filter-form' method='get' action='/search'>"
        f"<input name='q' placeholder='search' value='{_h(query.get('q', ''))}'>"
        f"<input name='suite' placeholder='suite' value='{_h(query.get('suite', ''))}'>"
        f"<input name='function_pattern' placeholder='function' value='{_h(query.get('function_pattern', ''))}'>"
        "<button type='submit'>Search</button></form>"
    )
    body = f"<h2>Cross-run search</h2>{form}{results_html}"
    return page("search", body, active="search", status=status)


def compare_page(html: str, a: str, b: str, status: dict | None = None) -> str:
    form = (
        "<form class='filter-form' method='get' action='/compare'>"
        f"<input name='a' placeholder='run_id a' value='{_h(a)}'>"
        f"<input name='b' placeholder='run_id b' value='{_h(b)}'>"
        "<button type='submit'>Compare</button></form>"
    )
    body = f"<h2>Compare runs</h2>{form}{html}"
    return page("compare", body, active="runs", status=status)


def correlate_page(html: str, a: str, b: str, join_on: str, status: dict | None = None) -> str:
    form = (
        "<form class='filter-form' method='get' action='/correlate'>"
        f"<input name='a' value='{_h(a)}'>"
        f"<input name='b' value='{_h(b)}'>"
        f"<input name='join_on' value='{_h(join_on)}' placeholder='join_on'>"
        "<button type='submit'>Correlate</button></form>"
    )
    body = f"<h2>Correlate runs</h2>{form}{html}"
    return page("correlate", body, active="runs", status=status)


def tools_page(check_html: str, status: dict | None = None) -> str:
    body = f"<h2>Installed tools</h2><pre>{_h(check_html)}</pre>"
    return page("tools", body, active="tools", status=status)


# --- tracker ------------------------------------------------------------------

STATUS_ORDER: tuple[str, ...] = ("open", "in_progress", "blocked", "done", "cancelled")
STATUS_LABEL: dict[str, str] = {
    "open": "Open",
    "in_progress": "In progress",
    "blocked": "Blocked",
    "done": "Done",
    "cancelled": "Cancelled",
}


def _progress_bar(passed: int, total: int) -> str:
    if total <= 0:
        return ""
    pct = int(100 * passed / total)
    return f"<div class='bar' title='{passed}/{total} criteria passing'>" f"<i style='width:{pct}%'></i></div>"


def _tags_html(tags_csv: str) -> str:
    names = [t for t in (tags_csv or "").split(",") if t]
    return "".join(f"<span class='tag'>{_h(name)}</span>" for name in names[:6])


def task_card(task: dict, extra_class: str = "", footnote: str = "") -> str:
    """One project-management card: key, kind, title, status, tags, progress."""
    key = task["key"]
    prio = "●" * (6 - int(task.get("priority", 3)))
    kids = task.get("n_children") or 0
    kid_note = f"<span class='badge'>{kids} sub</span>" if kids else ""
    status = task.get("status", "open")
    parent_html = ""
    if task.get("parent"):
        parent_html = f"<div class='sub'>parent {_h(task['parent'])}</div>"
    desc = task.get("description") or ""
    desc_html = f"<div class='card-desc'>{_snippet(desc, 180)}</div>" if desc.strip() else ""
    body = (
        f"<div class='row spread'>"
        f"<span class='key'>{_h(key)}</span>"
        f"<span class='badge kind-{_h(task.get('kind', 'task'))}'>{_h(task.get('kind', 'task'))}</span></div>"
        f"<h4>{_h(task['title'])}</h4>"
        f"{parent_html}{desc_html}"
        f"<div class='row spread'>"
        f"<span class='st {_h(status)}'>{_h(STATUS_LABEL.get(status, '?'))}</span>"
        f"<span class='prio' title='priority {_h(task.get('priority', 3))}'>{prio}</span></div>"
        f"<div>{_tags_html(task.get('tags', ''))} {kid_note}</div>"
        f"{_progress_bar(task.get('n_passed') or 0, task.get('n_criteria') or 0)}"
        f"{footnote}"
    )
    return _clickable_card(
        f"/tracker/task/{_h(key)}",
        body,
        f"<a href='/tracker/task/{_h(key)}'>open</a>",
        extra_class=extra_class,
        aria=f"Open task {key}",
    )


def tracker_overview(projects: list[dict]) -> str:
    """All projects as cards with status rollups and criteria progress."""
    if not projects:
        body = (
            "<h2>Tracker</h2><div class='empty'>No projects yet. Create one with "
            "<code>tracker_project(action=&quot;create&quot;)</code>.</div>"
        )
        return page("tracker", body, active="tracker")
    cards = []
    for p in projects:
        total = sum(p["by_status"].get(s, 0) for s in STATUS_ORDER)
        done = p["by_status"].get("done", 0)
        chips = " ".join(
            f"<span class='st {s}'>{p['by_status'].get(s, 0)}</span>" for s in STATUS_ORDER if p["by_status"].get(s, 0)
        )
        desc = p.get("description") or ""
        desc_html = f"<div class='card-desc'>{_snippet(desc, 200)}</div>" if desc.strip() else ""
        body = (
            f"<div class='row spread'>"
            f"<span class='key'>{_h(p['key'])}</span>"
            f"<span class='badge'>{_h(p['close_policy'])}</span></div>"
            f"<h4>{_h(p['name'])}</h4>"
            f"{desc_html}"
            f"<div class='row spread' style='margin-top:8px'>{chips}"
            f"<span class='sub'>{done}/{total} tasks</span></div>"
            f"{_progress_bar(p.get('criteria_passed', 0), p.get('criteria_total', 0) or 1)}"
        )
        cards.append(
            _clickable_card(
                f"/tracker/{_h(p['key'])}",
                body,
                f"<a href='/tracker/{_h(p['key'])}'>open board</a>",
                aria=f"Open project {p['key']}",
            )
        )
    body = f"<h2>Projects <span class='count'>{len(projects)}</span></h2>" f"<div class='grid'>{''.join(cards)}</div>"
    return page("tracker", body, active="tracker")


def tracker_board(
    project: dict,
    tasks_rows: list[dict],
    plan: dict | None,
    total_tasks: int = 0,
    shown: int = 0,
) -> str:
    """Per-project board: status columns of cards + the execution plan."""
    crumbs = f"<a href='/tracker/{_h(project['key'])}'>{_h(project['key'])}</a>"
    subnav = _tracker_subnav(project["key"])
    overflow = ""
    if total_tasks > shown:
        overflow = (
            f"<p class='warn'>Showing {shown} of {total_tasks} tasks — "
            "use tracker filters or sub-views for full lists.</p>"
        )
    columns = []
    for status in STATUS_ORDER:
        in_col = [t for t in tasks_rows if t.get("status") == status]
        if status in ("done", "cancelled") and not in_col:
            continue
        cards = "".join(task_card(t) for t in in_col) or "<p class='note'>—</p>"
        columns.append(
            f"<div class='col'><h3>{STATUS_LABEL[status]} "
            f"<span class='count'>{len(in_col)}</span></h3>{cards}</div>"
        )
    plan_html = _plan_section(plan) if plan else ""
    body = (
        f"<h2>{_h(project['key'])} — {_h(project['name'])} "
        f"<span class='count'>{shown} tasks</span></h2>"
        f"{subnav}{overflow}<div class='board'>{''.join(columns)}</div>{plan_html}"
    )
    return page(f"{project['key']} board", body, crumbs, active="tracker")


def _tracker_subnav(project_key: str) -> str:
    links = [
        ("board", f"/tracker/{_h(project_key)}"),
        ("tree", f"/tracker/{_h(project_key)}/tree"),
        ("rollup", f"/tracker/{_h(project_key)}/rollup"),
        ("criteria", f"/tracker/{_h(project_key)}/criteria"),
        ("commits", f"/tracker/{_h(project_key)}/commits"),
        ("sync", "/tracker/sync"),
    ]
    return "<div class='subnav'>" + " · ".join(f"<a href='{u}'>{label}</a>" for label, u in links) + "</div>"


def _plan_section(plan: dict) -> str:
    """'What needs to happen': ready, blocked, waiting on children."""
    ready = plan.get("ready", [])
    blocked = plan.get("blocked", [])
    waiting = plan.get("waiting_on_children", [])
    if not ready and not blocked and not waiting:
        return ""
    parts = ["<div class='section'><h3>What needs to happen</h3><div class='grid'>"]
    for task in ready[:12]:
        parts.append(task_card(task, extra_class="ready"))
    if len(ready) > 12:
        parts.append(f"<p class='note'>+{len(ready) - 12} more ready</p>")
    for task in waiting[:12]:
        parts.append(task_card(task, extra_class="waiting", footnote="<div class='sub'>open subtasks remain</div>"))
    for task, blockers in blocked[:12]:
        chips = " ".join(f"<a class='key' href='/tracker/task/{_h(b)}'>{_h(b)}</a>" for b in blockers[:5])
        parts.append(task_card(task, extra_class="stuck", footnote=f"<div class='sub'>waiting on {chips}</div>"))
    parts.append("</div></div>")
    return "".join(parts)


def task_detail(
    task: dict,
    criteria: list[dict],
    commits: list[dict],
    related: dict,
    issues: list[dict] | None = None,
    linked_runs: list[str] | None = None,
    activity: list[dict] | None = None,
) -> str:
    """One task, fully expanded: description, criteria, commits, links."""
    key = task["key"]
    crumbs = f"<a href='/tracker/{_h(task['project'])}'>{_h(task['project'])}</a> / {_h(key)}"
    checks = []
    for c in criteria:
        result = c.get("last_result") or ""
        mark = {"pass": "[pass]", "fail": "[fail]"}.get(result, "[ ]")
        ref = f" <code>{_h(c['test_ref'])}</code>" if c.get("test_ref") else ""
        run_at = f" <span class='sub'>{_h(c.get('last_run_at', ''))}</span>" if c.get("last_run_at") else ""
        cid = c.get("id", "")
        toggles = (
            f"<form method='post' action='/tracker/task/{_h(key)}/criteria/{cid}' style='display:inline'>"
            "<button class='btn' name='result' value='pass' type='submit'>pass</button> "
            "<button class='btn' name='result' value='fail' type='submit'>fail</button></form>"
        )
        checks.append(f"<li>{mark} <b>{_h(c['text'])}</b>{ref}{run_at} {toggles}</li>")
    checks_html = f"<h3>Acceptance criteria</h3><ul class='check'>{''.join(checks)}</ul>" if checks else ""
    commit_rows = "".join(
        f"<li><code>{_h(c['commit_hash'])}</code> "
        f"<span class='sub'>{_h(c.get('repo_path', ''))}</span> "
        f"{_h(c['message_snippet'])} "
        f"<span class='sub'>{_h(c.get('linked_at', ''))}</span></li>"
        for c in commits[:50]
    )
    commits_html = f"<h3>Commits</h3><ul class='check'>{commit_rows}</ul>" if commit_rows else ""
    issue_rows = ""
    for iss in issues or []:
        url = iss.get("url") or ""
        label = f"{iss.get('provider', '')} #{iss.get('ref_id', '')} ({iss.get('state', '')})"
        if url:
            synced = _h(iss.get("last_synced", ""))
            issue_rows += f"<li><a href='{_h(url)}'>{_h(label)}</a> <span class='sub'>{synced}</span></li>"
        else:
            issue_rows += f"<li>{_h(label)}</li>"
    issues_html = f"<h3>External issues</h3><ul class='check'>{issue_rows}</ul>" if issue_rows else ""
    runs_html = ""
    if linked_runs:
        links = " ".join(f"<a href='/run/{_h(r)}'>{_h(r[:8])}</a>" for r in linked_runs[:20])
        runs_html = f"<h3>Linked profiling runs</h3><p>{links}</p>"
    activity_html = ""
    if activity:
        touch_rows = "".join(
            f"<li><code>{_h(t.get('file_path', ''))}</code> {_h(t.get('op', ''))} "
            f"by <b>{_h(t.get('agent_label') or t.get('session_id', ''))}</b> "
            f"<span class='sub'>{_h(t.get('ts', ''))}</span></li>"
            for t in activity[:50]
        )
        activity_html = f"<h3>File activity</h3><ul class='check'>{touch_rows}</ul>"
    rel_parts = []
    if task.get("parent"):
        rel_parts.append(
            f"<div class='sub'>parent: <a class='key' href='/tracker/task/{_h(task['parent'])}'>"
            f"{_h(task['parent'])}</a></div>"
        )
    for label, keys in related.items():
        if keys:
            chips = " ".join(f"<a class='key' href='/tracker/task/{_h(k)}'>{_h(k)}</a>" for k in keys[:30])
            rel_parts.append(f"<div class='sub'>{_h(label)}: {chips}</div>")
    desc = f"<div class='desc'>{_h(task.get('description') or '')}</div>" if task.get("description") else ""
    body = (
        f"<h2><span class='key'>{_h(key)}</span> {_h(task['title'])}</h2>"
        f"<div class='row'>"
        f"<span class='badge kind-{_h(task.get('kind', 'task'))}'>{_h(task.get('kind'))}</span>"
        f"<span class='st {_h(task.get('status'))}'>{_h(STATUS_LABEL.get(task.get('status', ''), '?'))}</span>"
        f"<span class='badge'>priority {_h(task.get('priority'))}</span>"
        f"<span class='badge'>depth {_h(task.get('depth', ''))}</span>"
        f"{_tags_html(task.get('tags', ''))}</div>"
        f"{desc}{''.join(rel_parts)}{issues_html}{checks_html}{commits_html}{runs_html}{activity_html}"
        f"<p class='note'>created {_h(task.get('created_at'))} · updated {_h(task.get('updated_at'))}</p>"
        f"<form method='post' action='/tracker/task/{_h(key)}/status' style='margin-top:12px'>"
        f"<select name='status'>"
        + "".join(f"<option value='{s}'>{STATUS_LABEL[s]}</option>" for s in STATUS_ORDER)
        + "</select><button type='submit'>Update status</button></form>"
    )
    return page(f"{key}", body, crumbs, active="tracker")


def tracker_tree(project_key: str, lines: list[str]) -> str:
    pre = "\n".join(lines) if lines else "(empty)"
    body = f"<h2>{_h(project_key)} tree</h2><pre>{_h(pre)}</pre>"
    return page(f"{project_key} tree", body, active="tracker")


def tracker_table_page(
    project_key: str,
    view: str,
    table_html: str,
    global_page: bool = False,
) -> str:
    title = view if global_page else f"{project_key} {view}"
    crumbs = f"<a href='/tracker/{_h(project_key)}'>{_h(project_key)}</a> / {view}" if project_key else ""
    subnav = _tracker_subnav(project_key) if project_key else ""
    body = f"<h2>{_h(title)}</h2>{subnav}{table_html}"
    return page(title, body, crumbs, active="tracker")


def tracker_sync(status: dict) -> str:
    body = f"<h2>CRDT sync</h2><pre>{_h(str(status))}</pre>"
    return page("sync", body, active="tracker")


def collab_page(
    sessions: list[dict],
    claims: list[dict],
    recent: list[dict],
    contested: set[tuple[str, str]],
) -> str:
    """Local agent collaboration: who is working where, right now.

    `contested` marks (repo_root, file_path) pairs touched or claimed by more
    than one session — those rows get the conflict (stuck) border.
    """
    session_cards = "".join(
        f"<div class='card'><h4>{_h(s.get('agent_label') or s.get('session_id', ''))}</h4>"
        f"<div class='sub'>session <code>{_h(str(s.get('session_id', ''))[:16])}</code></div>"
        f"<div class='row'><span class='badge'>{_h(s.get('touches', 0))} touches</span>"
        f"<span class='badge'>{_h(s.get('claims', 0))} claims</span></div>"
        f"<div class='sub'>last seen {_h(s.get('last_seen', ''))}</div></div>"
        for s in sessions[:50]
    )
    sessions_html = (
        f"<div class='grid'>{session_cards}</div>"
        if session_cards
        else "<div class='empty'>No agent sessions yet — touches arrive via the "
        "Claude Code hooks or <code>tracker_files</code>.</div>"
    )
    claim_rows = "".join(
        f"<li class='{'stuck' if (c.get('repo_root', ''), c.get('file_path', '')) in contested else ''}'>"
        f"<code>{_h(c.get('file_path', ''))}</code> held by "
        f"<b>{_h(c.get('agent_label') or c.get('session_id', ''))}</b>"
        + (f" on <span class='key'>{_h(c['task_key'])}</span>" if c.get("task_key") else "")
        + f" <span class='sub'>expires {_h(c.get('expires_at', ''))} · {_h(c.get('repo_root', ''))}</span></li>"
        for c in claims[:100]
    )
    claims_html = f"<ul class='check'>{claim_rows}</ul>" if claim_rows else "<p class='note'>no active claims</p>"
    touch_rows = "".join(
        f"<li class='{'stuck' if (t.get('repo_root', ''), t.get('file_path', '')) in contested else ''}'>"
        f"<code>{_h(t.get('file_path', ''))}</code> {_h(t.get('op', ''))} by "
        f"<b>{_h(t.get('agent_label') or t.get('session_id', ''))}</b>"
        + (f" on <span class='key'>{_h(t['task_key'])}</span>" if t.get("task_key") else "")
        + f" <span class='sub'>{_h(t.get('ts', ''))} · {_h(t.get('repo_root', ''))}</span></li>"
        for t in recent[:100]
    )
    touches_html = f"<ul class='check'>{touch_rows}</ul>" if touch_rows else "<p class='note'>no recent activity</p>"
    body = (
        "<h2>Agent collaboration</h2>"
        "<p class='note'>File touches and advisory claims from every agent on this machine, "
        "reported by hooks and <code>tracker_files</code>. Highlighted rows are contested — "
        "more than one session on the same file. A multi-user team collab server is "
        "<b>coming soon</b>; this local view is its single-machine precursor.</p>"
        f"<div class='section'><h3>Sessions</h3>{sessions_html}</div>"
        f"<div class='section'><h3>Active claims</h3>{claims_html}</div>"
        f"<div class='section'><h3>Recent touches</h3>{touches_html}</div>"
    )
    return page("collab", body, active="collab", auto_refresh=True)


def skills_panel(rows: list[dict], mode: str) -> str:
    """Local control panel: router index + power-mode/enable toggles.

    `rows` are skills (name, description, category, modes, has_goap, live,
    disabled, override); `mode` is the active global power mode. Toggles POST to
    the /api/skilldoc/* JSON endpoints via inline fetch, then reload. Free/local:
    power + enable/disable act on live skills; static skills are read-only here.
    """
    assert isinstance(rows, list), "rows must be a list"
    by_cat: dict[str, list[dict]] = {}
    for row in rows[:1000]:  # bounded
        by_cat.setdefault(str(row.get("category", "?")), []).append(row)
    sections = []
    for category in sorted(by_cat):
        items = []
        for row in by_cat[category]:
            name = _h(row.get("name", ""))
            tags = []
            if row.get("modes"):
                tags.append("power: " + "/".join(_h(m) for m in row["modes"]))
            if row.get("has_goap"):
                tags.append("goap")
            if not row.get("live"):
                tags.append("static")
            tag_html = f" <span class='sub'>({'; '.join(tags)})</span>" if tags else ""
            controls = ""
            if row.get("live"):
                disabled = bool(row.get("disabled"))
                toggle = "enable" if disabled else "disable"
                label = "Enable" if disabled else "Disable"
                controls = (
                    f"<button onclick=\"toggle('{toggle}','{name}')\">{label}</button> "
                    f"<button onclick=\"setmode('{name}','low')\">low</button> "
                    f"<button onclick=\"setmode('{name}','high')\">high</button>"
                )
            state = " <span class='sub'>[disabled]</span>" if row.get("disabled") else ""
            override = row.get("override") or ""
            ov = f" <span class='sub'>[override: {_h(override)}]</span>" if override else ""
            items.append(
                f"<li><b>{name}</b>{tag_html}{state}{ov}<br>"
                f"<span class='note'>{_h(row.get('description', ''))}</span><br>{controls}</li>"
            )
        sections.append(f"<div class='section'><h3>{_h(category)}</h3><ul class='check'>{''.join(items)}</ul></div>")
    script = (
        "<script>"
        "async function post(path,body){await fetch(path,{method:'POST',"
        "headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});location.reload();}"
        "function setglobal(m){post('/api/skilldoc/mode',{mode:m});}"
        "function setmode(n,m){post('/api/skilldoc/mode',{name:n,mode:m});}"
        "function toggle(a,n){post('/api/skilldoc/'+a,{name:n});}"
        "function rebuild(){post('/api/skilldoc/route/rebuild',{});}"
        "</script>"
    )
    header = (
        f"<h2>Skills</h2><p class='note'>Active power mode: <b>{_h(mode)}</b>. "
        "Toggle how dynamic skills render and rebuild the router index. "
        f"{len(rows)} skill(s) indexed.</p>"
        "<p><button onclick=\"setglobal('low')\">Set mode: low</button> "
        "<button onclick=\"setglobal('high')\">Set mode: high</button> "
        "<button onclick=\"rebuild()\">Rebuild router</button></p>"
    )
    return page("skills", header + "".join(sections) + script, active="runs")


def graph_page(svg: str, focus: str, node_count: int, edge_count: int, note: str = "") -> str:
    """Wrap a code-graph SVG in the dashboard shell. Server-rendered, no CDN —
    every node is a focus link (click to re-root), like the flamegraph."""
    head = (
        "<h2>Code graph</h2>"
        f"<p class='note'>Focus: <code>{_h(focus)}</code> — {node_count} node(s), "
        f"{edge_count} edge(s) shown. Click any node to re-root. "
        "Built natively by LLM Station (<code>graph_build</code> → <code>graph_export</code>).</p>"
    )
    if note:
        head += f"<p class='note'>{_h(note)}</p>"
    body = head + f"<div class='section' style='overflow-x:auto'>{svg}</div>"
    return page("code graph", body, active="graph")
