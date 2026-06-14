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
.flame{background:#fff;border-radius:10px;overflow:auto;margin-top:8px}
.empty{border:1px dashed var(--border);border-radius:12px;padding:34px;text-align:center;color:var(--muted)}
.section{margin-top:22px}
.ready{border-left:3px solid var(--done)}
.stuck{border-left:3px solid var(--blocked)}
.desc{color:#aeb9c6;font-size:13px;white-space:pre-wrap;margin:10px 0}
.check{list-style:none;padding:0;margin:6px 0}
.check li{padding:3px 0;color:var(--muted)}
.check b{color:var(--text);font-weight:500}
"""


def _h(s: object) -> str:
    return html.escape(str(s if s is not None else ""))


def page(title: str, body: str, crumbs: str = "", active: str = "runs") -> str:
    """Wrap body in the shared shell (header, nav, content column)."""
    tabs = [("runs", "/", "Runs"), ("tracker", "/tracker", "Tracker")]
    nav = "".join(f"<a href='{href}' class='{'on' if key == active else ''}'>{label}</a>" for key, href, label in tabs)
    return (
        f"<!doctype html><html><head><meta charset='utf-8'><title>{_h(title)}</title>"
        f"<style>{_CSS}</style></head><body>"
        f"<header><span class='brand'><span class='dot'></span>devtools-mcp</span>"
        f"<nav>{nav}</nav><span class='crumbs'>{crumbs}</span></header>"
        f"<div class='wrap'>{body}</div></body></html>"
    )


# --- runs ---------------------------------------------------------------------


def dashboard(rows: list[dict]) -> str:
    """Landing page: every run across all workspaces, as cards."""
    if not rows:
        body = "<h2>Runs</h2><div class='empty'>No runs yet. Use <code>devtools_run</code>, " "then refresh.</div>"
        return page("devtools-mcp", body, active="runs")
    cards = []
    for r in rows:
        views = [f"<a href='/run/{_h(r['run_id'])}'>data</a>"]
        if r.get("has_stacks"):
            views.append(f"<a href='/flame/{_h(r['run_id'])}'>flame</a>")
        views.append(f"<a href='/raw/{_h(r['run_id'])}'>raw</a>")
        exit_note = "" if not r.get("exit") else f"<span class='badge'>exit {_h(r['exit'])}</span>"
        cards.append(
            f"<div class='card'><div class='row spread'>"
            f"<span class='badge suite'>{_h(r['suite'])}:{_h(r['tool'])}</span>"
            f"<span class='pill'>{_h(r['run_id'][:8])}</span></div>"
            f"<h4 class='mono'>{_h(r['binary'] or '(no target)')}</h4>"
            f"<div class='row spread'><span class='sub'>{_h(r['when'])} {exit_note}</span>"
            f"<span>{' · '.join(views)}</span></div></div>"
        )
    body = f"<h2>Runs <span class='count'>{len(rows)}</span></h2><div class='grid'>{''.join(cards)}</div>"
    return page("devtools-mcp", body, active="runs")


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
            f"<p class='note'>showing {max_rows} of {df.height:,} rows — query precisely " "with devtools_analyze</p>"
        )
    return f"<table><tr>{head}</tr>{''.join(body_rows)}</table>{note}"


def run_page(meta: dict, summary: str, table_html: str, has_stacks: bool) -> str:
    """A single run: summary + queryable table + view links."""
    rid = meta["run_id"]
    crumbs = f"<a href='/run/{_h(rid)}'>{_h(meta['suite'])}:{_h(meta['tool'])}</a>"
    links = [f"<a href='/raw/{_h(rid)}'>raw output</a>"]
    if has_stacks:
        links.insert(0, f"<a href='/flame/{_h(rid)}'>flame graph</a>")
    body = (
        f"<h2>{_h(meta['suite'])}:{_h(meta['tool'])} <span class='pill'>{_h(rid[:8])}</span></h2>"
        f"<p class='note mono'>{_h(meta['binary'])} · {' · '.join(links)}</p>"
        f"<pre>{_h(summary)}</pre><h3>Data</h3>{table_html}"
    )
    return page(f"run {rid[:8]}", body, crumbs, active="runs")


def flame_page(run_id: str, svg: str, total: int, focus_name: str | None) -> str:
    """Interactive flame graph: the SVG (click-to-zoom links) + breadcrumb."""
    crumbs = f"<a href='/run/{_h(run_id)}'>run</a> / flame"
    reset = ""
    if focus_name:
        reset = f"<p class='note'>focused on <b>{_h(focus_name)}</b> — " f"<a href='/flame/{_h(run_id)}'>reset</a></p>"
    body = (
        f"<h2>Flame graph <span class='pill'>{_h(run_id[:8])}</span></h2>"
        f"<p class='note'>{total:,} samples · click any frame to zoom into its subtree</p>"
        f"{reset}<div class='flame'>{svg}</div>"
    )
    return page(f"flame {run_id[:8]}", body, crumbs, active="runs")


def raw_page(run_id: str, text: str, truncated: bool) -> str:
    """Raw tool output / logs viewer."""
    crumbs = f"<a href='/run/{_h(run_id)}'>run</a> / raw"
    note = "<p class='note'>truncated — full output via devtools_raw</p>" if truncated else ""
    body = f"<h2>Raw output <span class='pill'>{_h(run_id[:8])}</span></h2>{note}<pre>{_h(text)}</pre>"
    return page(f"raw {run_id[:8]}", body, crumbs, active="runs")


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
    prio = "●" * (6 - int(task.get("priority", 3)))
    kids = task.get("n_children") or 0
    kid_note = f"<span class='badge'>{kids} sub</span>" if kids else ""
    status = task.get("status", "open")
    return (
        f"<div class='card {extra_class}'>"
        f"<div class='row spread'>"
        f"<a class='key' href='/tracker/task/{_h(task['key'])}'>{_h(task['key'])}</a>"
        f"<span class='badge kind-{_h(task.get('kind', 'task'))}'>{_h(task.get('kind', 'task'))}</span></div>"
        f"<h4>{_h(task['title'])}</h4>"
        f"<div class='row spread'>"
        f"<span class='st {_h(status)}'>{_h(STATUS_LABEL.get(status, '?'))}</span>"
        f"<span class='prio' title='priority {_h(task.get('priority', 3))}'>{prio}</span></div>"
        f"<div>{_tags_html(task.get('tags', ''))} {kid_note}</div>"
        f"{_progress_bar(task.get('n_passed') or 0, task.get('n_criteria') or 0)}"
        f"{footnote}</div>"
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
        cards.append(
            f"<div class='card'><div class='row spread'>"
            f"<a class='key' href='/tracker/{_h(p['key'])}'>{_h(p['key'])}</a>"
            f"<span class='badge'>{_h(p['close_policy'])}</span></div>"
            f"<h4>{_h(p['name'])}</h4>"
            f"<div class='sub'>{_h(p['description'] or '')}</div>"
            f"<div class='row spread' style='margin-top:8px'>{chips}"
            f"<span class='sub'>{done}/{total} done</span></div>"
            f"{_progress_bar(done, total)}</div>"
        )
    body = f"<h2>Projects <span class='count'>{len(projects)}</span></h2>" f"<div class='grid'>{''.join(cards)}</div>"
    return page("tracker", body, active="tracker")


def tracker_board(project: dict, tasks_rows: list[dict], plan: dict | None) -> str:
    """Per-project board: status columns of cards + the execution plan."""
    crumbs = f"<a href='/tracker/{_h(project['key'])}'>{_h(project['key'])}</a>"
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
        f"<span class='count'>{len(tasks_rows)} tasks</span></h2>"
        f"<div class='board'>{''.join(columns)}</div>{plan_html}"
    )
    return page(f"{project['key']} board", body, crumbs, active="tracker")


def _plan_section(plan: dict) -> str:
    """'What needs to happen': ready cards highlighted, blocked with blockers."""
    ready = plan.get("ready", [])
    blocked = plan.get("blocked", [])
    if not ready and not blocked:
        return ""
    parts = ["<div class='section'><h3>What needs to happen</h3><div class='grid'>"]
    for task in ready[:12]:
        parts.append(task_card(task, extra_class="ready"))
    for task, blockers in blocked[:12]:
        chips = " ".join(f"<a class='key' href='/tracker/task/{_h(b)}'>{_h(b)}</a>" for b in blockers[:5])
        parts.append(task_card(task, extra_class="stuck", footnote=f"<div class='sub'>waiting on {chips}</div>"))
    parts.append("</div></div>")
    return "".join(parts)


def task_detail(task: dict, criteria: list[dict], commits: list[dict], related: dict) -> str:
    """One task, fully expanded: description, criteria, commits, links."""
    key = task["key"]
    crumbs = f"<a href='/tracker/{_h(task['project'])}'>{_h(task['project'])}</a> / {_h(key)}"
    checks = []
    for c in criteria:
        mark = {"pass": "✅", "fail": "❌"}.get(c.get("last_result") or "", "⬜")
        ref = f" <code>{_h(c['test_ref'])}</code>" if c.get("test_ref") else ""
        checks.append(f"<li>{mark} <b>{_h(c['text'])}</b>{ref}</li>")
    checks_html = f"<h3>Acceptance criteria</h3><ul class='check'>{''.join(checks)}</ul>" if checks else ""
    commit_rows = "".join(
        f"<li><code>{_h(c['commit_hash'][:12])}</code> {_h(c['message_snippet'])}</li>" for c in commits[:10]
    )
    commits_html = f"<h3>Commits</h3><ul class='check'>{commit_rows}</ul>" if commit_rows else ""
    rel_parts = []
    for label, keys in related.items():
        if keys:
            chips = " ".join(f"<a class='key' href='/tracker/task/{_h(k)}'>{_h(k)}</a>" for k in keys[:10])
            rel_parts.append(f"<div class='sub'>{_h(label)}: {chips}</div>")
    desc = f"<div class='desc'>{_h(task.get('description') or '')}</div>" if task.get("description") else ""
    body = (
        f"<h2><span class='key'>{_h(key)}</span> {_h(task['title'])}</h2>"
        f"<div class='row'>"
        f"<span class='badge kind-{_h(task.get('kind', 'task'))}'>{_h(task.get('kind'))}</span>"
        f"<span class='st {_h(task.get('status'))}'>{_h(STATUS_LABEL.get(task.get('status', ''), '?'))}</span>"
        f"<span class='badge'>priority {_h(task.get('priority'))}</span>"
        f"{_tags_html(task.get('tags', ''))}</div>"
        f"{desc}{''.join(rel_parts)}{checks_html}{commits_html}"
        f"<p class='note'>created {_h(task.get('created_at'))} · updated {_h(task.get('updated_at'))}</p>"
    )
    return page(f"{key}", body, crumbs, active="tracker")
