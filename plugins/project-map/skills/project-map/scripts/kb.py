#!/usr/bin/env python3
"""kb.py: a local SQLite FTS5 / BM25 knowledge index for coding agents.

Stdlib only. One file. The index is derived (rebuild any time); the only
authored content is `notes`, which round-trip through docs/kb-notes.md.

    kb.py init | index [--full] | search Q [--mode] | symbol NAME | areas
    kb.py check | scaffold [--dry-run] | note add|done | notes Q
    kb.py export-notes | import-notes | embed | vendor | stats
    kb.py template [--kind code|docs|mixed] [--name N] [--dry-run]   # one-shot project setup

Embeddings (optional, see kb_embed.py) and the MCP server (kb_mcp.py) live
next to this file.

See ../SKILL.md for the workflow this supports.
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import kb_embed  # noqa: E402  (sibling module, stdlib-only)
import kb_template  # noqa: E402

DB_REL = ".claude/kb/index.sqlite"
CONFIG_REL = ".claude/kb.json"
NOTES_REL = "docs/kb-notes.md"
AREA_DOC_NAMES = ("CLAUDE.md", "AGENTS.md")
MAP_CANDIDATES = ("PROJECT_MAP.md", "MAP.md", "docs/PROJECT_MAP.md")
VENDOR_REL = ".claude/tools"
VENDOR_FILES = ("kb.py", "kb_embed.py", "kb_mcp.py", "kb_template.py")

DEFAULT_CONFIG = {
    "include": ["*"],
    "exclude": [
        ".git/*", ".claude/kb/*", ".claude/kb.json", "node_modules/*", "*/node_modules/*",
        "build/*", "build-*/*", "dist/*", "out/*", "target/*", ".venv/*",
        "venv/*", "__pycache__/*", "*/__pycache__/*", "extern/*",
        "vendor/*", "third_party/*", "*.min.js", "*.map", "*.lock",
        "package-lock.json", "*.png", "*.jpg", "*.jpeg", "*.gif", "*.ico",
        "*.pdf", "*.zip", "*.gz", "*.tar", "*.parquet", "*.sqlite", "*.db",
        "*.so", "*.dylib", "*.dll", "*.exe", "*.o", "*.a", "*.lib",
        "*.woff", "*.woff2", "*.ttf", "*.lic",
    ],
    "max_file_kb": 512,
    "auto_export_notes": True,
    "map_file": None,
    "area_roots": ["src", "lib", "packages", "apps", "services", "modules",
                   "include", "docs", "frontend/src", "app"],
    "area_min_files": 3,
    "top_level_areas": True,
    "area_exclude": [],
    "budgets": {"root_chars": 12000, "map_chars": 24000, "area_chars": 12000},
    "embeddings": {"backend": None, "model": None, "url": None,
                   "api_key_env": None, "dim": None, "batch": 32,
                   "auto": False, "max_chars": 2000},
}

MAX_CHUNK_LINES = 80
MIN_CHUNK_LINES = 4
WINDOW_LINES = 60
MAX_QUERY_TERMS = 32
BM25_WEIGHTS = (2.0, 5.0, 8.0, 1.0)  # path, heading, symbols, body

MD_EXT = {".md", ".markdown", ".mdx", ".rst", ".adoc", ".txt"}
CODE_EXT = {
    ".py", ".pyi", ".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs", ".go",
    ".rs", ".c", ".h", ".cc", ".cpp", ".cxx", ".hpp", ".hh", ".inc",
    ".java", ".kt", ".kts", ".cs", ".swift", ".rb", ".php", ".scala",
    ".sh", ".bash", ".zsh", ".ps1", ".sql", ".lua", ".dart", ".m", ".mm",
}

# (regex, kind). Group 1 is the symbol name. Applied per line.
# Long lines are regexes; splitting them hurts more than it helps.
_SYM = [
    (r"^\s*(?:async\s+)?def\s+([A-Za-z_]\w*)", "def"),
    (r"^\s*class\s+([A-Za-z_]\w*)", "class"),
    (r"^\s*(?:export\s+)?(?:default\s+)?(?:async\s+)?function\*?\s+([A-Za-z_$][\w$]*)", "function"),
    (r"^\s*(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*=>", "function"),
    (r"^\s*(?:export\s+)?(?:interface|type|enum)\s+([A-Za-z_$][\w$]*)", "type"),
    (r"^func\s+(?:\([^)]*\)\s*)?([A-Za-z_]\w*)", "func"),
    (r"^type\s+([A-Za-z_]\w*)\s+(?:struct|interface)", "type"),
    (r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_]\w*)", "fn"),
    (r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:struct|enum|trait|union)\s+([A-Za-z_]\w*)", "type"),
    (r"^\s*(?:template\s*<[^>]*>\s*)?(?:class|struct|union|enum(?:\s+class)?|namespace)\s+([A-Za-z_]\w*)\s*(?:final\s*)?[:{]?\s*$", "type"),
    (r"^(?!\s)(?!(?:if|for|while|switch|return|else|do|case|using|typedef|static_assert)\b)[A-Za-z_][\w:<>,\*&\s]*?\b([A-Za-z_~]\w*(?:::~?\w+)*)\s*\([^;]*$", "func"),
    (r"^\s*(?:(?:public|private|protected|internal|static|final|abstract|override|virtual|async|suspend|open)\s+)+[\w<>\[\],\s\?]*?\b([A-Za-z_]\w*)\s*\([^;]*$", "method"),
    (r"^\s*(?:fun)\s+(?:<[^>]*>\s*)?([A-Za-z_]\w*)", "fun"),
    (r"^\s*(?:function\s+)?([A-Za-z_][\w-]*)\s*\(\)\s*\{", "function"),
    (r"(?i)^\s*create\s+(?:or\s+replace\s+)?(?:table|view|function|procedure|index)\s+(?:if\s+not\s+exists\s+)?([\w.]+)", "sql"),
]
_SYM_RE = [(re.compile(p), k) for p, k in _SYM]
_MD_HEAD = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_FTS_OPS = re.compile(r'["*():^]|\b(?:OR|AND|NOT|NEAR)\b')
_LINK = re.compile(r"\]\(([^)\s#]+)(?:#[^)]*)?\)")
_AT_REF = re.compile(r"(?<![\w/])@([\w./-]+\.md)\b")


# ----------------------------------------------------------------- basics ---

def die(msg: str, code: int = 2) -> None:
    print(f"kb: {msg}", file=sys.stderr)
    sys.exit(code)


def find_root(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).resolve()
    try:
        out = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                             capture_output=True, text=True, check=True)
        return Path(out.stdout.strip()).resolve()
    except (OSError, subprocess.CalledProcessError):
        pass
    here = Path.cwd().resolve()
    for p in (here, *here.parents):
        if (p / ".claude").is_dir() or (p / ".git").exists():
            return p
    return here


def load_config(root: Path) -> dict:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    path = root / CONFIG_REL
    if path.is_file():
        try:
            user = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            die(f"{CONFIG_REL}: {e}")
        for k, v in user.items():
            if k.startswith("_"):
                continue
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg


def connect(root: Path, must_exist: bool = True) -> sqlite3.Connection:
    db = root / DB_REL
    if must_exist and not db.is_file():
        die(f"no index at {DB_REL}; run `kb.py init` then `kb.py index`")
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS files(
  path TEXT PRIMARY KEY, mtime REAL, size INTEGER, sha1 TEXT, kind TEXT,
  lines INTEGER, indexed_at REAL);
CREATE TABLE IF NOT EXISTS chunks(
  id INTEGER PRIMARY KEY, path TEXT, start_line INTEGER, end_line INTEGER,
  heading TEXT);
CREATE INDEX IF NOT EXISTS chunks_path ON chunks(path);
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
  path, heading, symbols, body, tokenize='porter unicode61');
CREATE TABLE IF NOT EXISTS symbols(
  name TEXT, lname TEXT, kind TEXT, path TEXT, line INTEGER);
CREATE INDEX IF NOT EXISTS symbols_lname ON symbols(lname);
CREATE INDEX IF NOT EXISTS symbols_path ON symbols(path);
CREATE TABLE IF NOT EXISTS notes(
  id INTEGER PRIMARY KEY, topic TEXT, body TEXT, paths TEXT,
  created REAL, status TEXT DEFAULT 'open');
CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
  topic, body, paths, tokenize='porter unicode61');
CREATE TABLE IF NOT EXISTS embeddings(
  chunk_id INTEGER PRIMARY KEY, model TEXT, vec BLOB);
"""


def ensure_schema(con: sqlite3.Connection) -> None:
    try:
        con.executescript(SCHEMA)
    except sqlite3.OperationalError as e:
        die(f"SQLite lacks FTS5 ({e}); use a Python built with FTS5")
    con.execute("INSERT OR REPLACE INTO meta VALUES('schema','1')")
    con.commit()


# ------------------------------------------------------------ file walk ---

def _match_any(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, p) for p in patterns)


def list_files(root: Path, cfg: dict) -> list[str]:
    raw: list[str] = []
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-co", "--exclude-standard", "-z"],
            capture_output=True, check=True)
        raw = [p for p in out.stdout.decode("utf-8", "replace").split("\0") if p]
    except (OSError, subprocess.CalledProcessError):
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules")]
            rel = Path(dirpath).relative_to(root)
            raw.extend((rel / f).as_posix() for f in filenames)
    max_bytes = int(cfg["max_file_kb"]) * 1024
    keep = []
    for rel in raw:
        rel = rel[2:] if rel.startswith("./") else rel
        if not _match_any(rel, cfg["include"]) or _match_any(rel, cfg["exclude"]):
            continue
        full = root / rel
        if not full.is_file() or full.stat().st_size > max_bytes:
            continue
        keep.append(rel)
    return sorted(set(keep))


def file_kind(rel: str) -> str:
    ext = Path(rel).suffix.lower()
    if ext in MD_EXT:
        return "md"
    if ext in CODE_EXT:
        return "code"
    return "text"


def read_text(full: Path) -> str | None:
    data = full.read_bytes()
    if b"\0" in data[:8192]:
        return None
    return data.decode("utf-8", "replace")


def sha1_of(full: Path) -> str:
    return hashlib.sha1(full.read_bytes()).hexdigest()


# -------------------------------------------------------------- chunking ---

def split_ident(name: str) -> str:
    """'computeNeighborhood' / 'find_path' -> extra searchable sub-words."""
    parts = re.split(r"[_:.\-~]+", name)
    words = []
    for p in parts:
        words.extend(re.findall(r"[A-Z]+(?=[A-Z][a-z]|\d|$)|[A-Z]?[a-z]+|\d+", p))
    return " ".join(w.lower() for w in words if w)


def extract_symbols(lines: list[str], base: int) -> list[tuple[str, str, int]]:
    out = []
    for i, line in enumerate(lines):
        if len(line) > 400:
            continue
        for rx, kind in _SYM_RE:
            m = rx.match(line)
            if m:
                out.append((m.group(1), kind, base + i))
                break
    return out


def _windows(lines: list[str], start: int, heading: str):
    for off in range(0, max(1, len(lines)), WINDOW_LINES):
        part = lines[off:off + WINDOW_LINES]
        if part:
            yield start + off, start + off + len(part) - 1, heading, part


def chunk_markdown(lines: list[str]):
    stack: list[str] = []
    cur_start, cur_head, buf = 1, "", []
    in_fence = False
    for i, line in enumerate(lines, 1):
        if line.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
        m = None if in_fence else _MD_HEAD.match(line)
        if m and buf:
            yield from _cap(cur_start, cur_head, buf)
            buf = []
        if m:
            level = len(m.group(1))
            stack = stack[:level - 1] + [m.group(2)]
            cur_start, cur_head = i, " > ".join(stack)
        buf.append(line)
    if buf:
        yield from _cap(cur_start, cur_head, buf)


def _cap(start: int, heading: str, buf: list[str]):
    if len(buf) <= MAX_CHUNK_LINES:
        yield start, start + len(buf) - 1, heading, buf
    else:
        yield from _windows(buf, start, heading)


def chunk_code(lines: list[str]):
    bounds = [0]
    for i, line in enumerate(lines):
        indent = len(line) - len(line.lstrip())
        is_def = i and indent <= 4 and any(rx.match(line) for rx, _ in _SYM_RE)
        if is_def and i - bounds[-1] >= MIN_CHUNK_LINES:
            bounds.append(i)
    bounds.append(len(lines))
    for a, b in zip(bounds, bounds[1:]):
        part = lines[a:b]
        head = next((ln.strip()[:160] for ln in part if ln.strip()), "")
        yield from _cap(a + 1, head, part)


def chunk_file(kind: str, lines: list[str]):
    if kind == "md":
        return chunk_markdown(lines)
    if kind == "code":
        return chunk_code(lines)
    return _windows(lines, 1, "")


# --------------------------------------------------------------- indexing ---

def drop_file(con: sqlite3.Connection, rel: str) -> None:
    ids = [r[0] for r in con.execute("SELECT id FROM chunks WHERE path=?", (rel,))]
    con.executemany("DELETE FROM chunks_fts WHERE rowid=?", [(i,) for i in ids])
    con.executemany("DELETE FROM embeddings WHERE chunk_id=?", [(i,) for i in ids])
    con.execute("DELETE FROM chunks WHERE path=?", (rel,))
    con.execute("DELETE FROM symbols WHERE path=?", (rel,))
    con.execute("DELETE FROM files WHERE path=?", (rel,))


def index_file(con: sqlite3.Connection, root: Path, rel: str, st, digest: str) -> int:
    drop_file(con, rel)
    text = read_text(root / rel)
    kind = file_kind(rel)
    lines = text.splitlines() if text is not None else []
    con.execute("INSERT INTO files VALUES(?,?,?,?,?,?,?)",
                (rel, st.st_mtime, st.st_size, digest,
                 kind if text is not None else "binary", len(lines), time.time()))
    if text is None:
        return 0
    syms = extract_symbols(lines, 1) if kind == "code" else []
    con.executemany("INSERT INTO symbols VALUES(?,?,?,?,?)",
                    [(n, n.lower(), k, rel, ln) for n, k, ln in syms])
    n = 0
    for start, end, head, part in chunk_file(kind, lines):
        in_chunk = [s for s, _, ln in syms if start <= ln <= end]
        sym_text = " ".join(in_chunk + [split_ident(s) for s in in_chunk])
        cur = con.execute("INSERT INTO chunks(path,start_line,end_line,heading) VALUES(?,?,?,?)",
                          (rel, start, end, head))
        con.execute("INSERT INTO chunks_fts(rowid,path,heading,symbols,body) VALUES(?,?,?,?,?)",
                    (cur.lastrowid, rel, head, sym_text, "\n".join(part)))
        n += 1
    return n


def cmd_index(args, root: Path, cfg: dict) -> None:
    if args.if_initialized and not (root / CONFIG_REL).is_file():
        return  # plugin hook: stay out of projects that never opted in
    con = connect(root, must_exist=False)
    ensure_schema(con)
    if args.full:
        for t in ("chunks_fts", "chunks", "symbols", "files", "embeddings"):
            con.execute(f"DELETE FROM {t}")
    known = {r["path"]: r for r in con.execute("SELECT * FROM files")}
    present = list_files(root, cfg)
    changed = added = chunks = 0
    t0 = time.time()
    with con:
        for rel in set(known) - set(present):
            drop_file(con, rel)
        for rel in present:
            st = (root / rel).stat()
            row = known.get(rel)
            if row and row["mtime"] == st.st_mtime and row["size"] == st.st_size:
                continue
            digest = sha1_of(root / rel)
            if row and row["sha1"] == digest:
                con.execute("UPDATE files SET mtime=?, size=? WHERE path=?",
                            (st.st_mtime, st.st_size, rel))
                continue
            chunks += index_file(con, root, rel, st, digest)
            changed += 1 if row else 0
            added += 0 if row else 1
    con.execute("INSERT OR REPLACE INTO meta VALUES('indexed_at',?)", (str(time.time()),))
    con.commit()
    removed = len(set(known) - set(present))
    if not args.quiet:
        print(f"kb: {len(present)} files | +{added} new ~{changed} changed "
              f"-{removed} removed | {chunks} chunks written | {time.time() - t0:.2f}s")
    if cfg["embeddings"].get("auto") and kb_embed.validate(cfg["embeddings"]) is None:
        embed_missing(con, cfg["embeddings"], limit=None, quiet=args.quiet)


# ----------------------------------------------------------------- search ---

def to_match(query: str, mode: str = "AND") -> str:
    if _FTS_OPS.search(query):
        return query
    terms = re.findall(r"[\w./-]+", query)[:MAX_QUERY_TERMS]
    terms = [t for t in (re.sub(r"[./-]+", " ", t).strip() for t in terms) if t]
    if not terms:
        die("empty query")
    return f" {mode} ".join(f'"{t}"' for t in terms)


def bm25_hits(con, match: str, n: int, kind: str | None, path_like: str | None) -> list[sqlite3.Row]:
    sql = f"""
      SELECT c.id, bm25(chunks_fts, {', '.join(map(str, BM25_WEIGHTS))}) AS score,
             snippet(chunks_fts, 3, '«', '»', ' … ', 14) AS snip
      FROM chunks_fts JOIN chunks c ON c.id = chunks_fts.rowid
      JOIN files f ON f.path = c.path
      WHERE chunks_fts MATCH ?
        AND (? IS NULL OR f.kind = ?) AND (? IS NULL OR c.path LIKE ?)
      ORDER BY score LIMIT ?"""
    return con.execute(sql, (match, kind, kind, path_like, path_like, n)).fetchall()


def bm25_with_fallback(con, query: str, n: int, kind, path_like) -> list[sqlite3.Row]:
    try:
        rows = bm25_hits(con, to_match(query), n, kind, path_like)
        if not rows and not _FTS_OPS.search(query):
            rows = bm25_hits(con, to_match(query, "OR"), n, kind, path_like)
    except sqlite3.OperationalError as e:
        die(f"bad FTS5 query {query!r}: {e}")
    return rows


def vector_hits(con, query: str, ecfg: dict, n: int, kind, path_like) -> list[tuple[int, float]]:
    rows = con.execute(
        "SELECT e.chunk_id, e.vec FROM embeddings e JOIN chunks c ON c.id = e.chunk_id "
        "JOIN files f ON f.path = c.path WHERE e.model = ? "
        "AND (? IS NULL OR f.kind = ?) AND (? IS NULL OR c.path LIKE ?)",
        (kb_embed.model_id(ecfg), kind, kind, path_like, path_like)).fetchall()
    if not rows:
        return []
    try:
        qvec = kb_embed.embed_texts([query], ecfg)[0]
    except kb_embed.EmbedError as e:
        die(f"embedding the query failed: {e}")
    return kb_embed.top_k(qvec, [(r[0], r[1]) for r in rows], n)


def resolve_mode(con, requested: str, ecfg: dict) -> str:
    if requested != "auto":
        if requested != "bm25" and kb_embed.validate(ecfg):
            die(kb_embed.validate(ecfg))
        return requested
    if kb_embed.validate(ecfg):
        return "bm25"
    have = con.execute("SELECT 1 FROM embeddings WHERE model=? LIMIT 1",
                       (kb_embed.model_id(ecfg),)).fetchone()
    return "hybrid" if have else "bm25"


def search_ranked(con, args, cfg: dict, kind) -> tuple[list[tuple[int, float]], dict, str]:
    """Return ([(chunk_id, score)], {chunk_id: snippet}, mode)."""
    ecfg = cfg["embeddings"]
    mode = resolve_mode(con, args.mode, ecfg)
    pool = max(args.k * 5, 50)
    lex = [] if mode == "semantic" else bm25_with_fallback(con, args.query, pool, kind, args.path)
    snips = {r["id"]: r["snip"] for r in lex}
    if mode == "bm25":
        return [(r["id"], -r["score"]) for r in lex][:args.k], snips, mode
    vec = vector_hits(con, args.query, ecfg, pool, kind, args.path)
    if mode == "semantic":
        return vec[:args.k], snips, mode
    fused = kb_embed.rrf([r["id"] for r in lex], [cid for cid, _ in vec])
    return fused[:args.k], snips, mode


def print_chunk_hits(con, hits, snips: dict, mode: str) -> None:
    for cid, score in hits:
        r = con.execute("SELECT c.path, c.start_line, c.end_line, c.heading, x.body "
                        "FROM chunks c JOIN chunks_fts x ON x.rowid = c.id WHERE c.id=?",
                        (cid,)).fetchone()
        if r is None:
            continue
        head = f"  [{r['heading'][:90]}]" if r["heading"] else ""
        print(f"{r['path']}:{r['start_line']}-{r['end_line']}{head}  ({mode} {score:.3f})")
        snip = " ".join((snips.get(cid) or r["body"][:300]).split())
        print(f"    {snip[:300]}")


def cmd_search(args, root: Path, cfg: dict) -> None:
    con = connect(root)
    kind = None if args.kind in (None, "note") else args.kind
    if args.kind in (None, "note"):
        print_notes(con, root, args.query, min(args.k, 5), header=True)
        if args.kind == "note":
            return
    hits, snips, mode = search_ranked(con, args, cfg, kind)
    if not hits:
        print("no matches; try fewer/other terms, a prefix (term*), --mode semantic, or grep")
        return
    print_chunk_hits(con, hits, snips, mode)


def embed_missing(con, ecfg: dict, limit: int | None, quiet: bool = False) -> int:
    err = kb_embed.validate(ecfg)
    if err:
        die(err)
    model = kb_embed.model_id(ecfg)
    with con:
        con.execute("DELETE FROM embeddings WHERE model != ?", (model,))
    todo = con.execute(
        "SELECT c.id, c.path, c.heading, x.body FROM chunks c "
        "JOIN chunks_fts x ON x.rowid = c.id LEFT JOIN embeddings e ON e.chunk_id = c.id "
        "WHERE e.chunk_id IS NULL ORDER BY c.id LIMIT ?", (limit or -1,)).fetchall()
    batch, cap, done, t0 = int(ecfg.get("batch") or 32), int(ecfg.get("max_chars") or 2000), 0, time.time()
    for off in range(0, len(todo), batch):
        part = todo[off:off + batch]
        texts = [f"{r['path']}\n{r['heading']}\n{r['body']}"[:cap] for r in part]
        try:
            blobs = kb_embed.embed_texts(texts, ecfg)
        except kb_embed.EmbedError as e:
            die(f"embedding failed after {done} chunks: {e}")
        with con:
            con.executemany("INSERT OR REPLACE INTO embeddings VALUES(?,?,?)",
                            [(r["id"], model, b) for r, b in zip(part, blobs)])
        done += len(part)
    if not quiet:
        print(f"kb: embedded {done} chunks with {model} in {time.time() - t0:.1f}s")
    return done


def cmd_embed(args, root: Path, cfg: dict) -> None:
    con = connect(root)
    embed_missing(con, cfg["embeddings"], args.limit)


def cmd_symbol(args, root: Path, cfg: dict) -> None:
    con = connect(root)
    name = args.name.lower()
    rows = con.execute(
        "SELECT name, kind, path, line FROM symbols WHERE lname = ? "
        "UNION ALL SELECT name, kind, path, line FROM symbols "
        "WHERE lname LIKE ? AND lname != ? LIMIT ?",
        (name, f"%{name}%", name, args.k)).fetchall()
    if not rows:
        print("no symbol matches (symbols are regex-extracted; try `search`)")
    for r in rows:
        print(f"{r['path']}:{r['line']}  {r['kind']:8} {r['name']}")


# ------------------------------------------------------------------ notes ---

def current_hash(con, root: Path, rel: str) -> str | None:
    full = root / rel
    if not full.is_file():
        return None
    row = con.execute("SELECT sha1, mtime, size FROM files WHERE path=?", (rel,)).fetchone()
    st = full.stat()
    if row and row["mtime"] == st.st_mtime and row["size"] == st.st_size:
        return row["sha1"]
    return sha1_of(full)


def note_is_stale(con, root: Path, paths_json: str) -> list[str]:
    stale = []
    for rel, h in json.loads(paths_json or "{}").items():
        if current_hash(con, root, rel) != h:
            stale.append(rel)
    return stale


def cmd_note(args, root: Path, cfg: dict) -> None:
    con = connect(root)
    if args.action == "add":
        if not args.topic or not args.body:
            die("note add needs --topic and --body")
        hashes = {}
        for rel in args.paths or []:
            rel = Path(rel).as_posix()
            h = current_hash(con, root, rel)
            if h is None:
                die(f"--paths entry not found: {rel}")
            hashes[rel] = h
        with con:
            cur = con.execute("INSERT INTO notes(topic,body,paths,created) VALUES(?,?,?,?)",
                              (args.topic, args.body, json.dumps(hashes), time.time()))
            con.execute("INSERT INTO notes_fts(rowid,topic,body,paths) VALUES(?,?,?,?)",
                        (cur.lastrowid, args.topic, args.body, " ".join(hashes)))
        print(f"note N{cur.lastrowid} recorded")
    elif args.action == "done":
        if not args.id:
            die("note done needs an id")
        with con:
            n = con.execute("UPDATE notes SET status='done' WHERE id=?", (args.id,)).rowcount
        print("marked done" if n else f"no note {args.id}")
    if cfg.get("auto_export_notes"):
        cmd_export_notes(args, root, cfg)


def print_notes(con, root: Path, query: str, k: int, header: bool = False) -> int:
    try:
        rows = con.execute(
            "SELECT n.*, bm25(notes_fts, 4.0, 1.0, 2.0) AS score FROM notes_fts "
            "JOIN notes n ON n.id = notes_fts.rowid WHERE notes_fts MATCH ? "
            "AND n.status='open' ORDER BY score LIMIT ?",
            (to_match(query, "OR"), k)).fetchall()
    except sqlite3.OperationalError:
        rows = []
    if rows and header:
        print("── notes ──")
    for r in rows:
        stale = note_is_stale(con, root, r["paths"])
        flag = f"  STALE({', '.join(stale)})" if stale else ""
        paths = ", ".join(json.loads(r["paths"] or "{}")) or "-"
        print(f"N{r['id']} {r['topic']}{flag}\n    paths: {paths}\n    {r['body'][:400]}")
    if rows and header:
        print("── files ──")
    return len(rows)


def cmd_notes(args, root: Path, cfg: dict) -> None:
    con = connect(root)
    if not print_notes(con, root, args.query, args.k):
        print("no open notes match")


def cmd_export_notes(args, root: Path, cfg: dict) -> None:
    con = connect(root)
    rows = con.execute("SELECT * FROM notes ORDER BY id").fetchall()
    out = ["# kb notes", "",
           "Agent-recorded findings (exported by `kb.py export-notes`; "
           "reload with `kb.py import-notes`). Durable facts should graduate "
           "into MAP.md or an area CLAUDE.md, then be marked done.", ""]
    for r in rows:
        created = time.strftime("%Y-%m-%d", time.gmtime(r["created"]))
        meta = json.dumps({"paths": json.loads(r["paths"] or "{}")})
        out += [f"## N{r['id']}: {r['topic']}", "",
                f"- status: {r['status']}", f"- created: {created}",
                f"- paths: {', '.join(json.loads(r['paths'] or '{}')) or '-'}",
                f"<!-- kb: {meta} -->", "", r["body"].rstrip(), ""]
    dest = root / NOTES_REL
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(out), encoding="utf-8")
    print(f"wrote {len(rows)} notes to {NOTES_REL}")


_NOTE_BLOCK = re.compile(
    r"^## N(\d+)(?:: | \u2014 )(.*?)\n\n- status: (\w+)\n- created: ([\d-]+)\n- paths: .*?\n"
    r"<!-- kb: (.*?) -->\n\n(.*?)(?=^## N\d+(?:: | \u2014 )|\Z)", re.S | re.M)


def cmd_import_notes(args, root: Path, cfg: dict) -> None:
    src = root / NOTES_REL
    if not src.is_file():
        die(f"{NOTES_REL} not found")
    con = connect(root)
    n = 0
    with con:
        con.execute("DELETE FROM notes")
        con.execute("DELETE FROM notes_fts")
        for m in _NOTE_BLOCK.finditer(src.read_text(encoding="utf-8")):
            nid, topic, status, created, meta, body = m.groups()
            paths = json.loads(meta).get("paths", {})
            ts = time.mktime(time.strptime(created, "%Y-%m-%d"))
            con.execute("INSERT INTO notes VALUES(?,?,?,?,?,?)",
                        (int(nid), topic, body.strip(), json.dumps(paths), ts, status))
            con.execute("INSERT INTO notes_fts(rowid,topic,body,paths) VALUES(?,?,?,?)",
                        (int(nid), topic, body.strip(), " ".join(paths)))
            n += 1
    print(f"imported {n} notes")


# ----------------------------------------------------------- areas/check ---

def find_areas(root: Path, cfg: dict, files: list[str]) -> list[str]:
    roots = [r for r in cfg["area_roots"] if (root / r).is_dir()]
    counts: dict[str, int] = {}
    for rel in files:
        for r in roots:
            prefix = r.rstrip("/") + "/"
            if rel.startswith(prefix):
                rest = rel[len(prefix):]
                if "/" in rest:
                    area = prefix + rest.split("/", 1)[0]
                    counts[area] = counts.get(area, 0) + 1
    if not roots or cfg.get("top_level_areas"):  # big top-level dirs are areas too
        for rel in files:
            top = rel.split("/", 1)[0]
            if "/" in rel and top not in roots:
                counts[top] = counts.get(top, 0) + 1
    documented = {Path(f).parent.as_posix() for f in files
                  if Path(f).name in AREA_DOC_NAMES and "/" in f
                  and any(f.startswith(r.rstrip("/") + "/") for r in roots)}
    areas = {a for a, c in counts.items() if c >= int(cfg["area_min_files"])} | documented
    return sorted(a for a in areas if not _match_any(a, cfg["area_exclude"])
                  and not a.split("/")[-1].startswith("."))


def area_doc(root: Path, area: str) -> Path | None:
    for name in AREA_DOC_NAMES:
        if (root / area / name).is_file():
            return root / area / name
    return None


def map_path(root: Path, cfg: dict) -> Path | None:
    for cand in ([cfg["map_file"]] if cfg.get("map_file") else []) + list(MAP_CANDIDATES):
        if (root / cand).is_file():
            return root / cand
    return None


def cmd_areas(args, root: Path, cfg: dict) -> None:
    files = list_files(root, cfg)
    for a in find_areas(root, cfg, files):
        doc = area_doc(root, a)
        state = "MISSING"
        if doc:
            state = "stub" if "STATUS: stub" in doc.read_text(encoding="utf-8", errors="replace") else "ok"
        n = sum(1 for f in files if f.startswith(a + "/"))
        print(f"{state:8} {a}/  ({n} files)")


def broken_links(root: Path, doc: Path) -> list[str]:
    text = doc.read_text(encoding="utf-8", errors="replace")
    text = re.sub(r"(?ms)^\s*(```|~~~).*?^\s*\1", "", text)  # fenced blocks
    text = re.sub(r"`[^`\n]*`", "", text)                       # inline code
    bad = []
    for target in _LINK.findall(text):
        if re.match(r"^[a-z]+:", target) or target.startswith("{{"):
            continue
        if not (doc.parent / target).exists() and not (root / target.lstrip("/")).exists():
            bad.append(target)
    if doc.name in AREA_DOC_NAMES:
        for target in _AT_REF.findall(text):
            if not (root / target).exists() and not (doc.parent / target).exists():
                bad.append("@" + target)
    return sorted(set(bad))


def overlap_ratio(a: Path, b: Path) -> float:
    def long_lines(p: Path) -> set[str]:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        return {ln.strip() for ln in lines if len(ln.strip()) > 20}
    la, lb = long_lines(a), long_lines(b)
    return len(la & lb) / max(1, min(len(la), len(lb)))


def chars_of(p: Path) -> int:
    return len(p.read_text(encoding="utf-8", errors="replace"))


def over_budget(p: Path, cap: int) -> str | None:
    n = chars_of(p)
    return f"{n:,} chars (~{n // 4:,} tokens; budget {cap:,} chars)" if n > cap else None


def check_root_docs(root: Path, cfg: dict, err: list, warn: list) -> Path | None:
    budgets = cfg["budgets"]
    claude, agents = root / "CLAUDE.md", root / "AGENTS.md"
    if not claude.is_file():
        err.append("missing root CLAUDE.md")
    elif over_budget(claude, budgets["root_chars"]):
        warn.append(f"CLAUDE.md is {over_budget(claude, budgets['root_chars'])}; it loads every turn")
    if not agents.is_file():
        warn.append("no AGENTS.md pointer (other agents won't find CLAUDE.md)")
    elif claude.is_file() and overlap_ratio(claude, agents) > 0.5:
        ratio = overlap_ratio(claude, agents)
        warn.append(f"AGENTS.md duplicates CLAUDE.md ({ratio:.0%} shared lines); make it a pointer, copies drift")
    mp = map_path(root, cfg)
    if mp is None:
        err.append("missing PROJECT_MAP.md (or MAP.md / map_file in .claude/kb.json)")
    elif over_budget(mp, budgets["map_chars"]):
        warn.append(f"{mp.relative_to(root)} is {over_budget(mp, budgets['map_chars'])}; move detail/history out")
    return mp


def check_areas(root: Path, cfg: dict, mp: Path | None, err: list, warn: list) -> list[Path]:
    map_text = mp.read_text(encoding="utf-8", errors="replace") if mp else ""
    docs = []
    for a in find_areas(root, cfg, list_files(root, cfg)):
        doc = area_doc(root, a)
        if doc is None:
            err.append(f"area {a}/ has no CLAUDE.md (kb.py scaffold, or add to area_exclude with a reason)")
            continue
        docs.append(doc)
        text = doc.read_text(encoding="utf-8", errors="replace")
        if "STATUS: stub" in text:
            warn.append(f"{doc.relative_to(root)} is still a stub")
        big = over_budget(doc, cfg["budgets"]["area_chars"])
        if big:
            warn.append(f"{doc.relative_to(root)} is {big}; move deep reference to a sibling NOTES.md")
        if mp and a not in map_text and a.split("/")[-1] + "/" not in map_text:
            warn.append(f"area {a}/ not mentioned in {mp.relative_to(root)}")
    return docs


def cmd_check(args, root: Path, cfg: dict) -> None:
    err: list[str] = []
    warn: list[str] = []
    mp = check_root_docs(root, cfg, err, warn)
    docs = check_areas(root, cfg, mp, err, warn)
    for d in [p for p in (root / "CLAUDE.md", root / "AGENTS.md", mp) if p and p.is_file()] + docs:
        for t in broken_links(root, d):
            err.append(f"{d.relative_to(root)}: broken link {t}")
        n = kb_template.unfilled(d.read_text(encoding="utf-8", errors="replace"))
        if n:
            warn.append(f"{d.relative_to(root)} has {n} unfilled {{{{…}}}} marker(s)")
    if (root / DB_REL).is_file():
        con = connect(root)
        for r in con.execute("SELECT id, topic, paths FROM notes WHERE status='open'"):
            if note_is_stale(con, root, r["paths"]):
                warn.append(f"note N{r['id']} ({r['topic']}) is stale; re-verify, update or `note done`")
    else:
        warn.append("no index yet (kb.py init && kb.py index)")
    for e in err:
        print(f"ERROR  {e}")
    for w in warn:
        print(f"warn   {w}")
    print(f"kb check: {len(err)} error(s), {len(warn)} warning(s)")
    sys.exit(1 if err else 0)


def scaffold_body(root: Path, area: str, template: str, files: list[str]) -> str:
    rel_files = [f for f in files if f.startswith(area + "/")]
    lines = []
    for f in rel_files[:40]:
        text = read_text(root / f) or ""
        syms = [s for s, _, _ in extract_symbols(text.splitlines(), 1)][:6] if file_kind(f) == "code" else []
        extra = f" (defines {', '.join(syms)})" if syms else ""
        lines.append(f"- **`{f[len(area) + 1:]}`**: {{{{responsibility}}}}{extra}")
    if len(rel_files) > 40:
        lines.append(f"- … and {len(rel_files) - 40} more files")
    body = template.replace("{{AREA_PATH}}", area).replace("{{AREA_TITLE}}", Path(area).name)
    body = body.replace("{{stub | current}}", "stub").replace("{{YYYY-MM-DD}}", "never")
    marker = "- **`{{file_or_subdir}}`**: {{responsibility}}. {{why it's shaped this way, if non-obvious}}\n- …"
    return body.replace(marker, "\n".join(lines) or marker)


def templates_dir() -> Path:
    here = Path(__file__).resolve().parent
    for cand in (here / "templates", here.parent / "templates"):
        if (cand / "CLAUDE.area.md").is_file():
            return cand
    die("templates/ not found next to kb.py (re-run `kb.py vendor` from the plugin)")


def cmd_scaffold(args, root: Path, cfg: dict) -> None:
    template = (templates_dir() / "CLAUDE.area.md").read_text(encoding="utf-8")
    files = list_files(root, cfg)
    made = 0
    for a in find_areas(root, cfg, files):
        if area_doc(root, a):
            continue
        dest = root / a / "CLAUDE.md"
        print(f"{'would create' if args.dry_run else 'created'} {dest.relative_to(root)}")
        if not args.dry_run:
            dest.write_text(scaffold_body(root, a, template, files), encoding="utf-8")
        made += 1
    dry = " (dry run)" if args.dry_run else ""
    print(f"{made} stub(s){dry}; fill them by reading the code (flagged until STATUS changes)")


# ------------------------------------------------------------ init/stats ---

def cmd_init(args, root: Path, cfg: dict) -> None:
    con = connect(root, must_exist=False)
    ensure_schema(con)
    cfg_path = root / CONFIG_REL
    if not cfg_path.is_file():
        seed = {"_comment": "kb.py config; '*' in globs matches across '/'. See .claude/skills/project-map/SKILL.md"}
        seed.update(DEFAULT_CONFIG)
        cfg_path.write_text(json.dumps(seed, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {CONFIG_REL}")
    gi = root / ".gitignore"
    line = ".claude/kb/"
    if not gi.is_file() or line not in gi.read_text(encoding="utf-8", errors="replace").split():
        with gi.open("a", encoding="utf-8") as f:
            f.write(f"\n# derived kb.py search index\n{line}\n")
        print("added .claude/kb/ to .gitignore")
    if (root / NOTES_REL).is_file() and not con.execute("SELECT 1 FROM notes LIMIT 1").fetchone():
        print(f"found {NOTES_REL}; run `kb.py import-notes` to load it")
    print(f"index ready at {DB_REL}; next: kb.py index")


def cmd_stats(args, root: Path, cfg: dict) -> None:
    con = connect(root)
    for row in con.execute("SELECT kind, COUNT(*) n, SUM(lines) l FROM files GROUP BY kind ORDER BY n DESC"):
        print(f"{row['kind']:7} {row['n']:6} files {row['l'] or 0:9} lines")
    chunks = con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    syms = con.execute("SELECT COUNT(*) FROM symbols").fetchone()[0]
    notes = con.execute("SELECT COUNT(*) FROM notes WHERE status='open'").fetchone()[0]
    size_mb = (root / DB_REL).stat().st_size / 1e6
    emb = con.execute("SELECT model, COUNT(*) FROM embeddings GROUP BY model").fetchall()
    emb_s = ", ".join(f"{m}={n}" for m, n in emb) or "none"
    print(f"chunks {chunks} | symbols {syms} | notes open {notes} | embeddings {emb_s} | db {size_mb:.1f} MB")


def template_facts(root: Path, cfg: dict, args, files: list[str]) -> dict:
    kind = args.kind or kb_template.detect_kind(files)
    return {
        "name": args.name or kb_template.detect_name(root),
        "kind": kind,
        "langs": kb_template.detect_languages(files),
        "commands": kb_template.detect_commands(root),
        "readme": kb_template.readme_paragraph(root),
        "areas": find_areas(root, cfg, files),
        "tree": kb_template.layout_tree(files, [r for r in cfg["area_roots"] if "/" not in r]),
        "entry_points": kb_template.entry_points(files),
    }


def cmd_template(args, root: Path, cfg: dict) -> None:
    """Template the whole project in one go; never overwrites."""
    files = list_files(root, cfg)
    facts = template_facts(root, cfg, args, files)
    today = time.strftime("%Y-%m-%d")
    created, skipped = [], []
    for rel, body in kb_template.plan(root, templates_dir(), facts):
        body = body.replace("{{YYYY-MM-DD}}: {{title}}", f"{today}: project templated with project-map")
        if (root / rel).exists():
            skipped.append(rel)
            continue
        created.append(rel)
        if not args.dry_run:
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(body, encoding="utf-8")
    print(f"project: {facts['name']} | kind: {facts['kind']} | languages: {', '.join(facts['langs']) or '-'}")
    print(f"commands: {facts['commands'] or 'none detected'}")
    for rel in created:
        print(f"{'would create' if args.dry_run else 'created'} {rel}")
    for rel in skipped:
        print(f"kept existing {rel}")
    cmd_scaffold(args, root, cfg)
    if args.dry_run:
        return
    cmd_vendor(argparse.Namespace(hook=not args.no_hook, mcp=args.mcp), root, cfg)
    cmd_init(args, root, cfg)
    cmd_index(argparse.Namespace(full=False, quiet=False, if_initialized=False), root, load_config(root))
    print("\nnext: fill the {{…}} markers from the code (the WHY per area, hard rules, invariants),")
    print("      then run `kb.py check` until it reports 0 errors. Markers are counted as warnings.")


def merge_json(path: Path, mutate) -> None:
    data = {}
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            die(f"{path}: not valid JSON ({e}); fix it before vendoring")
    mutate(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


HOOK_CMD = ('python3 "$CLAUDE_PROJECT_DIR/.claude/tools/kb.py" index --quiet --if-initialized'
            ' || python "$CLAUDE_PROJECT_DIR/.claude/tools/kb.py" index --quiet --if-initialized || true')


def _add_hook(data: dict) -> None:
    starts = data.setdefault("hooks", {}).setdefault("SessionStart", [])
    if any("kb.py" in h.get("command", "") for e in starts for h in e.get("hooks", [])):
        return
    starts.append({"hooks": [{"type": "command", "command": HOOK_CMD}]})


def _add_mcp(data: dict) -> None:
    data.setdefault("mcpServers", {})["project-kb"] = {
        "command": "python3", "args": [".claude/tools/kb_mcp.py"]}


def cmd_vendor(args, root: Path, cfg: dict) -> None:
    """Copy the tools into the project so it works without the plugin."""
    src = Path(__file__).resolve().parent
    dest = root / VENDOR_REL
    dest.mkdir(parents=True, exist_ok=True)
    for name in VENDOR_FILES:
        if (src / name).resolve() != (dest / name).resolve():
            shutil.copy2(src / name, dest / name)
    tsrc = templates_dir()
    if tsrc.resolve() != (dest / "templates").resolve():
        shutil.copytree(tsrc, dest / "templates", dirs_exist_ok=True)
    print(f"copied {', '.join(VENDOR_FILES)} + templates/ to {VENDOR_REL}/")
    if args.hook:
        merge_json(root / ".claude/settings.json", _add_hook)
        print("added SessionStart index hook to .claude/settings.json")
    if args.mcp:
        merge_json(root / ".mcp.json", _add_mcp)
        print("added project-kb MCP server to .mcp.json")


# ------------------------------------------------------------------- main ---

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="kb.py", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--root", help="project root (default: git toplevel)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    s = sub.add_parser("index")
    s.add_argument("--full", action="store_true")
    s.add_argument("--quiet", action="store_true")
    s.add_argument("--if-initialized", action="store_true",
                   help="no-op unless .claude/kb.json exists (for hooks)")
    s = sub.add_parser("search")
    s.add_argument("query")
    s.add_argument("-k", type=int, default=10)
    s.add_argument("--kind", choices=["md", "code", "text", "note"])
    s.add_argument("--path", help="SQL LIKE filter, e.g. 'src/api/%%'")
    s.add_argument("--mode", choices=["auto", "bm25", "semantic", "hybrid"], default="auto",
                   help="auto = hybrid when embeddings exist, else bm25")
    s = sub.add_parser("symbol")
    s.add_argument("name")
    s.add_argument("-k", type=int, default=20)
    sub.add_parser("areas")
    sub.add_parser("check")
    s = sub.add_parser("scaffold")
    s.add_argument("--dry-run", action="store_true")
    s = sub.add_parser("note")
    s.add_argument("action", choices=["add", "done"])
    s.add_argument("id", nargs="?", type=int)
    s.add_argument("--topic")
    s.add_argument("--body")
    s.add_argument("--paths", nargs="*")
    s = sub.add_parser("notes")
    s.add_argument("query")
    s.add_argument("-k", type=int, default=10)
    sub.add_parser("export-notes")
    sub.add_parser("import-notes")
    s = sub.add_parser("embed", help="embed chunks missing a vector (needs \"embeddings\" config)")
    s.add_argument("--limit", type=int)
    s = sub.add_parser("template", help="template the whole project (docs, area stubs, tools, index)")
    s.add_argument("--kind", choices=["code", "docs", "mixed"])
    s.add_argument("--name")
    s.add_argument("--dry-run", action="store_true")
    s.add_argument("--no-hook", action="store_true", help="don't add the SessionStart index hook")
    s.add_argument("--mcp", action="store_true", help="register the MCP server in .mcp.json")
    s = sub.add_parser("vendor", help=f"copy tools into {VENDOR_REL}/ for plugin-less use")
    s.add_argument("--hook", action="store_true", help="also add the SessionStart index hook")
    s.add_argument("--mcp", action="store_true", help="also register the MCP server in .mcp.json")
    sub.add_parser("stats")
    return p


COMMANDS = {
    "init": cmd_init, "index": cmd_index, "search": cmd_search,
    "symbol": cmd_symbol, "areas": cmd_areas, "check": cmd_check,
    "scaffold": cmd_scaffold, "note": cmd_note, "notes": cmd_notes,
    "export-notes": cmd_export_notes, "import-notes": cmd_import_notes,
    "stats": cmd_stats, "embed": cmd_embed, "vendor": cmd_vendor,
    "template": cmd_template,
}


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    root = find_root(args.root)
    COMMANDS[args.cmd](args, root, load_config(root))


if __name__ == "__main__":
    main()
