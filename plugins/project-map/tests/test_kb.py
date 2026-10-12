"""End-to-end tests for kb.py / kb_embed.py / kb_mcp.py. Stdlib only.

    python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import http.server
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "skills" / "project-map" / "scripts"
KB = SCRIPTS / "kb.py"
MCP = SCRIPTS / "kb_mcp.py"
sys.path.insert(0, str(SCRIPTS))
import kb_embed  # noqa: E402


def write(root: Path, rel: str, text: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def make_project(root: Path) -> None:
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    write(root, "src/auth/session.py",
          "class SessionStore:\n    \"\"\"Login sessions with TTL.\"\"\"\n"
          "    def create(self, user_id):\n        return token_for(user_id)\n\n\n"
          "def token_for(user_id):\n    # hmac over user id and expiry\n    return 't'\n")
    write(root, "src/auth/refresh.py", "def refresh_token():\n    pass\n")
    write(root, "src/auth/keys.py", "def rotate_keys():\n    pass\n")
    write(root, "src/billing/invoice.ts",
          "export function computeInvoiceTotal(lines) {\n  return 0;\n}\n"
          "export const applyTax = (x) => x;\n")
    write(root, "src/billing/a.ts", "export const a = 1;\n")
    write(root, "src/billing/b.ts", "export const b = 2;\n")
    write(root, "docs/guide.md", "# Guide\n\n## Billing\n\nInvoices are computed in cents to avoid float drift.\n")


class KbTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="kbtest-"))
        make_project(self.tmp)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def kb(self, *args: str, ok: bool = True) -> str:
        r = subprocess.run([sys.executable, str(KB), "--root", str(self.tmp), *args],
                           capture_output=True, text=True)
        if ok and r.returncode != 0:
            self.fail(f"kb {args} exited {r.returncode}:\n{r.stdout}\n{r.stderr}")
        return r.stdout + r.stderr

    def set_config(self, **kv) -> None:
        cfg_path = self.tmp / ".claude/kb.json"
        cfg = json.loads(cfg_path.read_text())
        cfg.update(kv)
        cfg_path.write_text(json.dumps(cfg))


class CoreTests(KbTestCase):
    def test_index_search_symbol(self):
        self.kb("init")
        self.assertIn(".claude/kb/", (self.tmp / ".gitignore").read_text())
        self.assertIn("+", self.kb("index"))
        out = self.kb("search", "invoice cents")
        self.assertIn("docs/guide.md", out.splitlines()[0])
        self.assertIn("src/billing/invoice.ts:1", self.kb("symbol", "computeInvoiceTotal"))
        # camelCase is split so natural words find the symbol
        self.assertIn("invoice.ts", self.kb("search", "compute invoice total", "--kind", "code"))
        self.assertIn("0 chunks written", self.kb("index"))  # incremental no-op

    def test_or_fallback_and_path_filter(self):
        self.kb("init"), self.kb("index")
        out = self.kb("search", "hmac nonexistentword")
        self.assertIn("session.py", out)
        self.assertNotIn("session.py", self.kb("search", "hmac", "--path", "docs/%"))

    def test_if_initialized_is_noop_without_config(self):
        self.kb("index", "--if-initialized")
        self.assertFalse((self.tmp / ".claude/kb/index.sqlite").exists())

    def test_notes_stale_export_import(self):
        self.kb("init"), self.kb("index")
        self.kb("note", "add", "--topic", "tokens use hmac", "--body", "verified at session.py:8",
                "--paths", "src/auth/session.py")
        exported = (self.tmp / "docs/kb-notes.md").read_text()  # auto-exported
        self.assertIn("tokens use hmac", exported)
        self.assertNotIn("STALE", self.kb("notes", "hmac"))
        write(self.tmp, "src/auth/session.py", "# rewritten\n")
        self.assertIn("STALE(src/auth/session.py)", self.kb("notes", "hmac"))
        (self.tmp / ".claude/kb/index.sqlite").unlink()
        self.kb("init"), self.kb("import-notes")
        self.assertIn("tokens use hmac", self.kb("notes", "hmac"))

    def test_check_scaffold_and_pass(self):
        self.kb("init"), self.kb("index")
        out = self.kb("check", ok=False)
        self.assertIn("missing root CLAUDE.md", out)
        self.assertIn("area src/auth/ has no CLAUDE.md", out)
        self.kb("scaffold")
        stub = (self.tmp / "src/auth/CLAUDE.md").read_text()
        self.assertIn("STATUS: stub", stub)
        self.assertIn("token_for", stub)  # pre-filled symbol hints
        write(self.tmp, "CLAUDE.md", "# Demo\n\nSee @PROJECT_MAP.md\n")
        write(self.tmp, "AGENTS.md", "Read [CLAUDE.md](CLAUDE.md).\n")
        write(self.tmp, "PROJECT_MAP.md",
              "# Map\n\n- [auth](src/auth/CLAUDE.md) src/auth/\n- [billing](src/billing/CLAUDE.md) src/billing/\n"
              "- docs/\n")
        for area in ("src/auth", "src/billing", "docs"):
            p = self.tmp / area / "CLAUDE.md"
            p.write_text(p.read_text().replace("STATUS: stub", "STATUS: current")
                         if p.exists() else "# docs\nSTATUS: current\n")
        out = self.kb("check")
        self.assertIn("0 error(s)", out)

    def test_broken_links_ignore_code(self):
        self.kb("init")
        write(self.tmp, "CLAUDE.md", "real [x](missing.md) and `[y](ignored.md)`\n")
        out = self.kb("check", ok=False)
        self.assertIn("broken link missing.md", out)
        self.assertNotIn("ignored.md", out)

    def test_agents_duplicate_warning(self):
        self.kb("init")
        body = "\n".join(f"- rule number {i} is a long enough line to count" for i in range(20))
        write(self.tmp, "CLAUDE.md", body)
        write(self.tmp, "AGENTS.md", body)
        self.assertIn("AGENTS.md duplicates CLAUDE.md", self.kb("check", ok=False))


class EmbeddingTests(KbTestCase):
    def test_hash_backend_hybrid_and_semantic(self):
        self.kb("init"), self.kb("index")
        self.set_config(embeddings={"backend": "hash", "dim": 128})
        self.assertIn("embedded", self.kb("embed"))
        out = self.kb("search", "invoice cents")
        self.assertIn("(hybrid", out)
        self.assertIn("docs/guide.md", out)
        self.assertIn("(semantic", self.kb("search", "invoice", "--mode", "semantic"))
        self.assertIn("(bm25", self.kb("search", "invoice", "--mode", "bm25"))
        # a changed file drops and re-embeds only its chunks
        write(self.tmp, "docs/guide.md", "# Guide\n\nnew text about ledgers\n")
        self.kb("index")
        self.assertIn("embedded 1 chunks", self.kb("embed"))
        self.assertIn("embeddings hash:128=", self.kb("stats"))

    def test_changing_model_invalidates_vectors(self):
        self.kb("init"), self.kb("index")
        self.set_config(embeddings={"backend": "hash", "dim": 64})
        n1 = self.kb("embed")
        self.set_config(embeddings={"backend": "hash", "dim": 32})
        self.assertEqual(n1.split()[2], self.kb("embed").split()[2])
        self.assertNotIn("hash:64", self.kb("stats"))

    def test_semantic_without_config_errors(self):
        self.kb("init"), self.kb("index")
        self.assertIn("not configured", self.kb("search", "x", "--mode", "semantic", ok=False))


class _MockEmbedHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        texts = body["input"]
        vecs = kb_embed._embed_hash(texts, {"dim": 16})
        if self.path.endswith("/api/embed"):
            payload = {"embeddings": vecs}
        else:
            self.server.auth = self.headers.get("Authorization")
            payload = {"data": [{"index": i, "embedding": v} for i, v in enumerate(vecs)]}
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


class HttpBackendTests(KbTestCase):
    def setUp(self):
        super().setUp()
        self.srv = http.server.HTTPServer(("127.0.0.1", 0), _MockEmbedHandler)
        self.srv.auth = None
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()
        super().tearDown()

    def test_openai_compatible(self):
        os.environ["KB_TEST_KEY"] = "sekret"
        self.kb("init"), self.kb("index")
        self.set_config(embeddings={"backend": "openai", "model": "m", "url": self.url + "/v1",
                                    "api_key_env": "KB_TEST_KEY", "batch": 3})
        self.assertIn("embedded", self.kb("embed"))
        self.assertEqual(self.srv.auth, "Bearer sekret")
        self.assertIn("(hybrid", self.kb("search", "invoice"))

    def test_ollama(self):
        self.kb("init"), self.kb("index")
        self.set_config(embeddings={"backend": "ollama", "model": "nomic", "url": self.url})
        self.assertIn("embedded", self.kb("embed"))

    def test_unreachable_backend_fails_cleanly(self):
        self.kb("init"), self.kb("index")
        self.set_config(embeddings={"backend": "ollama", "model": "x", "url": "http://127.0.0.1:9"})
        self.assertIn("embedding failed", self.kb("embed", ok=False))


class McpTests(KbTestCase):
    def rpc(self, msgs: list[dict]) -> list[dict]:
        env = dict(os.environ, CLAUDE_PROJECT_DIR=str(self.tmp))
        inp = "".join(json.dumps(m) + "\n" for m in msgs)
        r = subprocess.run([sys.executable, str(MCP)], input=inp, capture_output=True,
                           text=True, env=env, timeout=60)
        return [json.loads(ln) for ln in r.stdout.splitlines() if ln.strip()]

    def test_protocol_roundtrip(self):
        self.kb("init"), self.kb("index")
        out = self.rpc([
            {"jsonrpc": "2.0", "id": 1, "method": "initialize",
             "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t"}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
             "params": {"name": "kb_search", "arguments": {"query": "invoice cents", "k": 3}}},
            {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
             "params": {"name": "kb_note_add", "arguments": {"topic": "t", "body": "b",
                                                              "paths": ["docs/guide.md"]}}},
            {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
             "params": {"name": "kb_search", "arguments": {}}},
            {"jsonrpc": "2.0", "id": 6, "method": "nope"},
        ])
        self.assertEqual([m["id"] for m in out], [1, 2, 3, 4, 5, 6])  # notification got no reply
        self.assertEqual(out[0]["result"]["serverInfo"]["name"], "project-kb")
        names = {t["name"] for t in out[1]["result"]["tools"]}
        self.assertTrue({"kb_search", "kb_symbol", "kb_note_add", "kb_check"} <= names)
        self.assertIn("docs/guide.md", out[2]["result"]["content"][0]["text"])
        self.assertFalse(out[3]["result"]["isError"])
        self.assertTrue(out[4]["result"]["isError"])  # missing required arg
        self.assertEqual(out[5]["error"]["code"], -32601)

    def test_uninitialized_project_is_a_clean_error(self):
        out = self.rpc([{"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": "kb_search", "arguments": {"query": "x"}}}])
        self.assertTrue(out[0]["result"]["isError"])
        self.assertIn("kb.py init", out[0]["result"]["content"][0]["text"])


class VendorTests(KbTestCase):
    def test_vendor_hook_mcp_idempotent(self):
        write(self.tmp, ".claude/settings.json", json.dumps({"permissions": {"allow": ["Bash(ls)"]}}))
        for _ in range(2):
            self.kb("vendor", "--hook", "--mcp")
        for f in ("kb.py", "kb_embed.py", "kb_mcp.py"):
            self.assertTrue((self.tmp / ".claude/tools" / f).is_file())
        settings = json.loads((self.tmp / ".claude/settings.json").read_text())
        self.assertEqual(settings["permissions"]["allow"], ["Bash(ls)"])  # preserved
        self.assertEqual(len(settings["hooks"]["SessionStart"]), 1)  # not duplicated
        mcp = json.loads((self.tmp / ".mcp.json").read_text())
        self.assertIn("project-kb", mcp["mcpServers"])
        # the vendored copy runs standalone
        r = subprocess.run([sys.executable, str(self.tmp / ".claude/tools/kb.py"),
                            "--root", str(self.tmp), "init"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)


class TemplateTests(KbTestCase):
    def test_template_code_project(self):
        write(self.tmp, "pyproject.toml", '[project]\nname = "demo-app"\n[tool.ruff]\n')
        write(self.tmp, "README.md", "# Demo\n\nDemo app that bills customers and manages login sessions.\n")
        write(self.tmp, "tests/test_a.py", "def test_a():\n    pass\n")
        write(self.tmp, "tests/test_b.py", "def test_b():\n    pass\n")
        write(self.tmp, "tests/test_c.py", "def test_c():\n    pass\n")
        out = self.kb("template")
        self.assertIn("project: demo-app | kind: code", out)
        root_doc = (self.tmp / "CLAUDE.md").read_text()
        self.assertIn("# demo-app: Working Agreement", root_doc)
        self.assertIn("bills customers", root_doc)          # README paragraph
        self.assertIn("python -m pytest", root_doc)          # detected test command
        self.assertIn("ruff check .", root_doc)
        pmap = (self.tmp / "PROJECT_MAP.md").read_text()
        self.assertIn("[`src/auth/`](src/auth/CLAUDE.md)", pmap)
        self.assertIn("[`tests/`](tests/CLAUDE.md)", pmap)  # top-level area
        self.assertTrue((self.tmp / "docs/ENGINEERING.md").is_file())
        self.assertTrue((self.tmp / ".claude/tools/templates/CLAUDE.area.md").is_file())
        self.assertTrue((self.tmp / ".claude/kb/index.sqlite").is_file())
        self.assertIn("SessionStart", (self.tmp / ".claude/settings.json").read_text())
        chk = self.kb("check")
        self.assertIn("0 error(s)", chk)
        self.assertIn("unfilled", chk)

    def test_template_never_overwrites(self):
        write(self.tmp, "CLAUDE.md", "# mine\n")
        out = self.kb("template")
        self.assertIn("kept existing CLAUDE.md", out)
        self.assertEqual((self.tmp / "CLAUDE.md").read_text(), "# mine\n")

    def test_template_docs_project_and_dry_run(self):
        shutil.rmtree(self.tmp / "src")
        for d in ("guide", "reference"):
            for i in range(4):
                write(self.tmp, f"{d}/p{i}.md", f"# {d} {i}\n\ntext\n")
        out = self.kb("template", "--dry-run")
        self.assertIn("kind: docs", out)
        self.assertIn("would create docs/STYLE.md", out)
        self.assertFalse((self.tmp / "CLAUDE.md").exists())

    def test_vendored_copy_can_template(self):
        self.kb("vendor")
        other = Path(tempfile.mkdtemp(prefix="kbtest2-"))
        try:
            make_project(other)
            r = subprocess.run([sys.executable, str(self.tmp / ".claude/tools/kb.py"),
                                "--root", str(other), "template"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertTrue((other / "PROJECT_MAP.md").is_file())
        finally:
            shutil.rmtree(other, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
