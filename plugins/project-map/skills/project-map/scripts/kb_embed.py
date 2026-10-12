"""kb_embed.py: optional embedding backends + vector math for kb.py.

BM25 stays the default and needs nothing. Embeddings are opt-in via the
"embeddings" block in .claude/kb.json:

    "embeddings": {"backend": "ollama", "model": "nomic-embed-text",
                   "url": "http://localhost:11434"}

Backends
  openai                 any OpenAI-compatible /v1/embeddings endpoint
                         (OpenAI, Ollama /v1, LM Studio, vLLM, llama.cpp,
                         TEI). Key read from the env var named in
                         "api_key_env" (never stored in config).
  ollama                 native Ollama /api/embed
  sentence-transformers  in-process; `pip install sentence-transformers`
  fastembed              in-process ONNX; `pip install fastembed`
  hash                   stdlib feature hashing. NOT semantic; it only
                         smooths lexical matching. For tests / air-gapped use.

Vectors are L2-normalised float32 blobs, so cosine == dot product. numpy is
used when installed; otherwise a pure-Python scan (fine to ~50k chunks).
"""
from __future__ import annotations

import array
import hashlib
import json
import math
import os
import re
import urllib.error
import urllib.request

HTTP_TIMEOUT_S = 120
HASH_DIM_DEFAULT = 256


class EmbedError(Exception):
    """Backend unavailable or returned something unusable."""


# ------------------------------------------------------------ backends ---

def _post_json(url: str, payload: dict, api_key: str | None) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise EmbedError(f"{url}: {e}") from e


def _embed_openai(texts: list[str], cfg: dict) -> list[list[float]]:
    base = (cfg.get("url") or "https://api.openai.com/v1").rstrip("/")
    key_env = cfg.get("api_key_env")
    key = os.environ.get(key_env) if key_env else None
    if key_env and not key:
        raise EmbedError(f"env var {key_env} (api_key_env) is not set")
    out = _post_json(f"{base}/embeddings", {"model": cfg["model"], "input": texts}, key)
    rows = sorted(out.get("data", []), key=lambda d: d.get("index", 0))
    if len(rows) != len(texts):
        raise EmbedError(f"expected {len(texts)} embeddings, got {len(rows)}")
    return [r["embedding"] for r in rows]


def _embed_ollama(texts: list[str], cfg: dict) -> list[list[float]]:
    base = (cfg.get("url") or "http://localhost:11434").rstrip("/")
    out = _post_json(f"{base}/api/embed", {"model": cfg["model"], "input": texts}, None)
    vecs = out.get("embeddings")
    if not isinstance(vecs, list) or len(vecs) != len(texts):
        raise EmbedError(f"ollama returned {type(vecs).__name__} for {len(texts)} inputs")
    return vecs


_ST_CACHE: dict = {}


def _embed_st(texts: list[str], cfg: dict) -> list[list[float]]:
    model = _ST_CACHE.get(("st", cfg["model"]))
    if model is None:
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
        except ImportError as e:
            raise EmbedError("pip install sentence-transformers") from e
        model = _ST_CACHE[("st", cfg["model"])] = SentenceTransformer(cfg["model"])
    return [list(map(float, v)) for v in model.encode(texts)]


def _embed_fastembed(texts: list[str], cfg: dict) -> list[list[float]]:
    model = _ST_CACHE.get(("fe", cfg["model"]))
    if model is None:
        try:
            from fastembed import TextEmbedding  # type: ignore
        except ImportError as e:
            raise EmbedError("pip install fastembed") from e
        model = _ST_CACHE[("fe", cfg["model"])] = TextEmbedding(model_name=cfg["model"])
    return [list(map(float, v)) for v in model.embed(texts)]


def _embed_hash(texts: list[str], cfg: dict) -> list[list[float]]:
    dim = int(cfg.get("dim") or HASH_DIM_DEFAULT)
    out = []
    for t in texts:
        v = [0.0] * dim
        words = re.findall(r"[a-z0-9]+", t.lower())
        feats = words + [a + "_" + b for a, b in zip(words, words[1:])]
        for f in feats:
            h = int.from_bytes(hashlib.blake2b(f.encode(), digest_size=8).digest(), "little")
            v[h % dim] += 1.0 if (h >> 63) else -1.0
        out.append(v)
    return out


BACKENDS = {
    "openai": _embed_openai,
    "ollama": _embed_ollama,
    "sentence-transformers": _embed_st,
    "fastembed": _embed_fastembed,
    "hash": _embed_hash,
}


def model_id(cfg: dict) -> str:
    """Identity stored with each vector; changing it invalidates old vectors."""
    if cfg.get("backend") == "hash":
        return f"hash:{int(cfg.get('dim') or HASH_DIM_DEFAULT)}"
    return f"{cfg.get('backend')}:{cfg.get('model')}"


def validate(cfg: dict | None) -> str | None:
    """Return an error string, or None if the config is usable."""
    if not cfg or not cfg.get("backend"):
        return "embeddings are not configured (set \"embeddings\" in .claude/kb.json)"
    if cfg["backend"] not in BACKENDS:
        return f"unknown backend {cfg['backend']!r}; choose from {sorted(BACKENDS)}"
    if cfg["backend"] != "hash" and not cfg.get("model"):
        return f"backend {cfg['backend']!r} needs \"model\""
    return None


def embed_texts(texts: list[str], cfg: dict) -> list[bytes]:
    """Embed and return normalised float32 blobs, one per text."""
    assert texts, "embed_texts needs at least one text"
    vecs = BACKENDS[cfg["backend"]](texts, cfg)
    blobs = [to_blob(v) for v in vecs]
    assert len(blobs) == len(texts)
    return blobs


# ---------------------------------------------------------- vector math ---

def to_blob(vec) -> bytes:
    norm = math.sqrt(sum(float(x) * float(x) for x in vec)) or 1.0
    return array.array("f", (float(x) / norm for x in vec)).tobytes()


def from_blob(blob: bytes) -> array.array:
    a = array.array("f")
    a.frombytes(blob)
    return a


def top_k(query_blob: bytes, rows: list[tuple[int, bytes]], k: int) -> list[tuple[int, float]]:
    """rows = [(chunk_id, blob)] -> [(chunk_id, cosine)] best first."""
    if not rows:
        return []
    try:
        import numpy as np  # type: ignore
        q = np.frombuffer(query_blob, dtype=np.float32)
        mat = np.frombuffer(b"".join(b for _, b in rows), dtype=np.float32).reshape(len(rows), -1)
        scores = mat @ q
        idx = np.argsort(-scores)[:k]
        return [(rows[i][0], float(scores[i])) for i in idx]
    except ImportError:
        q = from_blob(query_blob)
        scored = [(cid, sum(a * b for a, b in zip(q, from_blob(blob)))) for cid, blob in rows]
        scored.sort(key=lambda t: -t[1])
        return scored[:k]


def rrf(*ranked: list[int], k: int = 60) -> list[tuple[int, float]]:
    """Reciprocal-rank fusion of several best-first id lists."""
    score: dict[int, float] = {}
    for lst in ranked:
        for rank, cid in enumerate(lst):
            score[cid] = score.get(cid, 0.0) + 1.0 / (k + rank + 1)
    return sorted(score.items(), key=lambda t: -t[1])
