"""Chunk + embed, zero-key.

`embed_text` is a deterministic hash "embedder": stable, no key, no download —
and NOT semantically meaningful. The lab is about the pipeline around it
(stable chunks, cache key = hash(text) + model version, no re-embedding on
re-runs). Swap in a real model in the extension exercise; bump
`config.EMBEDDING_MODEL_VERSION` when you do.
"""
from __future__ import annotations

import hashlib
import re

EMBED_DIM = 16


def chunk_words(text: str, size: int, overlap: int) -> list[str]:
    """Fixed-size word chunks with overlap. Deterministic -> stable chunk hashes."""
    words = text.split()
    if not words:
        return []
    chunks, i = [], 0
    while True:
        chunks.append(" ".join(words[i:i + size]))
        if i + size >= len(words):
            break
        i += size - overlap
    return chunks


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def embed_text(text: str) -> list[float]:
    vec = [0.0] * EMBED_DIM
    for tok in re.findall(r"\w+", text.lower()):
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        vec[h % EMBED_DIM] += 1.0
    norm = sum(v * v for v in vec) ** 0.5 or 1.0
    return [round(v / norm, 6) for v in vec]
