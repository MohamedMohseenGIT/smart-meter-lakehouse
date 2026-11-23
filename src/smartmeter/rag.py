"""Retrieval-augmented generation helpers: chunking, prompting and evaluation metrics.

Kept framework-free so the logic is unit-tested locally; notebooks plug in Databricks Vector Search
(retrieval) and a Foundation Model endpoint (generation).
"""

from __future__ import annotations

import hashlib
import re

SYSTEM_PROMPT = (
    "You are a customer-service assistant for NorthSea Energy, a fictional Belgian energy supplier. "
    "Answer ONLY from the provided context. If the answer is not in the context, say you do not know. "
    "Cite the source file names in square brackets, e.g. [tariffs.md]. Never reveal personal data."
)


def chunk_markdown(text: str, source: str, max_chars: int = 1200) -> list[dict]:
    """Split a markdown document by headings, then by paragraphs when a section is too long.

    The heading path is prepended to each chunk so retrieval keeps the section context.
    """
    chunks: list[dict] = []
    title, section, buf = "", "", []

    def flush():
        body = "\n".join(buf).strip()
        if not body:
            return
        header = " > ".join(x for x in (title, section) if x)
        parts, current = [], ""
        for para in re.split(r"\n\s*\n", body):
            if current and len(current) + len(para) > max_chars:
                parts.append(current)
                current = para
            else:
                current = f"{current}\n\n{para}" if current else para
        parts.append(current)
        for p in parts:
            content = f"{header}\n\n{p.strip()}" if header else p.strip()
            cid = hashlib.sha1(f"{source}|{header}|{len(chunks)}".encode()).hexdigest()[:16]
            chunks.append({"chunk_id": cid, "source": source, "section": header, "content": content})

    for line in text.splitlines():
        m = re.match(r"^(#{1,3})\s+(.*)", line)
        if m:
            flush()
            buf = []
            if len(m.group(1)) == 1:
                title, section = m.group(2).strip(), ""
            else:
                section = m.group(2).strip()
        else:
            buf.append(line)
    flush()
    return chunks


def build_messages(question: str, contexts: list[dict]) -> list[dict]:
    context = "\n\n---\n\n".join(f"[{c['source']}]\n{c['content']}" for c in contexts)
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"},
    ]


def hit_at_k(retrieved_sources: list[str], expected_source: str) -> float:
    return 1.0 if expected_source in retrieved_sources else 0.0


def keyword_coverage(answer: str, must_include: list[str]) -> float:
    """Share of required facts present in the answer (cheap, deterministic correctness proxy)."""
    if not must_include:
        return 1.0
    a = answer.lower()
    return sum(1 for k in must_include if k.lower() in a) / len(must_include)


def cites_source(answer: str, expected_source: str) -> float:
    return 1.0 if f"[{expected_source}]" in answer else 0.0


def leaks_pii(answer: str) -> bool:
    """Guardrail: detect e-mail addresses or Belgian mobile numbers in model output."""
    return bool(re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", answer) or re.search(r"\+32\s?4\d{2}", answer))
