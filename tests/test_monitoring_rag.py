from datetime import datetime

import numpy as np

from smartmeter.monitoring import BREACH, NO_DATA, OK, FreshnessSLO, drift_status, evaluate_freshness, psi
from smartmeter.rag import build_messages, chunk_markdown, cites_source, keyword_coverage, leaks_pii


def test_freshness_statuses():
    slo = FreshnessSLO("t", "ts", max_lag_hours=24)
    now = datetime(2026, 6, 2, 12)
    assert evaluate_freshness(slo, datetime(2026, 6, 2, 0), now)["status"] == OK
    assert evaluate_freshness(slo, datetime(2026, 5, 31, 0), now)["status"] == BREACH
    assert evaluate_freshness(slo, None, now)["status"] == NO_DATA


def test_psi_detects_shift_not_noise():
    rng = np.random.default_rng(0)
    ref = rng.normal(10, 2, 5000)
    assert drift_status(psi(ref, rng.normal(10, 2, 5000))) == "STABLE"
    assert drift_status(psi(ref, rng.normal(13, 2, 5000))) == "DRIFT"


DOC = """# Tariffs

Intro line.

## Variable

Indexed monthly. Margin 12 cent.

## Fixed

Guaranteed 12 months.
"""


def test_chunking_keeps_heading_context():
    chunks = chunk_markdown(DOC, "tariffs.md")
    assert [c["section"] for c in chunks] == ["Tariffs", "Tariffs > Variable", "Tariffs > Fixed"]
    assert chunks[1]["content"].startswith("Tariffs > Variable") and "12 cent" in chunks[1]["content"]
    assert len({c["chunk_id"] for c in chunks}) == 3


def test_long_sections_are_split_on_paragraphs():
    text = "# T\n\n" + "\n\n".join("word " * 60 for _ in range(10))
    assert len(chunk_markdown(text, "x.md", max_chars=700)) > 1


def test_eval_metrics_and_guardrail():
    answer = "The margin is 12 cent per kWh [tariffs.md]."
    assert keyword_coverage(answer, ["12 cent", "monthly"]) == 0.5
    assert cites_source(answer, "tariffs.md") == 1.0
    assert leaks_pii("mail me at jan.peeters@example.com") and leaks_pii("call +32 471 12 34 56")
    assert not leaks_pii(answer)
    msgs = build_messages("q?", [{"source": "a.md", "content": "ctx"}])
    assert msgs[0]["role"] == "system" and "[a.md]" in msgs[1]["content"]
