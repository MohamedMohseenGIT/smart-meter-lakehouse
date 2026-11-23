# Databricks notebook source
# MAGIC %md
# MAGIC # 21 · GenAI: RAG assistant + evaluation
# MAGIC
# MAGIC Retrieval from Vector Search, generation with a Foundation Model endpoint, and an **evaluation
# MAGIC set** (`docs/knowledge_base/eval_set.jsonl`) scored on every change:
# MAGIC
# MAGIC | Metric | Meaning |
# MAGIC |---|---|
# MAGIC | `hit_at_k` | the expected document was retrieved (retrieval quality) |
# MAGIC | `keyword_coverage` | required facts appear in the answer (correctness proxy) |
# MAGIC | `cites_source` | the answer cites the right document (groundedness) |
# MAGIC | `pii_leak` | guardrail: no e-mail / phone number in the output |
# MAGIC | `latency_s` | end-to-end response time |
# MAGIC
# MAGIC Results are logged to MLflow, so prompt, chunking or model changes are compared on evidence.

# COMMAND ----------

# MAGIC %pip install -q "databricks-vectorsearch>=0.40" "databricks-sdk>=0.30" "openai>=1.40" "mlflow>=2.16"
# MAGIC %restart_python

# COMMAND ----------

import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import mlflow
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

from smartmeter.config import LakehouseNames
from smartmeter.rag import build_messages, cites_source, hit_at_k, keyword_coverage, leaks_pii

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.text("vs_endpoint", "smart-meter-vs")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text("top_k", "4")
LLM = dbutils.widgets.get("llm_endpoint")
TOP_K = int(dbutils.widgets.get("top_k"))
index = VectorSearchClient(disable_notice=True).get_index(dbutils.widgets.get("vs_endpoint"), n.ml("kb_chunks_index"))
llm = WorkspaceClient().serving_endpoints.get_open_ai_client()
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/smart-meter-rag")

# COMMAND ----------

def retrieve(question: str, k: int = TOP_K) -> list[dict]:
    res = index.similarity_search(query_text=question, columns=["chunk_id", "source", "content"], num_results=k)
    cols = [c["name"] for c in res["manifest"]["columns"]]
    return [dict(zip(cols, row)) for row in res["result"].get("data_array", [])]


def answer(question: str) -> tuple[str, list[dict]]:
    contexts = retrieve(question)
    resp = llm.chat.completions.create(model=LLM, messages=build_messages(question, contexts),
                                       temperature=0.0, max_tokens=400)
    return resp.choices[0].message.content, contexts


print(answer("How much do I get paid for solar energy I inject into the grid?")[0])

# COMMAND ----------

kb_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "docs", "knowledge_base"))
with open(f"{kb_dir}/eval_set.jsonl") as fh:
    eval_set = [json.loads(line) for line in fh if line.strip()]

rows = []
for case in eval_set:
    start = time.time()
    text, ctx = answer(case["question"])
    rows.append({
        "question": case["question"], "answer": text,
        "retrieved": ", ".join(c["source"] for c in ctx),
        "hit_at_k": hit_at_k([c["source"] for c in ctx], case["expected_source"]),
        "keyword_coverage": keyword_coverage(text, case["must_include"]),
        "cites_source": cites_source(text, case["expected_source"]),
        "pii_leak": float(leaks_pii(text)),
        "latency_s": round(time.time() - start, 2),
    })
results = pd.DataFrame(rows)

with mlflow.start_run(run_name=f"rag-{LLM}-k{TOP_K}"):
    mlflow.log_params({"llm": LLM, "top_k": TOP_K, "embedding": "databricks-gte-large-en", "cases": len(results)})
    mlflow.log_metrics({m: float(results[m].mean()) for m in
                        ["hit_at_k", "keyword_coverage", "cites_source", "pii_leak", "latency_s"]})
    mlflow.log_table(results, "eval_results.json")

spark.createDataFrame(results).write.mode("overwrite").saveAsTable(n.ml("rag_eval_results"))
display(results)
assert results["pii_leak"].sum() == 0, "Guardrail violated: personal data in model output"
