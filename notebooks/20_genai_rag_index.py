# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · GenAI: build the RAG knowledge index
# MAGIC
# MAGIC Customer-service documents (`docs/knowledge_base/*.md`) are chunked by heading, stored in a Delta
# MAGIC table with Change Data Feed, and indexed by **Databricks Vector Search** with managed embeddings.
# MAGIC The index is a *Delta Sync* index: when documents change, re-running this notebook re-chunks them
# MAGIC and `sync()` embeds only the changed rows.
# MAGIC
# MAGIC Free Edition allows one Vector Search endpoint with one search unit, which is plenty here.

# COMMAND ----------

# MAGIC %pip install -q "databricks-vectorsearch>=0.40"
# MAGIC %restart_python

# COMMAND ----------

import glob
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from databricks.vector_search.client import VectorSearchClient

from smartmeter.config import LakehouseNames
from smartmeter.rag import chunk_markdown

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.text("vs_endpoint", "smart-meter-vs")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint")
EMBEDDING = dbutils.widgets.get("embedding_endpoint")
CHUNKS_TABLE = n.ml("kb_chunks")
INDEX_NAME = n.ml("kb_chunks_index")

# COMMAND ----------

kb_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "docs", "knowledge_base"))
chunks = []
for path in sorted(glob.glob(f"{kb_dir}/*.md")):
    with open(path) as fh:
        chunks += chunk_markdown(fh.read(), source=os.path.basename(path))
print(f"{len(chunks)} chunks from {kb_dir}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CHUNKS_TABLE}
              (chunk_id STRING NOT NULL, source STRING, section STRING, content STRING)
              TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
spark.createDataFrame(chunks, "chunk_id string, source string, section string, content string") \
    .createOrReplaceTempView("_chunks")
spark.sql(f"""MERGE INTO {CHUNKS_TABLE} t USING _chunks s ON t.chunk_id = s.chunk_id
              WHEN MATCHED AND t.content <> s.content THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *
              WHEN NOT MATCHED BY SOURCE THEN DELETE""")

# COMMAND ----------

vsc = VectorSearchClient(disable_notice=True)
if VS_ENDPOINT not in [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]:
    vsc.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type="STANDARD")

existing = [i["name"] for i in vsc.list_indexes(VS_ENDPOINT).get("vector_indexes", [])]
if INDEX_NAME in existing:
    index = vsc.get_index(VS_ENDPOINT, INDEX_NAME)
    index.sync()
else:
    index = vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT, index_name=INDEX_NAME, source_table_name=CHUNKS_TABLE,
        pipeline_type="TRIGGERED", primary_key="chunk_id", embedding_source_column="content",
        embedding_model_endpoint_name=EMBEDDING)

for _ in range(60):
    status = index.describe().get("status", {})
    if status.get("ready"):
        break
    time.sleep(20)
print(status.get("detailed_state"), status.get("indexed_row_count"))
