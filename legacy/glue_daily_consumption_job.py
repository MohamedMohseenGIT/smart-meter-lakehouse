"""LEGACY - AWS Glue job replaced by this project (kept for the migration parallel run).

Reads the raw meter files from S3, aggregates per meter and day, writes Parquet for Athena.
This file is reference material: it is not deployed and not linted.

Known issues found during the migration (see docs/migration.md):
  * aggregates by UTC date: Belgian days are shifted by 1-2 hours, DST days are wrong
  * no de-duplication: re-delivered rows are counted twice
  * only negative values are filtered; NULLs and unknown meters pass silently
  * full reload every night (no incremental processing), Glue Data Catalog for metadata, no tests
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "target_path"])
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

raw = glue.create_dynamic_frame.from_options(
    connection_type="s3", connection_options={"paths": [args["source_path"]], "recurse": True},
    format="json").toDF()

daily = (raw.withColumn("reading_ts", F.to_timestamp("reading_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
         .where(F.col("kwh_import") >= 0)
         .groupBy("meter_id", F.to_date("reading_ts").alias("reading_date"))
         .agg(F.sum("kwh_import").alias("kwh_import"), F.sum("kwh_export").alias("kwh_export")))

daily.write.mode("overwrite").partitionBy("reading_date").parquet(args["target_path"])
job.commit()
