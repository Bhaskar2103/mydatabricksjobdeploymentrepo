# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Topics Job — Enterprise-style ETL Pattern
# MAGIC Demonstrates parameterized execution, logging, and a Bronze -> Silver write pattern.

# COMMAND ----------
import logging
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("run_date", datetime.now().strftime("%Y-%m-%d"), "Run Date")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("topics_job")

logger.info(f"Starting topics_job | catalog={catalog} schema={schema} run_date={run_date}")

# COMMAND ----------
try:
    data = [
        (1, "billing", "active", run_date),
        (2, "payments", "active", run_date),
        (3, "support", "inactive", run_date),
    ]
    columns = ["topic_id", "topic_name", "status", "load_date"]

    df = spark.createDataFrame(data, columns)
    df = df.withColumn("ingested_at", F.current_timestamp())

    logger.info(f"Row count ingested: {df.count()}")
    display(df)

    df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.topics_bronze")

    logger.info("Job completed successfully")

except Exception as e:
    logger.error(f"Job failed: {str(e)}")
    raise
