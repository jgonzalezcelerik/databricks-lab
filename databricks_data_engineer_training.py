# Databricks notebook source
# Title
# Databricks Data Engineer — Training Notebook
# Format: Python + SQL mixed# COMMAND ----------
# Workspace & Cluster Basics

# COMMAND ----------

spark.range(5).toDF("n").show()

# COMMAND ----------

dbutils.widgets.text("ingest_date", "2025-10-01", "Ingest Date")
print("ingest_date =", dbutils.widgets.get("ingest_date"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp() AS ts

# COMMAND ----------

# Delta Lake Fundamentals

# COMMAND ----------

from pyspark.sql import functions as F
spark.sql("CREATE DATABASE IF NOT EXISTS training_db")
spark.sql("USE training_db")
data = [(1, "NA", 100.0), (2, "EU", 120.5), (3, "NA", 90.25)]
df = spark.createDataFrame(data, "id INT, region STRING, amount DOUBLE")
df.write.format("delta").mode("overwrite").saveAsTable("sales_delta")
df2 = spark.createDataFrame([(4, "APAC", 77.7, "promo")], "id INT, region STRING, amount DOUBLE, channel STRING")
df2.write.option("mergeSchema","true").format("delta").mode("append").saveAsTable("sales_delta")
current_version = spark.sql("DESCRIBE HISTORY sales_delta").select("version").agg(F.max("version").alias("v")).first().v
print("Current Delta version:", current_version)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_delta;
# MAGIC SELECT * FROM sales_delta VERSION AS OF 0;
# MAGIC VACUUM sales_delta RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sales_delta ZORDER BY (region);

# COMMAND ----------

# Medallion Architecture

# COMMAND ----------

from pyspark.sql import Row, functions as F

base = "/tmp/training/lakehouse"
bronze = f"{base}/bronze"
silver = f"{base}/silver"
gold = f"{base}/gold"

events = [Row(id=1, user="u1", ts="2025-10-01T10:00:00Z", action="view"),
          Row(id=2, user="u2", ts="2025-10-01T11:05:00Z", action="click"),
          Row(id=3, user="u3", ts=None, action="view")]
spark.createDataFrame(events).write.format("delta").mode("overwrite").save(f"{bronze}/events")
spark.sql("DROP TABLE IF EXISTS bronze_events")
spark.sql(f"CREATE TABLE bronze_events USING DELTA LOCATION '{bronze}/events'")
silver_df = (spark.read.format("delta").load(f"{bronze}/events")
             .filter(F.col("ts").isNotNull())
             .withColumn("ts", F.to_timestamp("ts")))
silver_df.write.format("delta").mode("overwrite").save(f"{silver}/events")
spark.sql("DROP TABLE IF EXISTS silver_events")
spark.sql(f"CREATE TABLE silver_events USING DELTA LOCATION '{silver}/events'")
gold_df = (silver_df.groupBy(F.date_trunc("day", F.col("ts")).alias("event_day"))
           .agg(F.count("*").alias("total_events")))
gold_df.write.format("delta").mode("overwrite").save(f"{gold}/daily_metrics")
spark.sql("DROP TABLE IF EXISTS gold_daily_metrics")
spark.sql(f"CREATE TABLE gold_daily_metrics USING DELTA LOCATION '{gold}/daily_metrics'")
display(spark.table("gold_daily_metrics").orderBy("event_day"))

# COMMAND ----------

# ETL & Auto Loader

# COMMAND ----------

from pyspark.sql import Row
raw_path = f"{base}/raw_json"
spark.createDataFrame([Row(x=1), Row(x=2)]).write.mode("overwrite").json(raw_path)

# COMMAND ----------

stream_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(raw_path))
# query = (stream_df.writeStream
#          .format("delta")
#          .option("checkpointLocation", f"{base}/chk_raw_json")
#          .start(f"{silver}/json"))
# query.stop()

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS silver_customers (id INT, name STRING, country STRING) USING DELTA")
updates = spark.createDataFrame([(1, "Alice", "US"), (4, "Diego", "CO")], "id INT, name STRING, country STRING")
updates.createOrReplaceTempView("updates")
spark.sql("""
MERGE INTO silver_customers AS t
USING updates AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
spark.table("silver_customers").show()

# COMMAND ----------

# Governance (Unity Catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE CATALOG IF NOT EXISTS main;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS main.training;
# MAGIC -- CREATE TABLE IF NOT EXISTS main.training.secure_sales AS SELECT * FROM training_db.sales_delta;
# MAGIC -- CREATE OR REPLACE MASKING POLICY mask_amount AS (val DOUBLE) -> CASE WHEN is_member('finance_admins') THEN val ELSE NULL END;
# MAGIC -- ALTER TABLE main.training.secure_sales ALTER COLUMN amount SET MASKING POLICY mask_amount;
# MAGIC -- CREATE OR REPLACE VIEW main.training.secure_sales_view AS SELECT * FROM main.training.secure_sales WHERE region = 'NA';

# COMMAND ----------

# Jobs & DLT

# COMMAND ----------

dlt_sql = """
CREATE OR REFRESH STREAMING LIVE TABLE bronze_customers
AS SELECT * FROM cloud_files('/mnt/raw/customers', 'json');

CREATE OR REFRESH LIVE TABLE silver_customers
(TBLPROPERTIES ("quality" = "silver"))
AS SELECT CAST(id AS INT) AS id, name, country FROM LIVE.bronze_customers
EXPECT id_not_null EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW;
"""
print(dlt_sql)

# COMMAND ----------

job_json = {
  "name": "Training ETL Pipeline",
  "tasks": [
    {
      "task_key": "silver_task",
      "notebook_task": {"notebook_path": "/Repos/training/03_silver_transforms"},
      "job_cluster_key": "etl_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_D3_v2",
        "autoscale": {"min_workers": 2, "max_workers": 8}
      }
    }
  ]
}
print(job_json)

# COMMAND ----------

# SQL / BI

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE VIEW gold.top_regions AS
# MAGIC -- SELECT region, SUM(amount) AS total_sales FROM training_db.sales_delta GROUP BY region ORDER BY total_sales DESC;

# COMMAND ----------

# DevOps & Terraform

# COMMAND ----------

terraform_hcl = """
resource "databricks_cluster" "etl" {
  cluster_name  = "etl-cluster"
  spark_version = "14.3.x-scala2.12"
  node_type_id  = "Standard_DS3_v2"
  autoscale { min_workers = 2, max_workers = 8 }
}
"""
print(terraform_hcl)

# COMMAND ----------

# Advanced & E2E

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALTER TABLE training_db.sales_delta SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC -- SELECT * FROM table_changes('training_db.sales_delta', 0, 10);

# COMMAND ----------

from pyspark.sql import functions as F

def etl_events(src_path, base):
    bronze_path = f"{base}/bronze_events2"
    silver_path = f"{base}/silver_events2"
    gold_path   = f"{base}/gold_metrics2"
    spark.read.json(src_path).write.mode("overwrite").format("delta").save(bronze_path)
    spark.sql("DROP TABLE IF EXISTS bronze_events2")
    spark.sql(f"CREATE TABLE bronze_events2 USING DELTA LOCATION '{bronze_path}'")
    df = spark.read.format("delta").load(bronze_path).filter("ts IS NOT NULL")
    (df.withColumn("ts", F.to_timestamp("ts"))
       .write.mode("overwrite").format("delta").save(silver_path))
    spark.sql("DROP TABLE IF EXISTS silver_events2")
    spark.sql(f"CREATE TABLE silver_events2 USING DELTA LOCATION '{silver_path}'")
    gdf = (spark.read.format("delta").load(silver_path)
           .groupBy(F.date_trunc("day", F.col("ts")).alias("event_day"))
           .agg(F.count("*").alias("total_events")))
    gdf.write.mode("overwrite").format("delta").save(gold_path)
    spark.sql("DROP TABLE IF EXISTS gold_daily_metrics2")
    spark.sql(f"CREATE TABLE gold_daily_metrics2 USING DELTA LOCATION '{gold_path}'")
    return spark.table("gold_daily_metrics2")

demo_src = "/tmp/training/lakehouse/events_src_json"
spark.createDataFrame([("2025-10-02T10:00:00Z", "view"), ("2025-10-02T11:10:00Z", "click")], "ts STRING, action STRING").write.mode("overwrite").json(demo_src)
display(etl_events(demo_src, "/tmp/training/lakehouse"))
