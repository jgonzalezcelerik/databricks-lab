# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG learning_databricks;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC        current_date() AS first_touh_date,
# MAGIC        _metadata.file_modification_time as file_modification_time,
# MAGIC        _metadata.file_name as source_file,
# MAGIC        current_timestamp() AS ingestion_time
# MAGIC FROM read_files(
# MAGIC   '/Volumes/learning_databricks/default/csv_files_autoloader',
# MAGIC   format => 'CSV',
# MAGIC   sep => '|',
# MAGIC   header => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS historical_users_bronze;
# MAGIC
# MAGIC CREATE TABLE historical_users_bronze AS
# MAGIC SELECT *,
# MAGIC        current_date() AS first_touh_date,
# MAGIC        _metadata.file_modification_time as file_modification_time,
# MAGIC        _metadata.file_name as source_file,
# MAGIC        current_timestamp() AS ingestion_time
# MAGIC FROM read_files(
# MAGIC   '/Volumes/learning_databricks/default/csv_files_autoloader',
# MAGIC   format => 'CSV',
# MAGIC   sep => '|',
# MAGIC   header => 'true'
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM historical_users_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC   source_file,
# MAGIC   count(*) as total
# MAGIC FROM historical_users_bronze
# MAGIC GROUP BY source_file
# MAGIC ORDER BY source_file DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## PYTHON

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime, current_timestamp, current_date
from pyspark.sql.types import DateType

df = (
    spark
    .read
    .format('csv')
    .option("inferSchema", "true")
    .option("header", "true")
    .load('/Volumes/learning_databricks/default/csv_files_autoloader')
)

df_with_metadata = (
    df.withColumn("first_touch_date",current_date())
    .withColumn("file_modification_time", col("_metadata.file_modification_time"))
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("ingestion_time", current_timestamp())                  
)

(df_with_metadata
 .write
 .format("delta")
 .option("mergeSchema", "true")
 .mode("overwrite")
 .saveAsTable("learning_databricks.default.historical_users_bronze_python_metadata")
)

historical_users_bronze_python_metadata = spark.table("learning_databricks.default.historical_users_bronze_python_metadata")


display(historical_users_bronze_python_metadata)
