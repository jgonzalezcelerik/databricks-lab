# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG learning_databricks;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();
# MAGIC
# MAGIC --- CREATE TABLE AS
# MAGIC --- COPY INTO
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create stream Tables with SQL using AUTO LOADER

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   '/Volumes/learning_databricks/default/csv_files_autoloader',
# MAGIC   format => 'CSV',
# MAGIC   sep => '|',
# MAGIC   header => 'true'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a STREAMING TABLE using Databricks SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE sql_csv_autoloader
# MAGIC SCHEDULE EVERY 1 WEEK
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM STREAM read_files(
# MAGIC   '/Volumes/learning_databricks/default/csv_files_autoloader',
# MAGIC   format => 'CSV',
# MAGIC   sep => '|',
# MAGIC   header => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sql_csv_autoloader;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED sql_csv_autoloader;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sql_csv_autoloader;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH STREAMING TABLE sql_csv_autoloader;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sql_csv_autoloader;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sql_csv_autoloader;

# COMMAND ----------

# MAGIC %md
# MAGIC ## PYTHON AUTOLOADER

# COMMAND ----------

spark.sql(f'CREATE VOLUME IF NOT EXISTS learning_databricks.default.auto_loader_files')

checkpoint_file_location = f'/Volumes/learning_databricks/default/auto_loader_files'

(spark
    .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep","|")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_file_location}")
        .load(f"/Volumes/learning_databricks/default/csv_files_autoloader/")
    .writeStream  
        .option("checkpointLocation", f"{checkpoint_file_location}/checkpoint")
        .trigger(once=True)
        .toTable(f"learning_databricks.default.python_csv_autoloader")
)
