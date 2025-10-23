# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG learning_academy;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   '/Volumes/learning_databricks/default/csv_files_autoloader',
# MAGIC   format => 'csv',
# MAGIC   sep => '|',
# MAGIC   header => true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_bronze;
# MAGIC
# MAGIC CREATE TABLE sales_bronze AS 
# MAGIC SELECT 
# MAGIC  *,
# MAGIC  _metadata.file_modification_time AS file_modification_time,
# MAGIC  _metadata.file_name AS source_file,
# MAGIC  current_timestamp() AS ingestion_time
# MAGIC FROM read_files(
# MAGIC   '/Volumes/learning_databricks/default/csv_files_autoloader',
# MAGIC   format => 'csv',
# MAGIC   sep => '|',
# MAGIC   header => true
# MAGIC );
# MAGIC     
# MAGIC SELECT * FROM sales_bronze;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## PYTHON

# COMMAND ----------

df = (spark
      .read
      .option("header", True)
      .option("sep", "|")
      .option("rescuedDataColumn", "_rescued_data")
      .csv('/Volumes/learning_databricks/default/csv_files_autoloader")
)

df.display()

# COMMAND ----------

spark.sql(
    f'''
    SELECT *
    FROM text.`/Volumes/learning_databricks/default/csv_files_autoloader`
    '''
).display()
