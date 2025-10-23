# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingesting Data into Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG learning_databricks;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC **18. Using PySpark**

# COMMAND ----------

spark.catalog.setCurrentCatalog('learning_databricks')
spark.catalog.setCurrentDatabase('default')

spark.catalog.listTables('default')

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Viewing the available files**

# COMMAND ----------

spark.sql(f"LIST '/Volumes/learning_databricks/default/myfiles'").display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_employees_ctas;
# MAGIC
# MAGIC CREATE TABLE current_employees_ctas 
# MAGIC AS
# MAGIC SELECT ID, FirstName, Country, Role
# MAGIC FROM read_files(
# MAGIC   '/Volumes/learning_databricks/default/myfiles/',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC );
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees_ctas;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_employees_copyinto;
# MAGIC
# MAGIC CREATE TABLE current_employees_copyinto (
# MAGIC   ID INT,
# MAGIC   FirstName STRING,
# MAGIC   Country STRING,
# MAGIC   Role STRING
# MAGIC );

# COMMAND ----------

spark.sql(
    f'''
    COPY INTO current_employees_copyinto
    FROM '/Volumes/learning_databricks/default/myfiles/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS (
        'header' = 'true',
        'inferSchema' = 'true'
    )
    '''
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees_copyinto;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE current_employees_copyinto;

# COMMAND ----------


