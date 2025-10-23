# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG learning_databricks;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

spark.sql(f"LIST '/Volumes/learning_databricks/default/myfiles/' ").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MEDALLION ARCHITECTURE

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_employees_bronze;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS current_employees_bronze (
# MAGIC   ID INT,
# MAGIC   Firstname STRING,
# MAGIC   Country STRING,
# MAGIC   Role STRING
# MAGIC );

# COMMAND ----------

spark.sql(
    f'''
    COPY INTO current_employees_bronze
    FROM '/Volumes/learning_databricks/default/myfiles/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS (
        'header' = 'true',
        'inferSchema' = 'true'
    )
    '''
).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_employees_silver AS
# MAGIC SELECT
# MAGIC   ID,
# MAGIC   FirstName,
# MAGIC   Country,
# MAGIC   upper(Role) as Role,
# MAGIC   current_timestamp() as CurrentTimeStamp,
# MAGIC   date(currenttimestamp) as CurrentDate
# MAGIC FROM
# MAGIC   current_employees_bronze;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view_total_roles AS
# MAGIC SELECT
# MAGIC   Role,
# MAGIC   count(*) as TotalEmployees
# MAGIC FROM
# MAGIC   current_employees_silver
# MAGIC GROUP BY Role;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM temp_view_total_roles;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS total_roles_gold (
# MAGIC   Role STRING,
# MAGIC   TotalEmployees INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE total_roles_gold
# MAGIC SELECT *
# MAGIC FROM temp_view_total_roles;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM total_roles_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY total_roles_gold;
