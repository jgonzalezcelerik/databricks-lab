# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG learning_databricks;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED default; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

spark.sql(f"LIST '/Volumes/learning_databricks/default/myfiles'").display()

# COMMAND ----------

spark.sql(f'''
          SELECT * 
          FROM csv.`/Volumes/learning_databricks/default/myfiles/`
          '''
).display()

# COMMAND ----------

spark.sql(f'''
          SELECT * 
          FROM text.`/Volumes/learning_databricks/default/myfiles/`
          '''
).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT *
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/learning_databricks/default/myfiles/',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     inferSchema => true
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_employees;
# MAGIC
# MAGIC -- Create a Delta table using the CSV file
# MAGIC
# MAGIC CREATE TABLE current_employees AS
# MAGIC SELECT
# MAGIC   ID,
# MAGIC   FirstName,
# MAGIC   Country,
# MAGIC   Role
# MAGIC FROM read_files(
# MAGIC   '/Volumes/learning_databricks/default/myfiles/',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC );
# MAGIC
# MAGIC -- Disaplay table
# MAGIC SELECT * 
# MAGIC FROM current_employees;

# COMMAND ----------

#
# Read the CSV file and create a Spark DataFrame
#
sdf = (
    spark
    .read
    .format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .load('/Volumes/learning_databricks/default/myfiles/')
)

#
# Create a Delta table from the Spark DataFrame
# 

(
    sdf
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable('current_employees_py')
)

# COMMAND ----------

spark.read.table('current_employees_py').display()

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL current_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED current_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY current_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Insert two employess into the table
# MAGIC INSERT INTO current_employees
# MAGIC VALUES 
# MAGIC   (5555,'Alex', 'USA', 'Instructor'),
# MAGIC   (6666, 'Sanjay', 'India', 'Instructor');
# MAGIC
# MAGIC -- 2. Update a record in the table
# MAGIC UPDATE current_employees
# MAGIC SET Role = 'Senior Manager'
# MAGIC WHERE ID = 1111;
# MAGIC
# MAGIC -- 3. Delete a record in the table
# MAGIC DELETE FROM current_employees
# MAGIC WHERE ID = 3333;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees
# MAGIC ORDER BY ID;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY current_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees VERSION AS OF 2
# MAGIC ORDER BY ID;
# MAGIC
# MAGIC ---
# MAGIC --- SELECT *
# MAGIC --- FROM current_employees@v2
# MAGIC --- ORDER BY ID

# COMMAND ----------

# MAGIC %sql
# MAGIC --- CLEAN TABLES
# MAGIC DROP TABLE IF EXISTS current_employees;
# MAGIC DROP TABLE IF EXISTS current_employees_py;
# MAGIC
