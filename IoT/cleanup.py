# Databricks notebook source
# MAGIC %md
# MAGIC Remove all storage directories

# COMMAND ----------

# remove directories
dbutils.fs.rm(workingDir,recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop database

# COMMAND ----------

spark.sql(f'DROP DATABASE IF EXISTS {db_name} CASCADE')
