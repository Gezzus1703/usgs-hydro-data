# Databricks notebook source
display(spark.read.table("usgs_data.bronze.bronze_usgs_raw"))

# COMMAND ----------

display(spark.sql("SELECT * FROM usgs_data.bronze.bronze_usgs_raw LIMIT 10;"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA usgs_data.silver CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS usgs_data.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS usgs_data.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT * FROM usgs_data.bronze.bronze_usgs_raw LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM usgs_data.bronze.bronze_usgs_raw WHERE features IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM usgs_data.bronze.bronze_usgs_raw WHERE features IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT monitoring_location_id FROM usgs_data.silver.usgs_streamflow_silver LIMIT 10