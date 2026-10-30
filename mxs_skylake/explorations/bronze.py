# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC SELECT * FROM mxs_skylake.mxs_skylake_bronze.bronze_airports

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_bronze.bronze_passengers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_bronze.bronze_flights

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_bronze.bronze_bookings

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_silver.bookings_silver