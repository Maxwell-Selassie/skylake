# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_silver.passengers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_silver.silver_airports

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table mxs_skylake.mxs_skylake_silver.silver_airports

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_silver.flights_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_silver.bookings_silver
# MAGIC