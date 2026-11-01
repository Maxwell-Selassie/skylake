# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC drop table mxs_skylake.mxs_skylake_gold.airport_scd1;
# MAGIC drop table mxs_skylake.mxs_skylake_gold.airport_scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_gold.airport_scd1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mxs_skylake.mxs_skylake_gold.airport_scd2

# COMMAND ----------


tables = [
    "mxs_skylake.mxs_skylake_gold.airport_scd1",
    "mxs_skylake.mxs_skylake_gold.airport_scd2",
    "mxs_skylake.mxs_skylake_gold.flights_scd1",
    "mxs_skylake.mxs_skylake_gold.flights_scd2",
    "mxs_skylake.mxs_skylake_gold.passengers_scd1",
    "mxs_skylake.mxs_skylake_gold.passengers_scd2",
    "mxs_skylake.mxs_skylake_silver.bookings_silver"
]

for t in tables:
    print(f"===== {t} =====")
    spark.table(t).printSchema()
    print()