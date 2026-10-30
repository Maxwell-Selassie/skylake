from pyspark import pipelines as dp

catalog = "mxs_skylake"
schema = "mxs_skylake_silver"

@dp.table(
    name=f"{catalog}.{schema}.flights_silver",
    comment="Silver layer flights table with data quality expectations"
)
@dp.expect_or_drop("valid_flight_id", "flight_id IS NOT NULL")
@dp.expect_or_drop("valid_airline", "airline IS NOT NULL")
def silver_flights():
    return spark.readStream.table("mxs_skylake.mxs_skylake_bronze.bronze_flights")