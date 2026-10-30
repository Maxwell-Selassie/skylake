from pyspark import pipelines as dp
from pyspark.sql.functions import col


catalog = "mxs_skylake"
schema = "mxs_skylake_silver"
SOURCE_PATH = "mxs_skylake.mxs_skylake_bronze.bronze_bookings"


@dp.table(
    name=f"{catalog}.{schema}.bookings_silver",
    comment="Streaming table of bookings with data quality expectations",
)
@dp.expect_or_drop("non_null_flight_id", "flight_id IS NOT NULL")
@dp.expect_or_drop("non_null_airport_id", "airport_id IS NOT NULL")
@dp.expect_or_drop("non_null_booking_id", "booking_id IS NOT NULL")
@dp.expect_or_drop("non_null_passenger_id", "passenger_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_booking_date",
    "booking_date IS NOT NULL AND TRY_CAST(booking_date AS DATE) IS NOT NULL",
)

@dp.expect_or_drop(
    "valid_amount",
    "amount IS NOT NULL AND amount >= 0",
)
def bookings_silver():
    return (
        spark.readStream.table(SOURCE_PATH).withColumn(
            "amount", col("amount").cast("decimal(10,2)")
        )
    )
