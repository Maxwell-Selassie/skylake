from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = "mxs_skylake"
schema = "mxs_skylake_bronze"
landing_path = "/Volumes/mxs_skylake/mxs_skylake_raw/mxs_skylake_raw_vol"

# Business columns (DDL) for each source table
table_name_schema = {
    "airports": "airport_id STRING, airport_name STRING, city STRING, country STRING",
    "bookings": "booking_id STRING, passenger_id STRING, flight_id STRING, airport_id STRING, amount DOUBLE, booking_date DATE",
    "flights": "flight_id STRING, airline STRING, origin STRING, destination STRING, flight_date DATE",
    "passengers": "passenger_id STRING, name STRING, gender STRING, nationality STRING",
}


def make_bronze_pipeline_step(table_name, table_schema):
    target = f"{catalog}.{schema}.bronze_{table_name}"

    # @dp.table creates the streaming table and manages the ingestion flow in a single step
    @dp.table(
        name=target,
        comment=f"Bronze ingestion of {table_name} CSV files via Auto Loader",
    )
    def flow():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaHints", table_schema)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .load(f"{landing_path}/{table_name}_data/")
            .select(
                "*",
                F.col("_metadata.file_path").alias("_file_path"),
                F.col("_metadata.file_name").alias("_file_name"),
                F.col("_metadata.file_modification_time").alias("_file_modification_time"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )

    return flow


# Build and register the pipeline structure dynamically
for t_name, t_schema in table_name_schema.items():
    make_bronze_pipeline_step(t_name, t_schema)

