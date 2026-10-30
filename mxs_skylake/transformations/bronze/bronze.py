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

# from pyspark import pipelines as dp
# from pyspark.sql import functions as F

# # Configuration
# catalog = "mxs_skylake"
# schema = "mxs_skylake_bronze"
# landing_path = "/Volumes/mxs_skylake/mxs_skylake_raw/mxs_skylake_landing"

# # Define properties for a single table
# table_name = "bookings"
# table_schema = "booking_id STRING, passenger_id STRING, flight_id STRING, airport_id STRING, amount DOUBLE, booking_date DATE"
# target_table = f"{catalog}.{schema}.bronze_{table_name}"

# # Combined metadata, audit, and rescue columns for target definition
# metadata_columns = (
#     "_rescued_data STRING, "
#     "_file_path STRING, "
#     "_file_name STRING, "
#     "_file_modification_time TIMESTAMP, "
#     "_ingested_at TIMESTAMP"
# )

# # 1. Initialize the Target Streaming Table with both business and system columns
# dp.create_streaming_table(
#     name=target_table,
#     schema=f"{table_schema}, {metadata_columns}",
#     comment=f"Bronze ingestion of {table_name} CSV files via Auto Loader"
# )

# # 2. Define the Ingestion Flow using schemaHints to allow clean schema evolution
# @dp.append_flow(target=target_table, name=f"ingest_{table_name}")
# def ingest_bookings_flow():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .option("cloudFiles.schemaHints", table_schema)
#         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#         .option("cloudFiles.rescuedDataColumn", "_rescued_data")
#         .load(f"{landing_path}/{table_name}_data/")
#         .select(
#             "*",
#             F.col("_metadata.file_path").alias("_file_path"),
#             F.col("_metadata.file_name").alias("_file_name"),
#             F.col("_metadata.file_modification_time").alias("_file_modification_time"),
#             F.current_timestamp().alias("_ingested_at"),
#         )
#     )
