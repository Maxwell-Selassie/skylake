from pyspark import pipelines as dp
import pyspark.sql.functions as F

METADATA_COLUMNS = ["_ingested_at", "_file_name","_file_path", "_file_modification_time", "_rescued_data"]
SEQUENCE_BY = F.struct("_ingested_at")

SILVER_FLIGHTS = "mxs_skylake.mxs_skylake_silver.flights_silver"
SILVER_PASSENGERS = "mxs_skylake.mxs_skylake_silver.passengers_silver"
SILVER = "mxs_skylake.mxs_skylake_silver.silver_airports"

GOLD_FLIGHTS_SCD1 = "mxs_skylake.mxs_skylake_gold.flights_scd1"
GOLD_FLIGHTS_SCD2 = "mxs_skylake.mxs_skylake_gold.flights_scd2"

GOLD_PASSENGERS_SCD1 = "mxs_skylake.mxs_skylake_gold.passengers_scd1"
GOLD_PASSENGERS_SCD2 = "mxs_skylake.mxs_skylake_gold.passengers_scd2"

GOLD_SCD1 = "mxs_skylake.mxs_skylake_gold.airport_scd1"
GOLD_SCD2 = "mxs_skylake.mxs_skylake_gold.airport_scd2"

FLIGHTS_KEY = ["flight_id"]
PASSENGERS_KEY = ["passenger_id"]

# --- Flights Target 1: SCD Type 1 (current state only) ---
dp.create_streaming_table(
    name=GOLD_FLIGHTS_SCD1,
    comment="Flights current state (SCD Type 1)",
)

dp.create_auto_cdc_flow(
    target=GOLD_FLIGHTS_SCD1,
    source=SILVER_FLIGHTS,
    keys=FLIGHTS_KEY,
    sequence_by=SEQUENCE_BY,
    except_column_list=METADATA_COLUMNS,
    stored_as_scd_type=1,
)

# --- Flights Target 2: SCD Type 2 (full history) ---
dp.create_streaming_table(
    name=GOLD_FLIGHTS_SCD2,
    comment="Flights full history (SCD Type 2)",
)

dp.create_auto_cdc_flow(
    target=GOLD_FLIGHTS_SCD2,
    source=SILVER_FLIGHTS,
    keys=FLIGHTS_KEY,
    sequence_by=SEQUENCE_BY,
    except_column_list=METADATA_COLUMNS,
    stored_as_scd_type=2,
)

# --- Passengers Target 1: SCD Type 1 (current state only) ---
dp.create_streaming_table(
    name=GOLD_PASSENGERS_SCD1,
    comment="Passengers current state (SCD Type 1)",
)

dp.create_auto_cdc_flow(
    target=GOLD_PASSENGERS_SCD1,
    source=SILVER_PASSENGERS,
    keys=PASSENGERS_KEY,
    sequence_by=SEQUENCE_BY,
    except_column_list=METADATA_COLUMNS,
    stored_as_scd_type=1,
)

# --- Passengers Target 2: SCD Type 2 (full history) ---
dp.create_streaming_table(
    name=GOLD_PASSENGERS_SCD2,
    comment="Passengers full history (SCD Type 2)",
)

dp.create_auto_cdc_flow(
    target=GOLD_PASSENGERS_SCD2,
    source=SILVER_PASSENGERS,
    keys=PASSENGERS_KEY,
    sequence_by=SEQUENCE_BY,
    except_column_list=METADATA_COLUMNS,
    stored_as_scd_type=2,
)



# --- Airport Target 1: SCD Type 1 (current state only) ---
dp.create_streaming_table(
    name=GOLD_SCD1,
    comment="Airports current state (SCD Type 1)",
)

dp.create_auto_cdc_flow(
    target=GOLD_SCD1,
    source=SILVER,
    keys=["airport_id"],
    sequence_by=SEQUENCE_BY,
    except_column_list=METADATA_COLUMNS,
    stored_as_scd_type=1,
)


# --- Airport Target 2: SCD Type 2 (full history) ---
dp.create_streaming_table(
    name=GOLD_SCD2,
    comment="Airports full history (SCD Type 2)",
)

dp.create_auto_cdc_flow(
    target=GOLD_SCD2,
    source=SILVER,
    keys=["airport_id"],
    sequence_by=SEQUENCE_BY,
    except_column_list=METADATA_COLUMNS,
    stored_as_scd_type=2,
)