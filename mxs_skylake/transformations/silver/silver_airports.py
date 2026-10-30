from pyspark import pipelines as dp
from pyspark.sql.functions import *

catalog = "mxs_skylake"
schema = "mxs_skylake_silver"

@dp.table(
    name=f"{catalog}.{schema}.silver_airports",
    comment="Silver table with trips and airport data, expectations applied, duplicates dropped"
)
@dp.expect_or_drop("airport_id_not_null", "airport_id IS NOT NULL")
@dp.expect_or_drop("airport_name_not_null", "airport_name IS NOT NULL")
def silver_airports():
    df = spark.readStream.table(f"{catalog}.mxs_skylake_bronze.bronze_airports")
    df = df.toDF(*[c.strip() for c in df.columns])
    return (
        df.dropDuplicates(["airport_id"])
    )