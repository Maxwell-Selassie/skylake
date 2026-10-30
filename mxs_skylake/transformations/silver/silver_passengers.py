from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ---------------------------------------------------------------------------
# Name parser: strip honorifics / suffixes, then split into first / last name
#
# Edge cases handled:
#   "Dr. Chris Dean"     -> first_name="Chris",    last_name="Dean"    (title)
#   "Eric Martin"        -> first_name="Eric",     last_name="Martin"   (simple two tokens)
#   "Spencer Grimes Jr." -> first_name="Spencer", last_name="Grimes"   (trailing suffix)
#   "Amy Kim MD"         -> first_name="Amy",      last_name="Kim"      (trailing suffix)
# ---------------------------------------------------------------------------
TITLES = {"dr", "mr", "mrs", "ms", "mx", "prof", "rev", "sir", "hon"}
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "md", "phd", "dds", "do", "esq", "rn", "dvm"}


def split_full_name(full_name):
    """Return (first_name, last_name) after removing titles and suffixes."""
    if full_name is None:
        return None, None
    # Strip periods so "Dr." -> "dr" and "Jr." -> "jr" match the token sets
    tokens = [t for t in full_name.strip().replace(".", "").split() if t]
    while tokens and tokens[0].lower() in TITLES:      # drop leading title(s)
        tokens.pop(0)
    while tokens and tokens[-1].lower() in SUFFIXES:   # drop trailing suffix(es)
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return tokens[0], None
    return tokens[0], " ".join(tokens[1:])


_name_struct = StructType([
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
])
split_names = F.udf(split_full_name, _name_struct)

# ---------------------------------------------------------------------------
# 1. Streaming table + 2. data-quality expectations
# ---------------------------------------------------------------------------
catalog = "mxs_skylake"
schema = "mxs_skylake_silver"
SOURCE_TABLE = "mxs_skylake.mxs_skylake_bronze.bronze_passengers"  

@dp.table(
    name=f"{catalog}.{schema}.passengers_silver",
    comment="Passenger data split into first/last name with quality expectations"
)
@dp.expect_all_or_drop(
    {
        "valid_passenger_id": "passenger_id IS NOT NULL",
        "valid_gender": "gender IN ('Male', 'Female', 'Others')",
        "valid_first_name": "first_name IS NOT NULL",
        "valid_last_name": "last_name IS NOT NULL",
    }
)
def passengers_silver():
    raw = spark.readStream.table(SOURCE_TABLE)
    return (
        raw.withColumns({
            "names": split_names("name"),
            "first_name": F.col("names.first_name"),
            "last_name": F.col("names.last_name")
        })
        .drop("names")
    )



