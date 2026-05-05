from pyspark import pipelines as dp
import sys
sys.path.append("/Workspace/Project_Transportation/Pipeline_Transportation/transformations/silver/configration_file.py")
from  configration_file import *
schema_config, rules, table_name = load_config()

@dp.view(name="silver_stg_view")
@dp.expect_all(rules)
def staging_view():

    df = spark.readStream.table("transportation.bronze.trips")

    df = apply_schema(df, schema_config)
    df = apply_dq(df, rules)

    return df
@dp.table(
    name="transportation.silver.trips_valid",
    comment="Only valid records"
)
def trips_valid():
    return spark.readStream.table("silver_stg_view") \
        .filter("is_valid = true")


@dp.table(
    name="transportation.silver.trips_invalid",
    comment="Invalid records (quarantine)"
)
def trips_invalid():
    return spark.readStream.table("silver_stg_view") \
        .filter("is_valid = false")

	 