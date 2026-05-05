from pyspark import pipelines as dp 
import pyspark.sql.functions as F

@dp.materialized_view(
    name="transportation.silver.trips_scd2_with_current"
)
def trips_scd2_current():

    df = spark.read.table("transportation.silver.trips_scd2")

    df = df.withColumn(
        "__IS_CURRENT",
        F.when(F.col("__END_AT").isNull(), True).otherwise(False)
    )

    return df