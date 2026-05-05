import pyspark.pipelines as dp
import pyspark.sql.functions as F
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
@dp.materialized_view(
    name="transportation.silver.city",
    comment="Cleaned City Data",
      table_properties={
        "quality": "silver",
        "layer": "silver",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver():

    df_bronze = spark.read.table("transportation.bronze.city")

    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("state").alias("state"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )

    return df_silver