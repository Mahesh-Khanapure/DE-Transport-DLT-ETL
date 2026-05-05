from pyspark import pipelines as dp
import pyspark.sql.functions as F
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
@dp.view(
    name="trips_silver_staging_scd2"
)
def trips_silver():

    df_clean = spark.readStream.table("transportation.silver.trips_valid")

    df_silver = df_clean.select(
        F.col("trip_id").alias("id"),
        F.to_date(F.col("trip_date"), "yyyy-MM-dd").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled").alias("distance_kms"),
        F.col("fare_amount").alias("sale_amt"),
        F.col("passenger_rating").alias("passenger_rating"),
        F.col("driver_rating").alias("driver_rating"),
        F.col("ingest_timestamp").alias("bronze_ingest_timestamp")
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )

    return df_silver

dp.create_streaming_table(
    name="transportation.silver.trips_scd2",
    comment="Trips table with SCD Type 2 history",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

dp.create_auto_cdc_flow(
    target="transportation.silver.trips_scd2",
    source="trips_silver_staging_scd2",
    keys=["id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=2,
    except_column_list=["silver_processed_timestamp",
        "bronze_ingest_timestamp"]
)