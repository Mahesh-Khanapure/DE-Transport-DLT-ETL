from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark.conf.set("spark.sql.adaptive.enabled", "true")
SOURCE_PATH = "/Volumes/transportation/source_data/trips_data"

@dp.table(
    name="transportation.bronze.trips",
    comment="Streaming ingestion of raw trips data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "checkpointLocation": "/Volumes/transportation/bronze/checkpoints/trips_bronze"
    }
)
def trips_bronze():

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .option("header", "true")
        .load(SOURCE_PATH)
    )

    df = (
        df.withColumn("file_path", col("_metadata.file_path"))
          .withColumn("ingest_timestamp", current_timestamp())
    )

    return df