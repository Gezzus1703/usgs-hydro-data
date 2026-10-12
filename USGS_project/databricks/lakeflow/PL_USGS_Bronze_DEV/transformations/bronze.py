from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

RAW_PATH = "/Volumes/usgs_data/bronze/raw_usgs"

@dp.table(
    name="bronze_usgs_raw",
    comment="Raw USGS GeoJSON data ingested incrementally with Auto Loader"
)
def bronze_usgs_raw():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(RAW_PATH)
        .withColumn(
            "source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
    )