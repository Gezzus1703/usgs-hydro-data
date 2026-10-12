from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.create_view(
    name="usgs_streamflow_silver_parsed"
)
def usgs_silver():
    df_bronze_stream = spark.readStream.table("usgs_data.bronze.bronze_usgs_raw")
    return(
        df_bronze_stream
        .select(
            explode(col("features")).alias("feature"),
            col("source_file"),
            col("ingestion_timestamp"),
        )
        .select(
            col("feature.id").alias("measurement_id"),
            col("feature.properties.monitoring_location_id").alias("monitoring_location_id"),
            col("feature.properties.parameter_code").alias("parameter_code"),
            col("feature.properties.time").cast("timestamp").alias("observation_time"),
            col("feature.properties.value").cast("double").alias("discharge_value"),
            col("feature.properties.unit_of_measure").alias("unit_of_measure"),
            col("feature.properties.approval_status").alias("approval_status"),
            col("feature.geometry.coordinates").alias("coordinates"),
            col("feature.geometry.coordinates")[0].cast("double").alias("longitude"),
            col("feature.geometry.coordinates")[1].cast("double").alias("latitude"),
            col("source_file"),
            col("ingestion_timestamp")
        )
    )

dp.create_streaming_table(
    name="usgs_streamflow_silver"
)

dp.apply_changes(
    target="usgs_streamflow_silver",
    source="usgs_streamflow_silver_parsed",
    keys=["measurement_id"],
    sequence_by=col("ingestion_timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=[
        "approval_status", 
        "discharge_value"
    ]
)