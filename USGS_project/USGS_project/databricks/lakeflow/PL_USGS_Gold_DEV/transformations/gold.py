from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="usgs_daily_discharge_gold",
    comment="Dzienne agregacje aktualnego przepływu dla stacji pomiarowych (Materialized View)"
)
def usgs_daily_discharge_gold():
    df_silver = spark.read.table("usgs_data.silver.usgs_streamflow_silver")
    
    df_gold = (
        df_silver
        .filter(col("__END_AT").isNull())

        .withColumn("observation_date", to_date(col("observation_time")))

        .groupBy(
            "monitoring_location_id",
            "latitude",
            "longitude",
            "unit_of_measure",
            "observation_date"
        )

        .agg(
            round(avg("discharge_value"), 2).alias("avg_daily_discharge"),
            max("discharge_value").alias("max_daily_discharge"),
            min("discharge_value").alias("min_daily_discharge"),
            count("measurement_id").alias("total_readings_that_day")
        )
    )
    
    return df_gold