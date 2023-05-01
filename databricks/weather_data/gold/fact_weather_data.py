# Databricks notebook source
import os

from delta.tables import DeltaTable
from pyspark.sql.functions import col, broadcast

from databricks.utils.constants import GOLD_STAGE_DIR_PATH
from databricks.utils.helpers import deduplicate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create fact table from weather_data_silver table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define storage location for fact table

# COMMAND ----------

fact_weather_storage_loc = os.path.join(GOLD_STAGE_DIR_PATH, "fact_weather_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create fact table 'fact_weather_data'

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE project_weather_air.weather_data.fact_weather_data
(
  weather_id BIGINT CONSTRAINT weather_data_pk PRIMARY KEY,
  weather_nk STRING,
  pressure FLOAT,
  measurement_date DATE,
  measurment_hour INT,
  station_sk BIGINT CONSTRAINT weather_data_station_fk FOREIGN KEY
    REFERENCES project_weather_air.weather_data.dim_weather_station,
  wind_direction INT,
  wind_speed INT,
  precipitation DOUBLE,
  temperature FLOAT,
  humidity DOUBLE
)
USING DELTA
LOCATION "{fact_weather_storage_loc}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load weather_data_silver table to data frame
# MAGIC - Stream read

# COMMAND ----------

weather_data_silver = (
    spark.readStream
        .option("readChangeFeed", True)
        .option("startingVersion", 1)
        .table("project_weather_air.weather_data.weather_data_silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get new and updated rows from weather_data_silver table

# COMMAND ----------

weater_data_silver_new = (
    weather_data_silver
        .filter(col("_change_type").isin(["insert", "update_postimage"]))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load dimension table dim_weather_stations
# MAGIC - Static read

# COMMAND ----------

dim_weather_station = (
    spark.read.
        table("project_weather_air.weather_data.dim_weather_station")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns from dimension to avoid ambiguity during join

# COMMAND ----------

dim_weather_station_renamed = (
    dim_weather_station
        .withColumnRenamed("station_id", "station_id_dim")
        .withColumnRenamed("station_name", "station_name_dim")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Prepare batch for insert to fact table
# MAGIC - Join with dimension table

# COMMAND ----------

condition = (
    weater_data_silver_new.station_id == dim_weather_station_renamed.station_id_dim
)

weater_data_silver_new_joined = (
    weater_data_silver_new
        .join(
            broadcast(dim_weather_station_renamed),
            condition, 
            "left"
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write dataframe to fact table
# MAGIC - Define fuction to perform upsert of each microbatch
# MAGIC - Write to delta table

# COMMAND ----------

def upsert_to_delta(microbatch, batch_id):

    # Initiate delta table
    target = DeltaTable.forName(spark, "project_weather_air.weather_data.fact_weather_data")

    # Deduplication based on the latest cdf timestamp
    dedup_microbatch = deduplicate_df(microbatch, "_commit_timestamp", "weather_nk")
    
    upsert_query = (
        target.alias("t")
            .merge(dedup_microbatch.alias("s"),
                  "t.weather_nk = s.weather_nk")
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute()
    )

# COMMAND ----------

query = (
    weater_data_silver_new_joined
        .writeStream
        .outputMode("update")
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", os.path.join(fact_weather_storage_loc, "_checkpoint"))
        .trigger(availableNow=True)
        .start()
)
