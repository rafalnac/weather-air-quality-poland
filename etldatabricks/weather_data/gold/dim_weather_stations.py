# Databricks notebook source
import os

from delta.tables import DeltaTable
from pyspark.sql.functions import col

from etldatabricks.utils.constants import GOLD_STAGE_DIR_PATH
from etldatabricks.utils.helpers import deduplicate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create dimension table from weather_data_silver table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load weather_data_silver table to data frame

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
# MAGIC ##### Select requred columns for further transformations

# COMMAND ----------

dim_weather_station_selected_cols = (
    weater_data_silver_new
        .select("station_id", "station_name", "_commit_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define storage location for dimesion

# COMMAND ----------

dim_weather_station_storage_loc = os.path.join(GOLD_STAGE_DIR_PATH, "dim_weather_station")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create dimension table
# MAGIC - SCD Type 1

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.weather_data.dim_weather_station
(
    station_sk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) CONSTRAINT station_pk PRIMARY KEY,
    station_id BIGINT NOT NULL,
    station_name STRING
)
USING DELTA
LOCATION "{dim_weather_station_storage_loc}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write dimesion to delta table
# MAGIC - Define fuction to perform upsert of each microbatch
# MAGIC - Write to delta table

# COMMAND ----------

def insert_into_dim(microbatch, batch_id):

    # Initiate Delta Table object
    target = DeltaTable.forName(spark, "project_weather_air.weather_data.dim_weather_station")

    # Microbatch: Drop duplicates based on the latest '_commit_timestamp'
    dim_weather_station_dedup = (
        deduplicate_df(microbatch, "_commit_timestamp", "station_id")
            .dropDuplicates(["station_id"]) # additional drop in case of many updates or inserts with the same timestamp
    )
    
    upsert_query = (
        target.alias("t")
            .merge(dim_weather_station_dedup.alias("s"),
                  "t.station_id = s.station_id")
            .whenNotMatchedInsert(
                values={
                    "t.station_id": "s.station_id",
                    "t.station_name": "s.station_name"
                })
            .whenMatchedUpdate(
                condition="t.station_name <> s.station_name",
                set={"t.station_name": "s.station_name"}
            )
            .execute()
    )

# COMMAND ----------

write_query = (
    dim_weather_station_selected_cols
        .writeStream             
        .format("delta")
        .outputMode("update")
        .foreachBatch(insert_into_dim)
        .option("checkpointLocation", os.path.join(dim_weather_station_storage_loc, "_checkpoint"))
        .trigger(availableNow=True)
        .start()
)

# COMMAND ----------

# %sql
# SELECT * FROM project_weather_air.weather_data.dim_weather_station;
