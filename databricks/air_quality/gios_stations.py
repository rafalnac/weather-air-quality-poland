# Databricks notebook source
import os

from pyspark.sql.functions import current_timestamp, input_file_name, schema_of_json, col, count
from pyspark.sql.types import *
from delta.tables import DeltaTable

from databricks.utils.constants import WEATHER_AIR_QUALITY_CONTAINER_PATH, BRONZE_STAGE_DIR_PATH, SILVER_STAGE_DIR_PATH
from databricks.utils.helpers import extract_timestamp_string, parse_file_timestamp, deduplicate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest monitoring stations data (BRONZE)
# MAGIC - Expect that each name of arriving station files contain timestamp in format 'yyyy-MM-dd'T'HH_mm_ss_SSSSSSSX'.</br> Timestamp pattern format according to: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html.</br>File name example 'stations_2023-03-23T11_08_40_3512115Z.json',

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define json raw data file schema

# COMMAND ----------

monitoring_stations_schema = StructType(
    [
        StructField("addressStreet", StringType(), True),
        StructField(
            "city",
            StructType(
                [
                    StructField(
                        "commune",
                        StructType(
                            [
                                StructField("communeName", StringType(), True),
                                StructField("districtName", StringType(), True),
                                StructField("provinceName", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("id", LongType(), True),
                    StructField("name", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("gegrLat", StringType(), True),
        StructField("gegrLon", StringType(), True),
        StructField("id", LongType(), True),
        StructField("stationName", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read stream

# COMMAND ----------

monitoring_stations = (
    spark.readStream
        .format("json")
        .schema(monitoring_stations_schema)
        .option("multiline", True)
        .load(os.path.join(WEATHER_AIR_QUALITY_CONTAINER_PATH, "raw-data/stations/*.json"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add audit columns
# MAGIC - ingestion timestamp
# MAGIC - soruce file name
# MAGIC - timestamp from source file name

# COMMAND ----------

monitor_stat_audit_col = (
    monitoring_stations
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn(
        "file_timestamp",
        parse_file_timestamp(extract_timestamp_string(col("source_file"))),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define storage location for bronze stage

# COMMAND ----------

MONITOR_STATIONS_BRONZE_SAVE_LOC = os.path.join(BRONZE_STAGE_DIR_PATH, 'monitor_stations')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to storage location in delta format

# COMMAND ----------

write_monitor_stations = (
    monitor_stat_audit_col.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            f"{os.path.join(MONITOR_STATIONS_BRONZE_SAVE_LOC, '_checkpoint')}"
        )
        .trigger(availableNow=True)
        .start(f"{MONITOR_STATIONS_BRONZE_SAVE_LOC}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create table on top of the ingested files

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS project_weather_air.air_quality.monitoring_stations_bronze
USING DELTA
LOCATION '{BRONZE_STAGE_DIR_PATH}/monitor_stations'
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean, transform and load data (SILVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define the storage location for silver stage

# COMMAND ----------

MONITOR_STATIONS_SILVER_LOC = os.path.join(SILVER_STAGE_DIR_PATH, 'monitoring_stations_silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create monitoring stations sliver table

# COMMAND ----------

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS project_weather_air.air_quality.monitoring_stations_silver
(
  station_id INT,
  street STRING,
  city_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  station_name STRING,
  processed_timestamp TIMESTAMP
)
COMMENT 'Air quality monitoring stations.'
LOCATION '{MONITOR_STATIONS_SILVER_LOC}'
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load bronze table to dataframe, perform transformations and load to silver stage
# MAGIC 1. Read bronze table as stream
# MAGIC 2. Perform deduplication:</br>
# MAGIC   2.1 Deduplication based on the latest file_timestamp column. This way latest records will be taken in case of multiple files present in the single microbatch.
# MAGIC 3. select and rename columns
# MAGIC 4. Load to silver stage using upsert.

# COMMAND ----------

# read table
monitor_stations_bronze_df = (spark.readStream
     .table("project_weather_air.air_quality.monitoring_stations_bronze")
)

# COMMAND ----------

# select and rename columns
monitor_stations_bronze_select_cols = monitor_stations_bronze_df.select(
    col("id").alias("station_id"),
    col("addressStreet").alias("street"),
    col("city.name").alias("city_name"),
    col("gegrLat").alias("latitude"),
    col("gegrLon").alias("longitude"),
    col("stationName").alias("station_name"),
    col("file_timestamp"),
)

# COMMAND ----------

def update_monitor_stations_silver(microbatch, batch_id):
    """
    Upsert to table project_weather_air.air_quality.monitoring_stations_silver
    Upsert is performed as SCD type 1.
    """

    # instantiate delta table object
    stations_silver = DeltaTable.forName(
        spark, "project_weather_air.air_quality.monitoring_stations_silver"
    )

    # perform microbatch deduplication
    microbatch_dedup = deduplicate_df(microbatch, "file_timestamp", "station_id")
    # remove duplicates in case of the same value in files_timestamp
    microbatch_dedup = microbatch_dedup.dropDuplicates(["station_id"])

    # perform upsert
    (
        stations_silver.alias("t")
        .merge(
            microbatch_dedup.alias("s"), "t.station_id = s.station_id"
        )
        .whenMatchedUpdate(
            condition="""
                t.street <> s.street OR
                t.city_name <> s.city_name OR
                t.latitude <> s.latitude OR
                t.longitude <> s.longitude OR
                t.station_name <> s.station_name
            """,
            set={
                "t.street": "s.street",
                "t.city_name": "s.city_name",
                "t.latitude": "s.latitude",
                "t.longitude": "s.longitude",
                "t.station_name": "s.station_name",
                "t.processed_timestamp": "current_timestamp()",
            },
        )
        .whenNotMatchedInsert(
            values={
                "t.station_id": "s.station_id",
                "t.street": "s.street",
                "t.city_name": "s.city_name",
                "t.latitude": "s.latitude",
                "t.longitude": "s.longitude",
                "t.station_name": "s.station_name",
                "t.processed_timestamp": "current_timestamp()"
            }
        )
        .execute()
    )

# COMMAND ----------

write_to_silver_query = (monitor_stations_bronze_select_cols
     .writeStream
     .foreachBatch(update_monitor_stations_silver)
     .outputMode("update")
     .option(
         "checkpointLocation",
         f"{os.path.join(MONITOR_STATIONS_SILVER_LOC, '_checkpoint')}"
     )
     .trigger(availableNow=True)
     .start()
)

# COMMAND ----------


