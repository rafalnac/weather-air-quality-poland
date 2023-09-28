# Databricks notebook source
import os

from pyspark.sql.functions import current_timestamp, input_file_name, schema_of_json, col
from pyspark.sql.types import *
from delta.tables import DeltaTable

from etldatabricks.utils.constants import WEATHER_AIR_QUALITY_CONTAINER_PATH, RAW_DATA_PATH, BRONZE_STAGE_DIR_PATH, SILVER_STAGE_DIR_PATH
from etldatabricks.utils.helpers import extract_timestamp_string, parse_file_timestamp, deduplicate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest sensor data (BRONZE)
# MAGIC Each station has muliple sensors. Each sensor measures level of different chemical compound.
# MAGIC - Each file comes in the json format
# MAGIC - Expect that each file name contains timestamp.
# MAGIC - File name example 'measure_points_station_2023-05-18T07_41_15.611049+00_00.json'
# MAGIC - Timestamp pattern format according to: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define schema for measure points JSON files

# COMMAND ----------

measure_points_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField(
            "param",
            StructType(
                [
                    StructField("idParam", LongType(), True),
                    StructField("paramCode", StringType(), True),
                    StructField("paramFormula", StringType(), True),
                    StructField("paramName", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("stationId", LongType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read measure points

# COMMAND ----------

measure_points_df = (
    spark.readStream.format("json")
    .option("mulitLine", "true")
    .schema(measure_points_schema)
    .load(os.path.join(RAW_DATA_PATH, "measure_points/*.json"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add audit columns
# MAGIC - ingestion timestamp
# MAGIC - soruce file name
# MAGIC - timestamp from source file name

# COMMAND ----------

measure_points_audit_col = (
    measure_points_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn(
        "file_timestamp",
        parse_file_timestamp(
            timestamp_str=extract_timestamp_string(col("source_file"), pat="\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}"),
            parse_pat="yyyy-MM-dd'T'HH_mm_ss"
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define storage location for bronze stage

# COMMAND ----------

MEASURE_POINTS_BRONZE_SAVE_LOC = os.path.join(BRONZE_STAGE_DIR_PATH, "measure_points")

# COMMAND ----------

write_measure_points = (
    measure_points_audit_col
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{os.path.join(MEASURE_POINTS_BRONZE_SAVE_LOC, '_checkpoint')}")
        .trigger(availableNow=True)
        .start(f"{MEASURE_POINTS_BRONZE_SAVE_LOC}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Build delta table based on the ingested files

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.air_quality.measure_points_bronze
USING DELTA
LOCATION "{MEASURE_POINTS_BRONZE_SAVE_LOC}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform and load data to silver table
# MAGIC 1) Load bronze delta table to dataframe
# MAGIC 2) Read required columns from nested json, select and rename columns
# MAGIC 3) Create silver talbe (if not already exist)
# MAGIC 4) Upsert:
# MAGIC       - Deduplicate microbatch based on the latest file timestamp
# MAGIC       - Upsert to silver table

# COMMAND ----------

MEASURE_POINTS_SILVER_LOC = os.path.join(SILVER_STAGE_DIR_PATH, 'measure_points_silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load bronze delta table to dataframe

# COMMAND ----------

monitor_stations_bronze_df = spark.readStream.table(
    "project_weather_air.air_quality.measure_points_bronze"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Measure Points - Read required columns from nested json, select and rename columns

# COMMAND ----------

monitor_stations_expand_rename_cols = (
    monitor_stations_bronze_df
        .select("id", "param.idParam", "param.paramCode", "stationId", "file_timestamp")
        .withColumnRenamed("id", "point_id")
        .withColumnRenamed("stationId", "station_id")
        .withColumnRenamed("idParam", "param_id")
        .withColumnRenamed("paramCode", "param_code")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create measure points silver table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.air_quality.measure_points_silver 
  (
    point_id INT,
    station_id INT,
    param_id INT,
    param_code STRING,
    processed_timestamp TIMESTAMP
  )
USING DELTA
COMMENT "Measure points with their parameters"
LOCATION "{MEASURE_POINTS_SILVER_LOC}"
""")

# COMMAND ----------

def update_measure_points_silver(microbatch, batch_id):
    """
    Performs SCD Type 1 upsert to the table:
    project_weather_air.air_quality.dim_measure_points
    """

    # deduplicate microbatch - measure points
    microbatch_dedup = deduplicate_df(microbatch, "file_timestamp", "point_id")

    # create temp view from microbatch df
    microbatch_dedup.createOrReplaceTempView("microbatch_measure_points")

    # upsert query
    upsert_query = """
        MERGE INTO project_weather_air.air_quality.measure_points_silver AS t
        USING microbatch_measure_points AS s
        ON s.point_id = t.point_id
        WHEN MATCHED AND
            t.station_id <> s.station_id OR
            t.param_id <> s.param_id OR
            t.param_code <> s.param_code
        THEN UPDATE SET
            t.station_id = s.station_id,
            t.param_code = s.param_code,
            t.processed_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            t.point_id,
            t.station_id,
            t.param_id,
            t.param_code,
            t.processed_timestamp
          )
        VALUES (
            s.point_id,
            s.station_id,
            s.param_id,
            s.param_code,
            current_timestamp()
        )
    """

    microbatch_dedup._jdf.sparkSession().sql(upsert_query)

# COMMAND ----------

monitor_stations_to_merge_df = monitor_stations_expand_rename_cols

def stream_measures_upsert_to_silver():
    writer = (monitor_stations_to_merge_df
        .writeStream
        .foreachBatch(update_measure_points_silver)
        .outputMode("update")
        .option("checkpointLocation", f"{os.path.join(MEASURE_POINTS_SILVER_LOC, '_checkpoint')}")
        .trigger(availableNow=True)
        .start()
    )
    return writer

# COMMAND ----------

stream_measures_upsert_to_silver()

# COMMAND ----------

# %sql
# SELECT
#   point_id,
#   COUNT(*)
# FROM project_weather_air.air_quality.measure_points_silver
# GROUP BY point_id
# HAVING COUNT(*) > 1;
