# Databricks notebook source
import os

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, input_file_name, col, explode, lit, concat, date_format
from delta.tables import DeltaTable

from etldatabricks.utils.constants import WEATHER_AIR_QUALITY_CONTAINER_PATH, RAW_DATA_PATH, BRONZE_STAGE_DIR_PATH, SILVER_STAGE_DIR_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest Measure Points Data (BRONZE)
# MAGIC - Each file comes in multiline JSON format.
# MAGIC - Expects that each file name contains timestamp in ISO format.
# MAGIC - File name example 'measure_points_data_2023-04-12T09_5402.661677+00_00.json'.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define schema for measure points data JSON

# COMMAND ----------

measure_points_data_schema = StructType(
    [
        StructField("key", StringType(), True),
        StructField("point_id", LongType(), True),
        StructField(
            "values",
            ArrayType(
                StructType(
                    [
                        StructField("date", StringType(), True),
                        StructField("value", DoubleType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
    ]
)

# COMMAND ----------

measure_points_data = (
    spark.readStream
        .format("JSON")
        .schema(measure_points_data_schema)
        .option("multiline", True)
        .option("inferSchema", True)
        .load(os.path.join(RAW_DATA_PATH, "measure_points_data/*.json"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add audit columns
# MAGIC - ingestion timestamp
# MAGIC - soruce file name

# COMMAND ----------

measure_points_data_audit_col = (measure_points_data
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define storage location for bronze table

# COMMAND ----------

measure_points_data_bronze_save_loc = os.path.join(BRONZE_STAGE_DIR_PATH, "measure_points_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to bronze table
# MAGIC - stream write
# MAGIC - output mode: append

# COMMAND ----------

write_measure_points_data = (
    measure_points_data_audit_col
        .writeStream
        .outputMode("append")
        .option(
            "checkpointLocation",
            os.path.join(measure_points_data_bronze_save_loc, "_checkpoint")
        )
        .trigger(availableNow=True)
        .start(measure_points_data_bronze_save_loc)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create delta table based on the ingested files

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.air_quality.measure_points_data_bronze
USING DELTA
LOCATION "{measure_points_data_bronze_save_loc}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform and load data to silver table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define storage location for silver table

# COMMAND ----------

measure_points_data_silver_loc = os.path.join(SILVER_STAGE_DIR_PATH, "measure_points_data_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load bronze table to dataframe

# COMMAND ----------

measure_data_bronze_df = (
    spark.readStream
        .table("project_weather_air.air_quality.measure_points_data_bronze")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Move array from 'values' column to rows

# COMMAND ----------

measure_data_expl = measure_data_bronze_df.select(
    "*",
    explode(col("values")).alias("data")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select required columns

# COMMAND ----------

measure_data_expl_selected = measure_data_expl.select(
    col("key").alias("parameter_name"),
    col("point_id"),
    col("data.date").alias("measure_date"),
    col("data.value").alias("measure_value")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create natural key
# MAGIC - Unique identifier will be craeted by concatenation 'parameter_name', 'point_id' and 'date'.
# MAGIC - 'Date' attribute specified when measurment was taken.

# COMMAND ----------

measure_data_expl_id = measure_data_expl_selected.withColumn(
    "measure_id",
    concat(
        col("parameter_name"),
        lit("_"),
        col("point_id"),
        lit("_"),
        date_format("measure_date", "yyyyMMdd")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create silver fact table 'measure_points_data_silver'

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.air_quality.measure_points_data_silver
(
  measure_id STRING,
  parameter_name STRING,
  point_id INT,
  measure_date DATE,
  measure_value DOUBLE,
  processed_timestamp TIMESTAMP
)
LOCATION "{measure_points_data_silver_loc}"
"""
)

# COMMAND ----------

# %sql
# ALTER TABLE project_weather_air.air_quality.measure_points_data_silver SET TBLPROPERTIES(delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Upsert
# MAGIC - Insert only rows which are not already in the silver table
# MAGIC
# MAGIC Each microbatch may contains duplicates because each API call
# MAGIC (https://api.gios.gov.pl/pjp-api/rest/data/getData/{measure_point})
# MAGIC returns data for 3 last days, starting from 01:00:00 AM.
# MAGIC
# MAGIC Example:
# MAGIC Time of HTTP request: 31.03 14:00, data returned: starting 29.03 01:00:00 AM,
# MAGIC increment every hour until 31.03 14:00.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create upsert function for each microbatch
# MAGIC 1) Dedup the data in batch
# MAGIC 2) Upsert to silver table 'measure_points_data_silver'

# COMMAND ----------

def update_silver_table(microbatch, batch_id):

    # perform deduplication
    batch_dedup = microbatch.dropDuplicates(["measure_id"])

    # instanciate delta table object
    target = DeltaTable.forName(
        spark,
        "project_weather_air.air_quality.measure_points_data_silver"
    )

    # insert only new data
    upsert_query = (
        target.alias("t")
            .merge(
                batch_dedup.alias("s"),
                "s.measure_id = t.measure_id"
            )
            .whenNotMatchedInsert(
                values={
                    "t.measure_id": "s.measure_id",
                    "t.parameter_name": "s.parameter_name",
                    "t.point_id": "s.point_id",
                    "t.measure_date": "s.measure_date",
                    "t.measure_value": "s.measure_value",
                    "t.processed_timestamp": current_timestamp()
                }
            )
            .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Update silver table

# COMMAND ----------

upsert_query = (
    measure_data_expl_id
        .writeStream
        .outputMode("update")
        .foreachBatch(update_silver_table)
        .option("checkpointLocation",
               os.path.join(measure_points_data_silver_loc, "_checkpoint")
               )
        .trigger(availableNow=True)
        .start()
)

# COMMAND ----------

# %sql
# SELECT * FROM project_weather_air.air_quality.measure_points_data_silver;
