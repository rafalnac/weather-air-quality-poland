# Databricks notebook source
import os

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, input_file_name, concat, col, date_format, lit
from delta.tables import DeltaTable

from databricks.utils.constants import RAW_DATA_PATH, BRONZE_STAGE_DIR_PATH, SILVER_STAGE_DIR_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest weather data (Bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define schema for JSON file

# COMMAND ----------

json_schema = StructType(
    [
        StructField("cisnienie", StringType(), True),
        StructField("data_pomiaru", DateType(), True),
        StructField("godzina_pomiaru", StringType(), True),
        StructField("id_stacji", StringType(), True),
        StructField("kierunek_wiatru", StringType(), True),
        StructField("predkosc_wiatru", StringType(), True),
        StructField("stacja", StringType(), True),
        StructField("suma_opadu", StringType(), True),
        StructField("temperatura", StringType(), True),
        StructField("wilgotnosc_wzgledna", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Specify raw data storage location

# COMMAND ----------

raw_data_path = os.path.join(RAW_DATA_PATH, "weather-data/*.json")

# COMMAND ----------

weather_raw_data = (
    spark.readStream
        .format("json")
        .schema(json_schema)
        .load(raw_data_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add audit column
# MAGIC 1) Ingestion timestamp
# MAGIC 2) Source file name

# COMMAND ----------

weather_data_audit_cols = (
    weather_raw_data
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define storage location for bronze table

# COMMAND ----------

weather_raw_data_bronze = os.path.join(BRONZE_STAGE_DIR_PATH, "weather_data_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to data lake in delta format

# COMMAND ----------

(
    weather_data_audit_cols.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", os.path.join(weather_raw_data_bronze, "_checkpoint"))
    .trigger(availableNow=True)
    .start(weather_raw_data_bronze)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create table based on the ingested files

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.weather_data.weather_data_bronze
USING DELTA
LOCATION "{weather_raw_data_bronze}";
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean, transfrom and load (Silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Storage location for silver table

# COMMAND ----------

weather_data_location_silver = os.path.join(SILVER_STAGE_DIR_PATH, "weather_data_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create silver table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.weather_data.weather_data_silver
(
  weather_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1),
  weather_nk STRING,
  pressure FLOAT,
  measurement_date DATE,
  measurment_hour INT,
  station_id INT,
  wind_direction INT,
  wind_speed INT,
  station_name STRING,
  precipitation DOUBLE,
  temperature FLOAT,
  humidity DOUBLE
)
USING DELTA
LOCATION "{weather_data_location_silver}";
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Enable CDC to existing table 'project_weather_air.weather_data.weather_data_silver'

# COMMAND ----------

# %sql
# ALTER TABLE project_weather_air.weather_data.weather_data_silver SET TBLPROPERTIES(delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load bronze table to dataframe

# COMMAND ----------

weather_data_bronze_df = spark.readStream.table(
    "project_weather_air.weather_data.weather_data_bronze"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Perform transformations
# MAGIC 1) Change names to english equivalents
# MAGIC 2) Replace nulls to '-1' in the 'preasure' column
# MAGIC 3) Create natural key: measurment_date_measurment_hour_station_id

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename polish column names to english equivalents

# COMMAND ----------

weather_data_eng_df = (
    weather_data_bronze_df
        .withColumnRenamed("cisnienie", "pressure")
        .withColumnRenamed("data_pomiaru", "measurement_date")
        .withColumnRenamed("godzina_pomiaru", "measurment_hour")
        .withColumnRenamed("id_stacji", "station_id")
        .withColumnRenamed("kierunek_wiatru", "wind_direction")
        .withColumnRenamed("predkosc_wiatru", "wind_speed")
        .withColumnRenamed("stacja", "station_name")
        .withColumnRenamed("suma_opadu", "precipitation")
        .withColumnRenamed("temperatura", "temperature")
        .withColumnRenamed("wilgotnosc_wzgledna", "humidity")
)

# COMMAND ----------

weather_data_cleaned_df = (
    weather_data_eng_df
        # fill null values with -1 in the 'pressure' column
        .fillna(
            value={
                "pressure": -1
            }
        )  
)

# COMMAND ----------

weather_data_transformed_df = (
    weather_data_cleaned_df
        # create natural key
        .withColumn(
            "weather_nk",
            concat(
                date_format("measurement_date", "yyyyMMdd"),
                lit("_"),
                col("measurment_hour"),
                lit("_"),
                col("station_id")
            )
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Append only now rows to the silver table

# COMMAND ----------

def append_to_delta(microbatch, batch_id):
    """Append only new rows to silver table."""

    # Perform microbatch deduplication
    batch_dedup = microbatch.dropDuplicates(["weather_nk"])

    # Instanciate delta table object
    target = DeltaTable.forName(
        spark,
        "project_weather_air.weather_data.weather_data_silver"
    )

    # Merge
    merge_query = (
        target.alias("t")
            .merge(
                batch_dedup.alias("s"),
                "t.weather_nk = s.weather_nk"
            )
            .whenNotMatchedInsert(
                values = {
                    "t.weather_nk": "s.weather_nk",
                    "t.pressure": "s.pressure",
                    "t.measurement_date": "s.measurement_date",
                    "t.measurment_hour": "s.measurment_hour",
                    "t.station_id": "s.station_id",
                    "t.wind_direction": "s.wind_direction",
                    "t.wind_speed": "s.wind_speed",
                    "t.station_name": "s.station_name",
                    "t.precipitation": "s.precipitation",
                    "t.temperature": "s.temperature",
                    "t.humidity": "s.humidity" 
                }
            )
            .execute()
    )

# COMMAND ----------

write_query = (
    weather_data_transformed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .foreachBatch(append_to_delta)
    .trigger(availableNow=True)
    .start()
)
