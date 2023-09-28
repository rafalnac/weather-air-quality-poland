# Databricks notebook source
import os

from pyspark.sql.functions import col
from delta.tables import DeltaTable

from etldatabricks.utils.constants import GOLD_STAGE_DIR_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create station dimension table
# MAGIC One station can contain multiple points
# MAGIC 1) Join measure_points_silver and monitoring_stations_silver (ON station_id)
# MAGIC 2) Select required columns

# COMMAND ----------

measurement_points_df = spark.read.table("project_weather_air.air_quality.measure_points_silver")

# COMMAND ----------

stations_df = spark.read.table("project_weather_air.air_quality.monitoring_stations_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns in station_df before join to avoind ambiguity

# COMMAND ----------

stations_df_renamed = (
    stations_df
        .withColumnRenamed("processed_timestamp", "processed_timestamp_stations")
)

# COMMAND ----------

measurement_dim_df = (
    measurement_points_df
        .join(stations_df_renamed, "station_id", "left")
)

# COMMAND ----------

measurement_dim_final = (
    measurement_dim_df
        .withColumnRenamed("param_code", "parameter_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define location to store table dim_measurement_point

# COMMAND ----------

storage_loc_dim_measurement_point = os.path.join(GOLD_STAGE_DIR_PATH, "dim_measurement_point")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create dimension table dim_measure_point

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.air_quality.dim_measurement_point
(
  point_sk BIGINT CONSTRAINT measure_point_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1),
  point_id INT NOT NULL,
  parameter_name STRING,
  city_name STRING,
  street STRING,
  latitude DOUBLE,
  longitude DOUBLE
)
USING DELTA
LOCATION '{storage_loc_dim_measurement_point}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Upsert to delta table

# COMMAND ----------

# instanciate target table as Delta Table object
dim_measure_point_delta = DeltaTable.forName(spark, "project_weather_air.air_quality.dim_measurement_point")

# COMMAND ----------

# perform upsert
(
    dim_measure_point_delta.alias("t")
    .merge(measurement_dim_final.alias("s"), "t.point_id = s.point_id")
    .whenNotMatchedInsert(
        values={
            "t.point_id": "s.point_id",
            "t.parameter_name": "s.parameter_name",
            "t.city_name": "s.city_name",
            "t.street": "s.street",
            "t.latitude": "s.latitude",
            "t.longitude": "s.longitude"
        }
    )
    .whenMatchedUpdate(
        condition="""
            t.parameter_name <> s.parameter_name OR
            t.latitude <> s.latitude OR
            t.longitude <> s.longitude
        """,
        set={
            "t.parameter_name":"s.parameter_name",
            "t.city_name": "s.city_name",
            "t.street": "s.street",
            "t.latitude": "s.latitude",
            "t.longitude": "s.longitude"
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM project_weather_air.air_quality.dim_measurement_point;

# COMMAND ----------


