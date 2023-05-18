# Databricks notebook source
import os

from pyspark.sql.functions import col, broadcast
from delta.tables import DeltaTable

from databricks.utils.constants import GOLD_STAGE_DIR_PATH
from databricks.utils.helpers import deduplicate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create fact table fact_measurement_data
# MAGIC Perform stream static join between each microbatch from measurement_data_silver table</br>and latest version of dimension tabledim_measurement_point table.
# MAGIC 1) Read stream - 'measure_points_data_silver'
# MAGIC 2) Static read - 'dim_measurement_point'
# MAGIC 3) Perform stream - static join
# MAGIC 4) Upsert into fact table 'fact_measurement_data'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Stream read: 'measure_points_data_silver' -> Delta Table as stream source

# COMMAND ----------

measure_points_data_silver = (
    spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 7)
        .table("project_weather_air.air_quality.measure_points_data_silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Static read: 'dim_measurement_point'

# COMMAND ----------

dim_measurement_point = spark.read.table("project_weather_air.air_quality.dim_measurement_point")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename 'parameter_name' to avoid ambiguity after join

# COMMAND ----------

dim_measurement_point = (
    dim_measurement_point
        .withColumnRenamed("parameter_name", "parameter_name_dim")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get only new and changed rows

# COMMAND ----------

measure_points_data_silver_new = (
    measure_points_data_silver
        .filter(col("_change_type").isin(["insert", "update_postimage"]))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join microbatch and dimesion table
# MAGIC - Unique row from dimesion table point_id and parameter_name

# COMMAND ----------

measurement_data_point_joined = (
    measure_points_data_silver_new
        .join(
            broadcast(dim_measurement_point),
            "point_id"
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create fact table fact_measurement_points_data

# COMMAND ----------

# create storage location for fact table
storage_location_fact = os.path.join(GOLD_STAGE_DIR_PATH, "fact_measurement_points_data")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS project_weather_air.air_quality.fact_measurement_points_data
(
  measure_sk BIGINT CONSTRAINT measure_points_data_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1),
  measure_id STRING,
  measure_date DATE,
  measure_value DOUBLE,
  point_sk BIGINT NOT NULL CONSTRAINT points_data_measurement_point_fk FOREIGN KEY
    REFERENCES project_weather_air.air_quality.dim_measurement_point(point_sk) 
)
USING DELTA
LOCATION '{storage_location_fact}'
""")

# COMMAND ----------

def upsert_to_delta(microbatch, batch_id):
    """Upsert to project_weather_air.air_quality.fact_measurement_points_data"""

    # Instanciate Delta Table object
    target = DeltaTable.forName(spark, "project_weather_air.air_quality.fact_measurement_points_data")

    # Perform microbatch deduplication
#     source_deduped = deduplicate_df(microbatch, "measure_id", "_commit_timestamp")

    # Upsert
    (
        target.alias("t")
            .merge(
                microbatch.alias("s"),
                "t.measure_id = s.measure_id"
            )
            .whenNotMatchedInsert(
                values={
                    "t.measure_id": "s.measure_id",
                    "t.measure_date": "s.measure_date",
                    "t.measure_value": "s.measure_value",
                    "t.point_sk": "s.point_sk"
                }
            )
            .whenMatchedUpdate(
                # measure_date is part of the measure_id
                # Because of that if measure_data will change measure_id either and first the row will be inserted, not updated
                set={
                    "t.measure_value": "s.measure_value"
                }
            )
            .execute()
    )

# COMMAND ----------

write_streaming_query = (
    measurement_data_point_joined
        .writeStream
        .format("delta")
        .outputMode("update")
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", os.path.join(storage_location_fact, "_checkpoint"))
        .trigger(availableNow=True)
        .start()
)
