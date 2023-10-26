"""
Ingest raw data contains weather conditions.
"""
import sys
from pathlib import Path

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import StructType, StructField, StringType

# add root dir to path
root_dir_path = Path(__file__).parent.parent.parent
sys.path.append(str(root_dir_path))

from functions.ingest import add_metadata
from functions.general import load_config_file

environment = sys.argv[1]


# load config file
cfg_file_path = str(root_dir_path / "config.toml")
cfg_file = load_config_file(cfg_file_path)

# get paths for development environment
weather_data_raw_path = f'{cfg_file[{environment}]["path_raw_data"]["weather_data"]}/.*json'
weather_data_ingestion_sink_path = cfg_file[environment]["path_ingestion"]["weather_data"]


def weather_data_ingestion(spark_session: DatabricksSession) -> DataFrameWriter:
    """Ingest weather raw data and write it to storage location as delta table.

    Function additionaly add metadata(file_name, file_modification_time)

    Returns:
        pyspark.sql.DataFrameWriter
    """

    # define schema
    json_schema = StructType(
        [
            StructField("id_stacji", StringType(), True),
            StructField("stacja", StringType(), True),
            StructField("data_pomiaru", StringType(), True),
            StructField("godzina_pomiaru", StringType(), True),
            StructField("temperatura", StringType(), True),
            StructField("predkosc_wiatru", StringType(), True),
            StructField("kierunek_wiatru", StringType(), True),
            StructField("wilgotnosc_wzgledna", StringType(), True),
            StructField("suma_opadu", StringType(), True),
            StructField("cisnienie", StringType(), True),
        ]
    )

    # load weather raw data
    weather_raw_data_df = (
        spark_session.read.format("json")
        .schema(json_schema)
        .mode("overwirte")
        .load(weather_data_raw_path)
    )

    # add metadata
    weather_raw_data_df_metadata = add_metadata(weather_raw_data_df)

    # write to storage location
    return weather_raw_data_df_metadata.write.format("delta").save(
        weather_data_ingestion_sink_path
    )


if __name__ == "__main__":
    # initialize spark session
    spark = DatabricksSession.builder.getOrCreate()
    weather_data_ingestion(spark)

    # create raw data delta table based on the ingested files
    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS project_weather_air_{environment}.weather_data.weather_data_ingestion
    USING DELTA
    LOCATION "{weather_data_ingestion_sink_path}";
    """
    )