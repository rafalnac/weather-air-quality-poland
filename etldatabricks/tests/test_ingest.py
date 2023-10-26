"""
Module to test ihelpers.py
"""
import sys
import os

import pytest

from datetime import datetime
from pathlib import Path

from pyspark.testing.utils import assertSchemaEqual
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from functions.ingest import add_metadata
from functions.general import load_config_file

# Add root directory to the path
# sys.path.append(str(Path(__file__).parent.parent))


@pytest.fixture(scope="session")
def spark_session():
    """Creates SparkSession."""

    global spark
    try:
        spark
    except NameError:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    yield spark


@pytest.fixture(scope="module")
def weather_raw_data_schema() -> StructType:
    """Returns schema for weather_raw_data sample json file."""

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

    return json_schema


@pytest.fixture(scope="module")
def weather_raw_data_sample(spark_session, weather_raw_data_schema):
    """Returns weather raw data sample as spark dataframe."""

    # load config file
    cfg_file_path = str(Path(__file__).parent.parent / "config.toml")
    cfg_file = load_config_file(cfg_file_path)

    # get path for weather raw data
    weather_raw_data_path = cfg_file["unit_test"]["weather_raw_data_sample_path"]

    # load data into dataframe
    sample_df = (
        spark_session.read.format("json")
        .schema(weather_raw_data_schema)
        .load(weather_raw_data_path)
    )

    return sample_df


def test_add_metadata(spark_session, weather_raw_data_schema, weather_raw_data_sample):
    sample_data_schema = weather_raw_data_schema.add(
        "file_name", StringType(), False
    ).add("file_modification_time", TimestampType(), False)

    sample_timestamp = datetime.utcnow()
    sample_data = [
        {
            "id_stacji": 12295,
            "stacja": "Białystok",
            "data_pomiaru": "2023-09-11",
            "godzina_pomiaru": 13,
            "temperatura": 27.7,
            "predkosc_wiatru": 2,
            "kierunek_wiatru": 260,
            "wilgotnosc_wzgledna": 39.1,
            "suma_opadu": 0.0,
            "cisnienie": 1014.3,
            "file_name": "weather_data_sample.json",
            "file_modification_time": sample_timestamp,
        }
    ]

    expected_df = spark_session.createDataFrame(sample_data, sample_data_schema)
    transformed_df = add_metadata(weather_raw_data_sample)

    assertSchemaEqual(transformed_df.schema, expected_df.schema)
