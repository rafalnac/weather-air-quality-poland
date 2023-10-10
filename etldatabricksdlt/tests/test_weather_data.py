import sys

import pytest

from pathlib import Path

# Add root directory to the path
sys.path.append(str(Path(__file__).parent.parent))
from ingestion import weather_data


@pytest.fixture(scope="session")
def spark_session():
    global spark
    try:
        spark
    except NameError:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    yield spark

def test_weather_data_bronze(spark_session):
    assert isinstance(weather_data.weather_data_bronze, spark_session.DataFrame)

