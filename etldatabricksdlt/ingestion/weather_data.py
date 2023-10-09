import dlt

from pathlib import Path
from pyspark.sql.functions import col

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

try:
    import tomlib
except ModuleNotFoundError:
    import tomli as tomlib

config_file_path = Path(__file__).parent.parent / "config.toml"
cfg_file = tomlib.loads(config_file_path.read_text())

raw_data_dev_path = cfg_file["paths_dev"]["raw_data_path"]


@dlt.table
def weather_data_bronze():
    data = spark.readStream.format("json").load(raw_data_dev_path)
    return data
