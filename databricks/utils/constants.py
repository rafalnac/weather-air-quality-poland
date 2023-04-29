import os

WEATHER_AIR_QUALITY_CONTAINER_PATH = "abfss://weather-air-quality-poland@ststreamingaccdev.dfs.core.windows.net"
RAW_DATA_PATH = os.path.join(WEATHER_AIR_QUALITY_CONTAINER_PATH, "raw-data")
BRONZE_STAGE_DIR_PATH = os.path.join(WEATHER_AIR_QUALITY_CONTAINER_PATH, "bronze")
SILVER_STAGE_DIR_PATH = os.path.join(WEATHER_AIR_QUALITY_CONTAINER_PATH, "silver")
GOLD_STAGE_DIR_PATH = os.path.join(WEATHER_AIR_QUALITY_CONTAINER_PATH, "gold")