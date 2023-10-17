"""
Module contains functions for data ingetions.
"""
from pyspark.sql.functions import col
from pyspark.sql.connect.dataframe import DataFrame


def add_metadata(df: DataFrame) -> DataFrame:
    """
    Adds file_name and file_modification_time column to dataframe.

    Args:
        df: pyspark.sql.Dataframe or pyspark.sql.connect.dataframe.Dataframe

    Returns:
        Dataframe
    """

    # df_metadata = df.select(
    #     "*", "_metadata.file_name", "_metadata.file_modification_time"
    # )
    df_metadata = df.select("*", "_metadata")
    return df_metadata




