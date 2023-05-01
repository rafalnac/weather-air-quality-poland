"""Module provided functions to simplify data transfromations inside notebooks."""

from typing import Union
from pyspark.sql.functions import regexp_extract, to_timestamp, Column, DataFrame


def parse_file_timestamp(
    timestamp_str: Union[str, Column], parse_pat: str = "yyyy-MM-dd'T'HH_mm_ss_SSSSSSSX"
) -> Column:
    """
    Function extract timestamp from file name and parse it to the timestamp value.

    Args:
        timestamp_str: Timestamp as string to parse.
        parse_pat: Timestamp pattern.
            According to https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    Returns
        Column: Column with parsed string to pyspark.sql.types.TimestampType.
            Function returns null if pattern not matched.
    """

    return to_timestamp(timestamp_str, parse_pat)


def extract_timestamp_string(
    text: Union[str, Column], pat: str = "\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}_\d{7}Z"
) -> Column:
    """
    Extract timestamp string from provided string colum.
    In case of multiple timestamps present, firs one is returned.

    Args:
        str: Column or name of the column which contains string,
        pat: Java regular expression pattern to search. Default expected timestamp format
            'yyyy-MM-dd'T'HH_mm_ss_SSSSSSSX' -> Egz.'2023-03-23T11_08_40_3512115Z'.
            Format provided accoridng to https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    Returns:
        Column with found pattern. In case regex not found returns null.
    """

    return regexp_extract(text, pat, 0)


def deduplicate_df(
    df: DataFrame, timestamp_col: str, partition_by: str
) -> DataFrame:
    """
    Deduplicate dataframe based on maximum value of timestamp_col
    for specified partition_by group.

    Function deduplicates provided dataframe using window funcion.
    It uses aggregate funcion MAX() to get latest timestamp(timestamp_col)
    for every group(partition_by).

    Args:
        timestamp_col: Name of the column from dataframe based on what window funicion MAX()
            will be executed.
        partition_by: Name of the column from dataframe to define grouping scope for MAX() function.

    Returns: Deduped dataframe with addtional column 'max_timestamp'

    Raises:
        TypeError Exception: When column is not a string type.
    """

    if not isinstance(timestamp_col, str) or not isinstance(partition_by, str):
        raise TypeError("Column must be a string")

    df_dedup = (
        df.selectExpr(
        "*", f"MAX({timestamp_col}) OVER (PARTITION BY {partition_by}) AS max_timestamp"
        )
        .where(f"{timestamp_col} = max_timestamp")
        .selectExpr("* EXCEPT(max_timestamp)")
    )

    return df_dedup