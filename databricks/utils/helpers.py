"""Module provided functions to simplify data transfromations inside notebooks."""

from typing import Union
from pyspark.sql.functions import regexp_extract, to_timestamp, Column


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