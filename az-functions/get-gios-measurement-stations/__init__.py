import logging
import json
import datetime

import requests
import pyodbc
import azure.functions as func

from .connection_config import syn_sql_pool_conn_string, container_weather_dir_raw_data
from ..transform.transfom import FileName


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    # Instanciate connection obj
    cnx = pyodbc.connect(syn_sql_pool_conn_string)

    logging.info("Established connection to server")

    # Define query to select measure point ids from view [air_quality].[v_monitoring_stations]
    query_select = f"""
    SELECT
        [station_id]
    FROM [air_quality].[v_monitoring_stations]
    """

    # Instanciate cursor object
    cursor_object = cnx.cursor()

    # Execute the query
    cursor_object.execute(query_select)

    # Fetch the records
    records = cursor_object.fetchall()

    # Convert records from List[Row] to List[Tuple]
    records = [tuple(record) for record in records]

    # Unpack nested tuples to list
    records_flat = [item for nested_tuple in records for item in nested_tuple]

    # Initialize a list to store JSON objects
    api_list = []

    # Loop through each point_id
    for station_id in records_flat:
        # Get measurement_points_for_each_station
        reqs = requests.get(
            url=f"https://api.gios.gov.pl/pjp-api/rest/station/sensors/{station_id}"
        )
        # Convert response object to JSON
        req_json = reqs.json()

        # Append JSONs to the list
        api_list.append(req_json)
    

    # Convert list with nested JSONs to string
    api_json = json.dumps(obj=api_list)

    # Crete file name
    iso_timestamp = FileName.get_iso_timestamp()
    file_name = FileName("measure_points_station")
    file_name = file_name.add_suffix(iso_timestamp)

    # Create blob in container
    container_weather_dir_raw_data.upload_blob(
        name=f"{file_name}.json",
        data=api_json,
        blob_type="BlockBlob",
    )

    return func.HttpResponse(status_code=200)
