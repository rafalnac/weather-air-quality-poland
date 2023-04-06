import logging
import json
import requests

import pyodbc
import azure.functions as func
from .connect_config import syn_sql_pool_conn_string

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    # Instanciate connection obj
    cnx = pyodbc.connect(syn_sql_pool_conn_string)

    logging.info("Established connection to server")

    # Define query to select measure point ids from view [air_quality].[v_measure_points]
    query_select = """
    SELECT TOP 3
        [point_id]
    FROM [air_quality].[v_measure_points]
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
    for point_id in records_flat:
    
        # Get measurment data for each point_id
        reqs = requests.get(
            url=f"https://api.gios.gov.pl/pjp-api/rest/data/getData/{point_id}"
        )
        # Convert response object to JSON
        req_json = reqs.json()

        # Specify point_id key to update JSON
        point_id_to_insert = {"point_id": point_id}

        # Update_json with point id
        req_json.update(point_id_to_insert)

        # Append to the list
        api_list.append(req_json)
    
    # Convert list to JSON
    api_json = json.dumps(obj=api_list)

    return func.HttpResponse(
        body=api_json,
        status_code=200
    )

