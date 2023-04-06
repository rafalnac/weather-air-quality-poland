import logging
import json
import requests

import pyodbc
import azure.functions as func
from .connect_config import syn_sql_pool_conn_string

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    # instanciate connection obj
    cnx = pyodbc.connect(syn_sql_pool_conn_string)

    logging.info("Established connection to server")

    # define query to select measure point ids from view [air_quality].[v_measure_points]
    query_select = """
    SELECT TOP 3
        [point_id]
    FROM [air_quality].[v_measure_points]
    """

    # instanciate cursor object
    cursor_object = cnx.cursor()

    # execute the query
    cursor_object.execute(query_select)

    # fetch the records
    records = cursor_object.fetchall()

    # convert records from List[Row] to List[Tuple]
    records = [tuple(record) for record in records]

    # unpack nested tuples to list
    records_flat = [item for nested_tuple in records for item in nested_tuple]

    api_list = []
    for point_id in records_flat:
        req = requests.get(
            url=f"https://api.gios.gov.pl/pjp-api/rest/data/getData/{point_id}"
        )
        # convert response object to JSON
        req_json = req.json()

        # specify point_id key to update JSON
        point_id_to_insert = {"point_id": point_id}

        # update_json with point id
        req_json.update(point_id_to_insert)

        # append to the list
        api_list.append(req_json)
    
    # convert list to JSON
    api_json = json.dumps(obj=api_list)

    return func.HttpResponse(
        body=api_json,
        status_code=200
    )

