import datetime
import json
import logging
import requests
import time

import azure.functions as func

from ..transform.transfom import FileName
from .connect import container_weather_raw_data


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    # API URL
    weather_data_api_url = "https://danepubliczne.imgw.pl/api/data/synop"

    # Explicitly set encoding to corectlly read polish characters
    headers = {"charset": "utf-8 BOM"}
    # API call
    try:
        reqst = requests.get(weather_data_api_url, headers=headers)
    # Rerun the request in case of timeout
    except requests.exceptions.Timeout as e:
        num_sec_to_wait = 5
        logging.info(
            f"{e}.\nTrying again to perform API request in {num_sec_to_wait} sec."
        )
        time.sleep(num_sec_to_wait)
        reqst = requests.get(weather_data_api_url, headers=headers)

    # Convert Response object to JSON
    reqst_data = reqst.json()

    # Convert JSON to string. JSON contains polish characters
    json_str = json.dumps(reqst_data, ensure_ascii=False)

    # Create file name
    iso_timestamp = FileName.get_iso_timestamp()
    file_name = FileName("weather_data").add_suffix(iso_timestamp)

    # Write to the blob container
    container_weather_raw_data.upload_blob(
        name=f"{file_name}.json",
        data=json_str
    )

    return func.HttpResponse(status_code=200)
