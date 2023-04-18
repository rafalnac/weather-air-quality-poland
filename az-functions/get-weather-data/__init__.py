import datetime
import json
import logging
import requests
import time

import azure.functions as func


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

    # Convert JSON to string.
    # Param ensure_ascii is set to false because JSON contains polish chars
    json_str = json.dumps(reqst_data, ensure_ascii=False)

    return func.HttpResponse(json_str, status_code=200)
