import logging
import json
import datetime
from os import environ as env

import pyodbc
import azure.functions as func


def serialize_to_iso(obj: datetime.datetime | datetime.date) -> str:
    """Serialize object to iso format string"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
