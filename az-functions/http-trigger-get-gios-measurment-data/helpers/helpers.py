import datetime

def serialize_to_iso(obj: datetime.datetime | datetime.date) -> str:
    """Serialize object to iso format string"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()