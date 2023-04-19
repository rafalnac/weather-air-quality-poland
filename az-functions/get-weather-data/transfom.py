"""Module provides class to perform required transformations."""

import datetime


class FileName:
    """Craetes file name object."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        class_name = type(self).__name__
        return f"{class_name}(name={self.name!r})"

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if not isinstance(value, str):
            raise TypeError("Name must be a string.")
        self._name = value

    def add_suffix(self, suffix: str, sep: str = "_") -> str:
        """Cretes string with file_name and suffix separated by sep."""

        return sep.join([self.name, str(suffix)])

    @staticmethod
    def get_iso_timestamp() -> str:
        """Function creates UTC timestamp in iso format.

        Function replaces ':'(colon) to '_'(underscore) in timestamp.
        Example:
            2023-04-12T09:54:02.661677+00:00 -> 2023-04-12T09_54_02.661677+00_00
        """

        # Create UTC timestamp in iso format.
        timestamp_utc = datetime.datetime.isoformat(
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        )
        # Replace ':' to '-' in timestamp
        timestamp_utc_formated = timestamp_utc.replace(":", "_")
        return timestamp_utc_formated


if __name__ == "__main__":
    file_name = FileName("6")
    current_timestamp = FileName.get_iso_timestamp()
    print(file_name)
    print(file_name.add_suffix(current_timestamp))
