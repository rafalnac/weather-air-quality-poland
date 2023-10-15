"""
Module contains function used for ingestion and transformation.

"""

import tomli as tomlib


def load_config_file(path: str) -> dict[str, any]:
    """Load configuration file in TOML format.

    Args:
        path: Absolute path to file.

    Returns:
        Dictionary.

    Raises:
        TypeError: If path is not str type.
    """

    if not isinstance(path, str):
        TypeError("Path must be a string type.")

    with open(path, "rb") as cfg:
        cfg_file = tomlib.load(cfg)

    return cfg_file
