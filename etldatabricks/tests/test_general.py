"""
Test module general.py.
"""

import sys

import pytest
import tomli as tomlib
from pathlib import Path


# Add root directory to the path
sys.path.append(str(Path(__file__).parent.parent))
from functions.general import load_config_file


@pytest.fixture
def config_file_path() -> str:
    cfg_path = str(Path(__file__).parent.parent / "config.toml")
    return cfg_path

@pytest.fixture
def config_file(config_file_path):
    with open(config_file_path, "rb") as cfg:
        cfg_file = tomlib.load(cfg)
    return cfg_file


def test_load_config_file(config_file_path, config_file):
    cfg_file = load_config_file(config_file_path)

    assert isinstance(cfg_file, dict)
    assert cfg_file == config_file
