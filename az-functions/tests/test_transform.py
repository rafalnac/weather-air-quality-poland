"""
Unit tests for transform module.
"""

import sys
import pytest

from pathlib import Path

# Append root az-funcions
sys.path.append(str(Path(__file__).parent.parent))

from utils.transfom import FileName


@pytest.fixture
def constructor():
    return FileName("name_of_the_file")


def test_constructor(constructor):
    assert isinstance(constructor, FileName)
    assert constructor.name == "name_of_the_file"


def test_properties(constructor):
    constructor.name = "New Name"
    assert constructor.name == "New Name"
    with pytest.raises(TypeError) as err_info:
        constructor.name = 5


@pytest.mark.parametrize("param1, param2", [("1", 2), (1, "2"), (1, 2)])
def test_add_suffix(constructor, param1, param2):
    assert constructor.add_suffix("a", "_") == "name_of_the_file_a"
    with pytest.raises(TypeError) as err_info:
        constructor.add_suffix(param1, param2)


def test_iso_timestamp(constructor):
    # Check if timestamp does not have ":"
    assert ":" not in constructor.get_iso_timestamp()
    # Check if timestamp is in UTC
    assert constructor.get_iso_timestamp()[-5:] == "00_00"
