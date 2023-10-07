import pytest
import sys
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import ContainerClient

sys.path.append(str(Path(__file__).parent.parent))
from connection import connect


@pytest.fixture
def default_credentials():
    default_credentials = DefaultAzureCredential()
    return default_credentials


@pytest.fixture
def keyvault_name():
    return "https://kv-get-measurment-data.vault.azure.net/"


def test_create_secret_client(keyvault_name, default_credentials):
    secret_client = connect.create_secret_client(keyvault_name, default_credentials)
    assert isinstance(secret_client, SecretClient)

    with pytest.raises(TypeError) as err_val:
        connect.create_secret_client(2, default_credentials)


def test_create_container_client(default_credentials):
    container_path = (
        "https://ststreamingaccdev.blob.core.windows.net/name")
    container_client = connect.create_container_client(
        container_path, default_credentials
    )
    assert isinstance(container_client, ContainerClient)
    assert container_client.container_name == "name"