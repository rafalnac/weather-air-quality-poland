"""Authenticate to the Azure services.

Authentication to azure services is based on DefaultAzureCredential.

DefaultAzureCredential:
https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python
"""

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import ContainerClient

# Initialize credentials
default_credentials = DefaultAzureCredential()

# Instanciate Secret Client
secret_client = SecretClient(
    vault_url="https://kv-get-measurment-data.vault.azure.net/",
    credential=default_credentials
)

# Instanciate Container Client
_container_url = secret_client.get_secret("blobContainerWeatherStr").value
container_weather_raw_data = ContainerClient.from_container_url(
    container_url=f"{_container_url}/raw-data/weather-data",
    credential=default_credentials
)