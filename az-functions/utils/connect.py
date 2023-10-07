from azure.keyvault.secrets import SecretClient
from azure.storage.blob import ContainerClient
from azure.core.credentials import TokenCredential


def create_secret_client(kv_name: str, credential: TokenCredential) -> SecretClient:
    """Create secret client to access values in keyvault

    Args:
        kv_name: Key valut name
        credential: Object which can provide access token for key vault
            As an example DefaultCredential() -> azure.identity.DefaultAzureCredential
            https://learn.microsoft.com/en-us/python/api/azure-identity/
            azure.identity.defaultazurecredential?view=azure-python

    Returns:
        Instance of SecretClient object.

    Raises:
        TypeError: IF kv_name is not string type
    """

    if not isinstance(kv_name, str):
        raise TypeError("Name of the keyvalut must be a string.")

    secret_client = SecretClient(vault_url=kv_name, credential=credential)
    return secret_client


def create_container_client(
    container_url: str, credential: TokenCredential
) -> ContainerClient:
    """Create container client from provided url.

    Function assume that authentication is not based on SAS token.

    Args:
        container_url: Full container url.
            https://<storage_acc>.blob.core.windows.net/<container_name>
        credential: Object which can provide access token for key vault
            As an example DefaultCredential() -> azure.identity.DefaultAzureCredential
            https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python

    Returns:
        Instance of ContainerClient object.
    """

    container_client = ContainerClient.from_container_url(
        container_url=container_url, credential=credential
    )

    return container_client
