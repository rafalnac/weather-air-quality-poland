"""Module contains connection configurations.

It creates ODBC connection string to Azure SQL Database. Authentication type
has been set via service principal.

Authentication to required azure services is based on DefaultAzureCredential.

DefaultAzureCredential:
https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python
"""

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import ContainerClient


# Initialize the Credentials
default_credentials = DefaultAzureCredential()

# Create a secret client
secret_client = SecretClient(
    vault_url="https://kv-get-measurment-data.vault.azure.net/",
    credential=default_credentials,
)

# Get container URL string
blob_container_conn_string = secret_client.get_secret("blobContainerWeatherStr").value

# Create container client
container_weather_dir_raw_data = ContainerClient.from_container_url(
    container_url=f"{blob_container_conn_string}/raw-data/measure_points",
    credential=default_credentials,
)

# Create OBBC SQL Server connection string

# Service principal access should be granted on the database and storage level
# https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/sql-authentication?tabs=serverless
# https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-storage-files-storage-access-control?tabs=user-identity

# Additionaly in order to use table/view which leverages OPENROWSET(BULK)
# it is needed to 'GRANT ADMINISTER DATABASE BULK OPERATIONS TO <service_principal_name>'
# in the conext of the related database.

# Define connection parameters
driver = "ODBC Driver 17 for SQL Server"
server = secret_client.get_secret("SynapseSQLPoolUrl").value
database = "project1"
sql_syn_connect_client_id = secret_client.get_secret("ConnectSynSqlClientId").value
sql_syn_connect_client_secret = secret_client.get_secret(
    "ConnectSynSqlClientSecret"
).value

# Create connection string
# ODBC connection string for AAD interated authentication
# https://learn.microsoft.com/en-us/sql/connect/odbc/using-azure-active-directory?view=azure-sqldw-latest
syn_sql_pool_conn_string = f"""
    Driver={driver};
    Server={server};
    Database={database};
    Uid={sql_syn_connect_client_id};
    Pwd={sql_syn_connect_client_secret};
    Authentication=ActiveDirectoryServicePrincipal;
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;"""
