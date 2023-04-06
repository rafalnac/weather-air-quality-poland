"""
This module creates ODBC connection string to Azure SQL Database.
Connection string uses authentication via AAD Service Principal. 
"""

from os import environ as env

# Service principal access should be granted on the database and storage level
# https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/sql-authentication?tabs=serverless
# https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-storage-files-storage-access-control?tabs=user-identity

# Additionaly in order to use table/view which leverages OPENROWSET(BULK)
# it is needed to 'GRANT ADMINISTER DATABASE BULK OPERATIONS TO <service_principal_name>'
# in the conext of the related database.

# define connection parameters
driver = "ODBC Driver 17 for SQL Server"
server = env.get("SynapseSQLPoolUrl")
database = "project1"
sql_syn_connect_client_id = env.get("ConnectSynSqlClientId")
sql_syn_connect_client_secret = env.get("ConnectSynSqlClientSecret")

# create connection string
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
