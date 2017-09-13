import os.path
from kusto_client import KustoClient
import requests

# Microsoft Tenant authority URL
authority_url = 'https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47'

def connection_str(**kargs):
    return KustoClient(cluster_url=conn.cluster_url, client_id=conn.client_id, username=conn.username, password=conn.password)

class KustoEngine(object):
    # Object constructor
    def __init__(self, conn_str):
        self.auth_type = 'adal_username_password'
        self.authority_url = 'https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47'
        self.conn_str = conn_str
        self.username = 'michabin@microsoft.com'
        self.password = 'f=Gg-h34F'
        self.cluster_name = 'Oiildc'
        self.database_name = 'OperationInsights_PFS_PROD'
        self.cluster_url = 'https://{0}.kusto.windows.net'.format(self.cluster_name)
        self.client_id = 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'
        self.bind_url = 'kusto://{0}:<password>@{1}/{2}'.format(self.username,self.cluster_url,self.database_name)
        self.name = None
        self.kusto_client = None

    def set_name(self, name):
        self.name = name

    def set_kusto_client(self, kusto_client):
        self.kusto_client = kusto_client

        # "cluster_url=https://laint.kusto.windows.net;database:UID=yairip@microsoft.com;PWD=xxxx;Database=AdventureWorks;" 
        # "Data Source=https://laint.kusto.windows.net:443;Initial Catalog=NetDefaultDB;AAD Federated Security=True"
        # "kusto://user:password@host/database


    def kusto_client(self, code, conn):
        return KustoClient(cluster_url=conn.cluster_url, client_id=conn.client_id, username=conn.username, password=conn.password)



    def get_access_token(self):
        # Authenticate as an application to AAD and get back
        # a token for Kusto:
        import adal
        resource_id = self._cluster_url
        context = adal.AuthenticationContext(authority_url)
        token_response=context.acquire_token_with_client_credentials(
        resource_id,
        client_id,
        client_secret)
        access_token = token_response['accessToken']
        return access_token

    def getClusterUrl(self):
        return 'https://{0}.kusto.windows.net'.format(os.getenv('CLUSTER_NAME', ''))

    def getDatabaseName(self):
        return os.getenv('DATABASE_NAME', '')

    def getAuthorityUrl(self):
        return 'https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47'

    def get_access_token(self, cluster_url, authority_url):
        # Authenticate as an application to AAD and get back
        # a token for Kusto:
        resource_id = cluster_url
        context = adal.AuthenticationContext(authority_url)
        token_response=context.acquire_token_with_client_credentials(
        resource_id,
        client_id,
        client_secret)
        access_token = token_response['accessToken']
        return access_token

class KustoEngineError(Exception):
    """Generic error class."""

