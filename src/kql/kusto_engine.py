import os.path
import re
from kusto_client import KustoClient
import requests

# Microsoft Tenant authority URL
authority_url = 'https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47'

def connection_str(**kargs):
    return KustoClient(cluster_url=conn.cluster_url, client_id=conn.client_id, username=conn.username, password=conn.password)

class KustoEngine(object):
    # Object constructor
    def __init__(self, conn_str, current=None):
        self.parse_connection_str(conn_str, current)
        self.authority_url = 'https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47'
        self.conn_str = conn_str
        self.name = None
        self.kusto_client = None
        self.auth_type = 'adal_username_password'
        self.cluster_url = 'https://{0}.kusto.windows.net'.format(self.cluster_name)
        self.client_id = 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'


    def __eq__(self, other):
        return self.bind_url and self.bind_url == other.bind_url

    def parse_connection_str(self, conn_str : str, current):
        self.username = None
        self.password = None
        self.cluster_name = None
        self.database_name = None
        self.bind_url = None
        match = None
        # conn_str = "kusto://username('michabin@microsoft.com').password('g=Hh-h34G').cluster('Oiildc').database('OperationInsights_PFS_PROD')"
        if not match:
            pattern = re.compile(r'^kusto://username\((?P<username>.*)\)\.password\((?P<password>.*)\)\.cluster\((?P<cluster>.*)\)\.database\((?P<database>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.username = match.group('username').strip()[1:-1]
                self.password = match.group('password').strip()[1:-1]
                self.cluster_name = match.group('cluster').strip()[1:-1]
                self.database_name = match.group('database').strip()[1:-1]

        if not match:
            pattern = re.compile(r'^kusto://cluster\((?P<cluster>.*)\)\.database\((?P<database>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.cluster_name = match.group('cluster').strip()[1:-1]
                self.database_name = match.group('database').strip()[1:-1]

        if not match:
            pattern = re.compile(r'^kusto://database\((?P<database>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.database_name = match.group('database').strip()[1:-1]

        if not match:
            pattern = re.compile(r'^kusto://cluster\((?P<cluster>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.cluster_name = match.group('cluster').strip()[1:-1]

        if not match:
            pattern = re.compile(r'^kusto://username\((?P<username>.*)\)\.password\((?P<password>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.username = match.group('username').strip()[1:-1]
                self.password = match.group('password').strip()[1:-1]

        if not match:
            pattern = re.compile(r'^kusto://username\((?P<username>.*)\)\.password\((?P<password>.*)\)\.cluster\((?P<cluster>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.username = match.group('username').strip()[1:-1]
                self.password = match.group('password').strip()[1:-1]
                self.cluster_name = match.group('cluster').strip()[1:-1]

        if not match:
            raise ConnectionError('Invalid connection string.')

        if not self.username or not self.password:
            if not current or not current.username or not current.password:
                raise ConnectionError("Username and Password are not defined.")
            self.username = current.username
            self.password = current.password

        if self.database_name and not self.cluster_name:
            if not current or not current.cluster_name:
                raise ConnectionError("Cluster is not defined.")
            self.cluster_name = current.cluster_name

        if self.database_name:
            self.bind_url = "kusto://username('{0}').password('{1}').cluster('{2}').database('{3}')".format(self.username,self.password,self.cluster_name,self.database_name)


    def set_name(self, name):
        self.name = name


    def set_kusto_client(self, kusto_client):
        self.kusto_client = kusto_client

        # "cluster_url=https://laint.kusto.windows.net;database:UID=yairip@microsoft.com;PWD=xxxx;Database=AdventureWorks;" 
        # "Data Source=https://laint.kusto.windows.net:443;Initial Catalog=NetDefaultDB;AAD Federated Security=True"


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

