import os.path
import re
from kusto_client import KustoClient
import requests

class KustoEngine(object):

    @classmethod
    def tell_format(cls):
        return """
               kusto://username('username').password('password').cluster('clustername').database('databasename')
               kusto://username('username').password('password').cluster('clustername')
               kusto://username('username').password('password')
               kusto://cluster('clustername').database('databasename')
               kusto://cluster('clustername')
               kusto://database('databasename')"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        self.parse_connection_str(conn_str, current)
        self.schema = 'kusto'
        self.authority_url = 'https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47'
        self.client_id = 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'
        self.name = None
        self.client = None
        self.cluster_url = 'https://{0}.kusto.windows.net'.format(self.cluster_name)
        self.conn_str = conn_str


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
                self.auth_type = 'adal_username_password'
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
                self.auth_type = 'adal_username_password'
                self.username = match.group('username').strip()[1:-1]
                self.password = match.group('password').strip()[1:-1]

        if not match:
            pattern = re.compile(r'^kusto://username\((?P<username>.*)\)\.password\((?P<password>.*)\)\.cluster\((?P<cluster>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.auth_type = 'adal_username_password'
                self.username = match.group('username').strip()[1:-1]
                self.password = match.group('password').strip()[1:-1]
                self.cluster_name = match.group('cluster').strip()[1:-1]

        if not match:
            raise KustoEngineError('Invalid connection string.')

        if not self.username or not self.password:
            if not current or not current.username or not current.password:
                raise KustoEngineError("Username and Password are not defined.")
            self.auth_type = 'adal_username_password'
            self.username = current.username
            self.password = current.password

        if self.database_name and not self.cluster_name:
            if not current or not current.cluster_name:
                raise KustoEngineError("Cluster is not defined.")
            self.cluster_name = current.cluster_name

        if self.database_name:
            self.bind_url = "kusto://username('{0}').password('{1}').cluster('{2}').database('{3}')".format(self.username,self.password,self.cluster_name,self.database_name)


    def set_name(self, name):
        self.name = name


    def get_client(self):
        if not self.client:
            if not self.cluster_url:
                raise KustoEngineError("Cluster is not defined.")
            if not self.username or not self.password:
                raise KustoEngineError("Username and Password are not defined.")
            self.client = KustoClient(kusto_cluster=self.cluster_url, client_id=self.client_id, username=self.username, password=self.password)

        if not self.database_name:
            raise KustoEngineError("Database is not defined.")

        return self.client

        # "cluster_url=https://laint.kusto.windows.net;database:UID=yairip@microsoft.com;PWD=xxxx;Database=AdventureWorks;" 
        # "Data Source=https://laint.kusto.windows.net:443;Initial Catalog=NetDefaultDB;AAD Federated Security=True"


    def get_access_token(self):
        # Authenticate as an application to AAD and get back
        # a token for Kusto:
        get_access_token(self._cluster_url, self.authority_url)


    def get_access_token(self, cluster_url, authority_url):
        # Authenticate as an application to AAD and get back
        # a token for Kusto:
        resource_id = cluster_url
        context = adal.AuthenticationContext(authority_url)
        token_response=context.acquire_token_with_client_credentials(
        resource_id,
        self.client_id,
        self.client_secret)
        access_token = token_response['accessToken']
        return access_token

class KustoEngineError(Exception):
    """Generic error class."""

