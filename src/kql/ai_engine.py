import os.path
import re
from kql.ai_client import AppinsightsClient
import requests

class AppinsightsEngine(object):

    @classmethod
    def tell_format(cls):
        return """
                  appinsights://appid('appid').appkey('appkey')"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        self.parse_connection_str(conn_str, current)
        self.api_version = 'v1'
        self.schema = 'appinsights'
        self.authority_url = 'https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47'
        self.name = None
        self.client = None
        self.cluster_url = 'https://api.applicationinsights.io/{0}/apps'.format(self.api_version)
        self.conn_str = conn_str


    def __eq__(self, other):
        return self.bind_url and self.bind_url == other.bind_url

    def parse_connection_str(self, conn_str : str, current):
        self.username = None
        self.password = None
        self.appid = None
        self.appkey = None
        self.bind_url = None
        match = None
        # conn_str = "kusto://username('michabin@microsoft.com').password('g=Hh-h34G').cluster('Oiildc').database('OperationInsights_PFS_PROD')"
        if not match:
            pattern = re.compile(r'^appinsights://appid\((?P<appid>.*)\)\.appkey\((?P<appkey>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.auth_type = 'api_key_authentication'
                self.appid = match.group('appid').strip()[1:-1]
                self.appkey = match.group('appkey').strip()[1:-1]
                self.cluster_name = 'appinsights'
                self.database_name = self.appid

        if not match:
            raise AppinsightsEngineError('Invalid connection string.')

        if self.database_name:
            self.bind_url = "appinsights://appid('{0}').appkey('{1}').cluster('{2}').database('{3}')".format(self.appid,self.appkey,self.cluster_name,self.database_name)


    def set_name(self, name):
        self.name = name


    def get_client(self):
        if not self.client:
            if not self.appid or not self.appkey:
                raise AppinsightsEngineError("appid and appkey are not defined.")
            self.client = AppinsightsClient(appid=self.appid, appkey=self.appkey, version=self.api_version)

        if not self.database_name:
            raise AppinsightsEngineError("Database is not defined.")

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

class AppinsightsEngineError(Exception):
    """Generic error class."""

