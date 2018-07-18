import os.path
import re
from kql.la_client import LoganalyticsClient
import requests
import getpass

class LoganalyticsEngine(object):
    schema = 'loganalytics://'

    @classmethod
    def tell_format(cls):
        return """
               loganalytics://workspace('workspaceid').appkey('appkey')

               ## Note: if appkey is missing, user will be prompted to enter appkey"""

    # Object constructor
    def __init__(self, conn_str, current=None):
        self.api_version = 'v1'
        self.name = None
        self.bind_url = None
        self.client = None
        self.cluster_url = 'https://api.applicationinsights.io/{0}/apps'.format(self.api_version)
        self.parse_connection_str(conn_str, current)


    def __eq__(self, other):
        return self.bind_url and self.bind_url == other.bind_url

    def parse_connection_str(self, conn_str : str, current):
        self.username = None
        self.password = None
        self.workspace = None
        self.appkey = None
        self.bind_url = None
        self.name = None
        match = None
        # conn_str = "kusto://username('michabin@microsoft.com').password('g=Hh-h34G').cluster('Oiildc').database('OperationInsights_PFS_PROD')"
        if not match:
            pattern = re.compile(r'^loganalytics://workspace\((?P<workspace>.*)\)\.appkey\((?P<appkey>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.workspace = match.group('workspace').strip()[1:-1]
                self.appkey = match.group('appkey').strip()[1:-1]
                self.cluster_name = 'loganalytics'
                self.database_name = self.workspace
                if self.appkey.lower() == '<appkey>':
                    self.appkey = getpass.getpass(prompt = 'please enter appkey: ')

        if not match:
            pattern = re.compile(r'^loganalytics://workspace\((?P<workspace>.*)\)$')
            match = pattern.search(conn_str)
            if match:
                self.workspace = match.group('workspace').strip()[1:-1]
                self.cluster_name = 'loganalytics'
                self.database_name = self.workspace
                self.appkey = getpass.getpass(prompt = 'please enter appkey: ')

        if not match:
            raise LoganalyticsEngineError('Invalid connection string.')

        if self.database_name:
            self.bind_url = "loganalytics://workspace('{0}').appkey('{1}').cluster('{2}').database('{3}')".format(self.workspace,self.appkey,self.cluster_name,self.database_name)
            self.name = '{0}@loganalytics'.format(self.workspace)


    def get_client(self):
        if not self.client:
            if not self.workspace or not self.appkey:
                raise LoganalyticsEngineError("workspace and appkey are not defined.")
            self.client = LoganalyticsClient(workspace=self.workspace, appkey=self.appkey, version=self.api_version)
        return self.client

    def get_database(self):
        database_name = self.database_name
        if not self.database_name:
            raise LoganalyticsEngineError("Database is not defined.")
        return database_name


class LoganalyticsEngineError(Exception):
    """Generic error class."""

