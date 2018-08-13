from datetime import timedelta, datetime
import re
import json
import adal 
import dateutil.parser
import requests
from azure.kusto.data import KustoClient
from azure.kusto.data._response import WellKnownDataSet
from kql.my_aad_helper import _MyAadHelper


# Regex for TimeSpan
TIMESPAN_PATTERN = re.compile(r'((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2})(.(?P<ms>[0-9]*))?')

__version__ = '0.1.0'

class KustoResult(dict):
    """ Simple wrapper around dictionary, to enable both index and key access to rows in result """
    def __init__(self, index2column_mapping, *args, **kwargs):
        super(KustoResult, self).__init__(*args, **kwargs)
        # TODO: this is not optimal, if client will not access all fields.
        # In that case, we are having unnecessary perf hit to convert Timestamp, even if client don't use it.
        # In this case, it would be better for KustoResult to extend list class. In this case,
        # KustoResultIter.index2column_mapping should be reversed, e.g. column2index_mapping.
        self.index2column_mapping = index2column_mapping

    def __getitem__(self, key):
        if isinstance(key, int):
            val = dict.__getitem__(self, self.index2column_mapping[key])
        else:
            val = dict.__getitem__(self, key)
        return val


class KustoResponseTable(object):
    """ Iterator over returned rows """
    def __init__(self, json_response_table):
        self.json_response_table = json_response_table
        self.index2column_mapping = []
        self.index2type_mapping = []
        for c in json_response_table['Columns']:
            self.index2column_mapping.append(c['ColumnName'])
            ctype = c["ColumnType"] if "ColumnType" in c else c["DataType"]
            self.index2type_mapping.append(ctype)
        self.next = 0
        self.last = len(json_response_table['Rows'])
        # Here we keep converter functions for each type that we need to take special care (e.g. convert)
        self.converters_lambda_mappings = {
            "datetime": self.to_datetime,
            "timespan": self.to_timedelta,
            'DateTime': self.to_datetime, 
            'TimeSpan': self.to_timedelta}

    @staticmethod
    def to_datetime(value):
        if value is None:
            return None
        return dateutil.parser.parse(value)

    @staticmethod
    def to_timedelta(value):
        if value is None:
            return None
        m = TIMESPAN_PATTERN.match(value)
        if m:
            return timedelta(
                days=int(m.group('d') or 0),
                hours=int(m.group('h')),
                minutes=int(m.group('m')),
                seconds=int(m.group('s')),
                milliseconds=int(m.group('ms') or 0))
        else:
            raise ValueError('Timespan value \'{}\' cannot be decoded'.format(value))

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.next >= self.last:
            raise StopIteration
        else:
            row = self.json_response_table['Rows'][self.next]
            result_dict = {}
            for index, value in enumerate(row):
                data_type = self.index2type_mapping[index]
                if data_type in self.converters_lambda_mappings:
                    result_dict[self.index2column_mapping[index]] = self.converters_lambda_mappings[data_type](value)
                else:
                    result_dict[self.index2column_mapping[index]] = value
            self.next = self.next + 1
            return KustoResult(self.index2column_mapping, result_dict)

    @property
    def columns_name(self):
        return self.index2column_mapping


    @property
    def rows_count(self):
        return len(self.json_response_table['Rows'])

    @property
    def columns_count(self):
        return len(self.json_response_table['Columns'])

    def fetchall(self):
        """ Returns iterator to get rows from response """
        # TODO: we called this fethall to resemble Python DB API,
        # but this can be as easily called result or similar
        return self.__iter__()

    def iter_all(self):
        """ Returns iterator to get rows from response """
        # TODO: we called this fethall to resemble Python DB API,
        # but this can be as easily called result or similar
        return self.__iter__()

class KustoResponse(object):
    """ Wrapper for response """
    # TODO: add support to get additional infromation from response, like execution time

    def __init__(self, json_response, endpoint_version):
        self.json_response = json_response
        self.endpoint_version = endpoint_version
        if self.endpoint_version == 'v2':
            self.all_tables = [t for t in json_response if t["FrameType"] == "DataTable"]
            self.tables = [t for t in json_response if t["FrameType"] == "DataTable" and t["TableKind"] == "PrimaryResult"]
        else:
            self.all_tables = self.json_response['Tables']
            self.tables = self.json_response['Tables']
        self.primary_results = KustoResponseTable(self.tables[0])

    @property
    def visualization_results(self):
        if self.endpoint_version == 'v2':
            for table in self.all_tables:
                if table['TableName'] == '@ExtendedProperties':
                    for row in table['Rows']:
                        if row[1] == 'Visualization':
                            # print('visualization_properties: {}'.format(row[2]))
                            return json.loads(row[2])
        else:
            tables_num = self.json_response['Tables'].__len__()
            last_table = self.json_response['Tables'][tables_num - 1]
            for row in last_table['Rows']:
                if row[2] == "@ExtendedProperties":
                    table = self.json_response['Tables'][row[0]]
                    # print('visualization_properties: {}'.format(table['Rows'][0][0]))
                    return json.loads(table['Rows'][0][0])
        return {}

    @property
    def completion_query_info_results(self):
        if self.endpoint_version == 'v2':
            for table in self.all_tables:
                if table['TableName'] == 'QueryCompletionInformation':
                    cols_idx_map = self._map_columns_to_index(table['Columns'])
                    event_type_name_idx =cols_idx_map.get('EventTypeName')
                    payload_idx =cols_idx_map.get('Payload')
                    if event_type_name_idx is not None and payload_idx is not None:
                        for row in table['Rows']:
                            if row[event_type_name_idx] == 'QueryInfo':
                                return json.loads(row[payload_idx])
        else:
            pass
            # todo: implement it
        return {}


    @property
    def completion_query_resource_consumption_results(self):
        if self.endpoint_version == 'v2':
            for table in self.all_tables:
                if table['TableName'] == 'QueryCompletionInformation':
                    cols_idx_map = self._map_columns_to_index(table['Columns'])
                    event_type_name_idx =cols_idx_map.get('EventTypeName')
                    payload_idx =cols_idx_map.get('Payload')
                    if event_type_name_idx is not None and payload_idx is not None:
                        for row in table['Rows']:
                            if row[event_type_name_idx] == 'QueryResourceConsumption':
                                return json.loads(row[payload_idx])
        else:
            pass
            # todo: implement it
        return {}

    def _map_columns_to_index(self, columns : list):
        map = {}
        for idx, col in enumerate(columns):
            map[col['ColumnName']] = idx
        return map

    def get_raw_response(self):
        return self.json_response

    def get_table_count(self):
        return len(self.tables)
        

    def has_exceptions(self):
        return 'Exceptions' in self.json_response

    def get_exceptions(self):
        return self.json_response['Exceptions']

# used in Kqlmagic
class KustoError(Exception):
    """
    Represents error returned from server. Error can contain partial results of the executed query.
    """
    def __init__(self, messages, http_response, appinsights_response = None):
        super(KustoError, self).__init__(messages)
        self.http_response = http_response
        self.appinsights_response = appinsights_response

    def get_raw_http_response(self):
        return self.http_response

    def is_semantic_error(self):
        return self.http_response.text.startswith("Semantic error:")

    def has_partial_results(self):
        return self.appinsights_response is not None

    def get_partial_results(self):
        return self.appinsights_response

class KustoResponseDataSet(object):
    def __init__(self, dataset_response):
        self.dataset_response = dataset_response
        self.primary_results = self.get_primary_results() 

    def get_primary_results(self):
        primary_results = self.dataset_response.primary_results
        if isinstance(primary_results, list):
            primary_results = primary_results[0]
        return primary_results


    @property
    def rows_count(self):
        len(self.primary_results)

    @property
    def columns_count(self):
        # if self.rows_count > 0:
        #     return len(self.primary_results[0])
        return self.primary_results.columns_count

    @property
    def visualization_results(self):
        try:
            table = self.dataset_response['@ExtendedProperties']
            for row in table:
                if row[1] == 'Visualization':
                    return json.loads(r[2])
        except:
            pass

        return {}

    @property
    def completion_query_info_results(self):
        try:
            table = self.dataset_response['QueryCompletionInformation']
            for row in table:
                if row['EventTypeName'] == 'QueryInfo':
                    return json.loads(row['Payload'])
        except:
            pass
        return {}

    @property
    def completion_query_resource_consumption_results(self):
        try:
            table = self.dataset_response['QueryCompletionInformation']
            for row in table:
                if row['EventTypeName'] == 'QueryResourceConsumption':
                    return json.loads(row['Payload'])
        except:
            pass
        return {}

    def get_raw_response(self):
        # todo: implement
        return {}

    def fetchall(self):
        """ Returns iterator to get rows from response """
        # TODO: we called this fethall to resemble Python DB API,
        # but this can be as easily called result or similar
        return self.__iter__()

    def iter_all(self):
        """ Returns iterator to get rows from response """
        # TODO: we called this fethall to resemble Python DB API,
        # but this can be as easily called result or similar
        return self.__iter__()

class Kusto_Client(object):
    """
    Kusto client wrapper for Python.

    KustoClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    KustoClient takes care of ADAL authentication, parsing response and giving you typed result set,
    and offers familiar Python DB API.

    Test are run using nose.

    Examples
    --------
    To use KustoClient, you can choose betwen two ways of authentication.
     
    For the first option, you'll need to have your own AAD application and know your client credentials (client_id and client_secret).
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = KustoClient(kusto_cluster, client_id, client_secret='your_app_secret')

    For the second option, you can use KustoClient's client id and authenticate using your username and password.
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> client_id = 'e07cf1fb-c6a6-4668-b21a-f74731afa19a'
    >>> kusto_client = KustoClient(kusto_cluster, client_id, username='your_username', password='your_password')

    After connecting, use the kusto_client instance to execute a management command or a query: 
    >>> kusto_database = 'Samples'
    >>> response = kusto_client.execute_query(kusto_database, 'StormEvents | take 10')
    You can access rows now by index or by key.
    >>> for row in response.iter_all():
    >>>    print(row[0])
    >>>    print(row["ColumnName"])    """

    def __init__(
        self,
        kusto_cluster,
        client_id=None,
        client_secret=None,
        username=None,
        password=None,
        authority=None,
    ):
        """
        Kusto Client constructor.

        Parameters
        ----------
        kusto_cluster : str
            Kusto cluster endpoint. Example: https://help.kusto.windows.net
        client_id : str
            The AAD application ID of the application making the request to Kusto
        client_secret : str
            The AAD application key of the application making the request to Kusto.
            if this is given, then username/password should not be.
        username : str
            The username of the user making the request to Kusto.
            if this is given, then password must follow and the client_secret should not be given.
        password : str
            The password matching the username of the user making the request to Kusto
        authority : 'microsoft.com', optional
            In case your tenant is not microsoft please use this param.
        """

        self.client = KustoClient(kusto_cluster, client_id, client_secret, username, password, authority)

        # replace aadhelper to use remote browser in interactive mode
        my_aad_helper = _MyAadHelper(kusto_cluster=kusto_cluster, 
                                    client_id=client_id, 
                                    client_secret=client_secret, 
                                    username=username, 
                                    password=password,
                                    authority=authority)
        self.client._aad_helper = my_aad_helper
        self.mgmt_endpoint_version = 'v2' if self.client._mgmt_endpoint.endswith("v2/rest/query") else 'v1'
        self.query_endpoint_version = 'v2' if self.client._query_endpoint.endswith("v2/rest/query") else 'v1'

    def execute(
        self,
        kusto_database,
        query,
        accept_partial_results=False,
        timeout=None,
        get_raw_response=False,
    ):
        """ Execute a simple query or management command

        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        query : str
            Query to be executed
        accept_partial_results : bool
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        timeout : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        get_raw_response : bool, optional
            Optional parameter. Whether to get a raw response, or a parsed one.
        """
        endpoint_version = self.mgmt_endpoint_version if query.startswith(".") else self.query_endpoint_version
        response = self.client.execute(kusto_database, query, accept_partial_results, timeout, get_raw_response)
        return KustoResponse(response, endpoint_version) if get_raw_response else  KustoResponseDataSet(response)

    def execute_query(
        self,
        kusto_database,
        query,
        accept_partial_results=False,
        timeout=None,
        get_raw_response=False,
    ):
        """ Execute a simple query

        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        kusto_query : str
            Query to be executed
        query_endpoint : str
            The query's endpoint
        accept_partial_results : bool
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        timeout : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        get_raw_response : bool, optional
            Optional parameter. Whether to get a raw response, or a parsed one.
        """
        response = self.client.execute_query(kusto_database, query, accept_partial_results, timeout, get_raw_response,)
        return KustoResponse(response, self.query_endpoint_version) if get_raw_response else  KustoResponseDataSet(response)

    def execute_mgmt(
        self,
        kusto_database,
        query,
        accept_partial_results=False,
        timeout=None,
        get_raw_response=False,
    ):
        """ Execute a management command

        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        kusto_query : str
            Query to be executed
        query_endpoint : str
            The query's endpoint
        accept_partial_results : bool
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        timeout : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        get_raw_response : bool, optional
            Optional parameter. Whether to get a raw response, or a parsed one.
        """
        response = self.client.execute_mgmt(kusto_database, query, accept_partial_results, timeout, get_raw_response,)
        
        return KustoResponse(response, self.mgmt_endpoint_version) if get_raw_response else  KustoResponseDataSet(response)


