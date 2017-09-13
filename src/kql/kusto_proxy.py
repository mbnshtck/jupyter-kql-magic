import os.path
from kusto_client import KustoClient
import requests

class KustoRow(object):
    def __init__(self, row, col_num):
        self.row = row
        self.next = 0
        self.last = col_num

    def __iter__(self):
        self.next = 0
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.next >= self.last:
            raise StopIteration
        else:
            val = self.__getitem__(self.next)
            self.next = self.next + 1
            return val

    def __getitem__(self, key):
        return self.row[key]

    def __len__(self):
        return self.last

    def __eq__(self, other):
        if (len(other) != self.last):
            return False
        for i in range(0, self.last):
            s = self.__getitem__(i)
            o = other[i]
            if o != s:
                return False
        return True

    def __str__(self):
        return ", ".join(str(self.__getitem__(i)) for i in range(0, self.last))

class KustoRowsIter(object):
    """ Iterator over returned rows, limited by size """
    def __init__(self, response, row_num, col_num):
        self.response = response
        self.next = 0
        self.last = row_num
        self.col_num = col_num

    def __iter__(self):
        self.next = 0
        self.fetchall_iter = self.response.fetchall()
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.next >= self.last:
            raise StopIteration
        else:
            self.next = self.next + 1
            return KustoRow(self.fetchall_iter.__next__(), self.col_num)

    def __len__(self):
        return self.last

class KustoResponse(object):
    # Object constructor
    def __init__(self, response):
        self.response = response
        self.row_count = len(self.response.get_raw_response()['Tables'][0]['Rows'])
        self.col_count = len(self.response.get_raw_response()['Tables'][0]['Columns'])

    def fetchall(self):
        return KustoRowsIter(self.response, self.row_count, self.col_count)

    def fetchmany(self, size):
        return KustoRowsIter(self.response, min(size, self.row_count), self.col_count)

    def rowcount(self):
        return 0

    def keys(self):
        result = []
        for value in self.response.get_raw_response()['Tables'][0]['Columns']:
            result.append(value['ColumnName'])
        return result

    def returns_rows(self):
        return self.row_count > 0

class KustoProxy(object):

    # Object constructor
    def __init__(self, conn):
        self.conn = conn


    def execute(self, code, user_namespace):
        self.headers = None
        self.rows = None
        self.code = code
        return self.__kusto_client_execute(code, self.conn)


    def __kusto_client_execute(self, code, conn):
        if code.strip():
            if not conn.kusto_client:
                kusto_client = KustoClient(kusto_cluster=conn.cluster_url, client_id=conn.client_id, username=conn.username, password=conn.password)
                conn.set_kusto_client(kusto_client)

            response = conn.kusto_client.execute(conn.database_name, code, False)
            # response = conn.kusto_client.execute(kusto_database=conn.database_name, query=code, accept_partial_results= False)
            return KustoResponse(response)


    __kusto_python_client_version__ = '0.4.0'

    def __rest_execute(self, code, conn):

        # Create a Kusto request and send it
        kusto_request_headers = {
            'Authorization': 'Bearer {0}'.format(access_token),
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip,deflate',
            'Fed': 'True',
            'x-ms-client-version':'Kusto.Python.Client:' + __kusto_python_client_version__,
        }

        kusto_request_payload = {
            'db': conn.database_name,
            'csl': code
        }
        
        kusto_query_endpoint = '{0}/v1/rest/query'.format(conn.cluster_url)
        
        response = requests.post(kusto_query_endpoint, headers=kusto_request_headers, json=kusto_request_payload)

        # Make sure that the response is success
        response.raise_for_status()

        # TODO deserialize response.json() to headers and rows
        self.headers = []
        self.rows = []


class FakeResultProxy(object):
    """A fake class that pretends to behave like the ResultProxy from
    SqlAlchemy.
    """
    # Object constructor
    def __init__(self, cursor, headers):
        self.fetchall = cursor.fetchall
        self.fetchmany = cursor.fetchmany
        self.rowcount = cursor.rowcount
        self.keys = lambda: headers
        self.returns_rows = True
