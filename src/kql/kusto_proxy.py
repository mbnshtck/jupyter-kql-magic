import os.path
import json
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
        for i in range(self.last):
            s = self.__getitem__(i)
            o = other[i]
            if o != s:
                return False
        return True


    def __str__(self):
        return ", ".join(str(self.__getitem__(i)) for i in range(self.last))


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


class KqlResponse(object):
    # Object constructor
    def __init__(self, response):
        self.data_records_index = 0
        self.extended_properties_index = None
        self.response = response
        self.row_count = len(self.response.get_raw_response()['Tables'][self.data_records_index]['Rows'])
        self.col_count = len(self.response.get_raw_response()['Tables'][self.data_records_index]['Columns'])


    def fetchall(self):
        return KustoRowsIter(self.response, self.row_count, self.col_count)


    def fetchmany(self, size):
        return KustoRowsIter(self.response, min(size, self.row_count), self.col_count)


    def rowcount(self):
        return self.row_count

    def colcount(self):
        return self.col_count

    def recordscount(self):
        return self.row_count


    def keys(self):
        result = []
        for value in self.response.get_raw_response()['Tables'][self.data_records_index]['Columns']:
            result.append(value['ColumnName'])
        return result



    def extended_properties(self, name):
        " returns value of attribute: Visualization, Title, Accumulate, IsQuerySorted, Kind, Annotation, By"
        self.get_extended_properties_index()
        if not self.extended_properties_index:
            return None
        attrib_str = self.response.get_raw_response()['Tables'][self.extended_properties_index]['Rows'][0][0]
        # print('extended_properties: {}'.format(attrib_str))
        json_obj = json.loads(attrib_str)
        try:
            value = json_obj[name]
            return value if value != "" else None
        except:
            return None


    def get_extended_properties_index(self):
        " returns the index to the table that contains the extended properties"
        if not self.extended_properties_index:
            table_num = self.response.get_raw_response()['Tables'].__len__()
            for r in self.response.get_raw_response()['Tables'][table_num - 1]['Rows']:
                if r[2] == "@ExtendedProperties":
                    self.extended_properties_index = r[0]


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
        if code.strip():
            client = self.conn.get_client()

            response = client.execute(self.conn.database_name, code, False)
            return KqlResponse(response)


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
