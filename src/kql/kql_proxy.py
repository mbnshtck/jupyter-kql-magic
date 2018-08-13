import json

class KqlRow(object):
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


class KqlRowsIter(object):
    """ Iterator over returned rows, limited by size """
    def __init__(self, table, row_num, col_num):
        self.table = table
        self.next = 0
        self.last = row_num
        self.col_num = col_num


    def __iter__(self):
        self.next = 0
        self.iter_all_iter = self.table.iter_all()
        return self


    def next(self):
        return self.__next__()


    def __next__(self):
        if self.next >= self.last:
            raise StopIteration
        else:
            self.next = self.next + 1
            return KqlRow(self.iter_all_iter.__next__(), self.col_num)


    def __len__(self):
        return self.last


class KqlResponse(object):
    # Object constructor
    def __init__(self, response):
        self.completion_query_info = None
        self.completion_query_resource_consumption = None
        self.data_table =  response.primary_results
        self.columns_count = self.data_table.columns_count
        self.visualization_properties = response.visualization_results
        self.completion_query_info = response.completion_query_info_results
        self.completion_query_resource_consumption =response.completion_query_resource_consumption_results

    def fetchall(self):
        return KqlRowsIter(self.data_table, self.data_table.rows_count, self.data_table.columns_count)


    def fetchmany(self, size):
        return KqlRowsIter(self.data_table, min(size, self.data_table.rows_count), self.data_table.columns_count)


    def rowcount(self):
        return self.data_table.rows_count

    def colcount(self):
        return self.data_table.columns_count

    def recordscount(self):
        return self.data_table.rows_count


    def keys(self):
        return self.data_table.columns_name


    def visualization_property(self, name):
        " returns value of attribute: Visualization, Title, Accumulate, IsQuerySorted, Kind, Annotation, By"
        if not self.visualization_properties:
            return None
        try:
            value = self.visualization_properties[name]
            return value if value != "" else None
        except:
            return None
    
    def _map_columns_to_index(self, columns : list):
        map = {}
        for idx, col in enumerate(columns):
            map[col['ColumnName']] = idx
        return map


    def returns_rows(self):
        return self.data_table.rows_count > 0

class FakeResultProxy(object):
    """A fake class that pretends to behave like the ResultProxy.
    """
    # Object constructor
    def __init__(self, cursor, headers):
        self.fetchall = cursor.fetchall
        self.fetchmany = cursor.fetchmany
        self.rowcount = cursor.rowcount
        self.keys = lambda: headers
        self.returns_rows = True
