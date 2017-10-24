import os.path
from kql.kusto_proxy import KustoProxy
from kql.results import ResultSet

class Runner(object):

    @staticmethod
    def log(val):
        with open('KqlMagic.log', 'a') as f:
            f.write(str(val) + '\n')
        return val


    @staticmethod
    def run(conn, code, config, user_namespace = None):
        if code.strip():

            #
            # split string to queries
            #

            queries = []
            queryLines = []
            for line in code.splitlines(True):
                if line.isspace():
                    if len(queryLines) > 0:
                        queries.append(''.join(queryLines))
                        queryLines = []
                else:
                    queryLines.append(line)

            if len(queryLines) > 0:
                queries.append(''.join(queryLines))

            #
            # execute the queries sequentialy
            #

            for query in queries:
                kusto_proxy = KustoProxy(conn)
                result = kusto_proxy.execute(query, user_namespace)

            #
            # returning only last result, intentionally
            #

            return ResultSet(result, query, config)
        
        #
        # No kustoString, just return the current connection
        #

        else:
            return 'Connected: %s' % conn.name

