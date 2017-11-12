import os
from kql.kusto_proxy import KustoProxy
from kql.kusto_engine import KustoEngine
from kql.ai_engine import AppinsightsEngine

class ConnectionError(Exception):
    pass


class Connection(object):
    current = None
    connections = {}

    @classmethod
    def tell_format(cls, str1 = None, str2 = None):
        return """Connection info needed in KQL Magic format, example:{0}{1}
               or an existing connection: {2}
                   """.format( str1, str2, str(Connection.connection_list()))

    # Object constructor
    def __init__(self, connect_str=None):
        if connect_str.startswith('kusto://'):
            engine = KustoEngine
        elif connect_str.startswith('appinsights://'):
            engine = AppinsightsEngine
        else:
            print(Connection.tell_format(KustoEngine.tell_format(), AppinsightsEngine.tell_format()))
            raise ConnectionError('invalid connection_str, unknown schema. valid schemas are: "kusto://" and "appinsights://"')
        try:
            conn = engine(connect_str, Connection.current)
        except: # TODO: bare except; but what's an ArgumentError?
            print(Connection.tell_format(engine.tell_format()))
            raise
        Connection.current = conn
        if conn.bind_url:
            if self.connections.get(conn.bind_url):
                Connection.current = self.connections[conn.bind_url]
            else:
                name = self.assign_name(conn)
                conn.set_name(name)
                self.connections[name] = conn
                self.connections[conn.bind_url] = conn

    @classmethod
    def set(cls, descriptor):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                cls.current = cls.connections.get(descriptor) or Connection(descriptor).current
        else:
            if cls.connections:
                pass
            else:
                if os.getenv('CONNECTION_STR'):
                    cls.current = Connection(os.getenv('CONNECTION_STR')).current
                else:
                    raise ConnectionError('Environment variable $CONNECTION_STR not set, and no connect string given.')
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        "Assign a unique name for the connection"

        core_name = '%s@%s' % (engine.database_name or 'unknown', engine.cluster_name or 'unknown')
        incrementer = 1
        name = core_name
        while name in cls.connections:
            name = '%s_%d' % (core_name, incrementer)
            incrementer += 1
        return name

    @classmethod
    def connection_list(cls):
        return [k for k in sorted(cls.connections) if cls.connections[k].bind_url != k]


    @classmethod
    def connection_list_formatted(cls):
        result = []
        for key in Connection.connection_list():
            if cls.connections[key] == cls.current:
                template = ' * {}'
            else:
                template = '   {}'
            result.append(template.format(key))
        return '\n'.join(result)
