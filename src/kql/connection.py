import os
from kql.kql_proxy import KqlProxy
from kql.kusto_engine import KustoEngine
from kql.ai_engine import AppinsightsEngine
from kql.la_engine import LoganalyticsEngine
from kql.display import Display

class ConnectionError(Exception):
    pass


class Connection(object):
    current = None
    connections = {}

    @classmethod
    def tell_format(cls, engine1 = None, engine2 = None, engine3 = None):
        str1 = engine1.tell_format() if engine1 else ''
        str2 = engine2.tell_format() if engine2 else ''
        str3 = engine2.tell_format() if engine3 else ''
        lst1 = Connection.connection_schema_list(engine1.schema) if engine1 else []
        lst2 = Connection.connection_schema_list(engine2.schema) if engine2 else []
        lst3 = Connection.connection_schema_list(engine3.schema) if engine3 else []
        msg = """kql magic format requires connection info, examples:{0}{1}{2}
               or an existing connection: {3}
                   """.format( str1, str2, str3, str(lst1 + lst2 + lst3))
        return msg

    # Object constructor
    def __init__(self, connect_str=None):
        engine  = None
        try:
            if connect_str.startswith('kusto://'):
                engine = KustoEngine
            elif connect_str.startswith('appinsights://'):
                engine = AppinsightsEngine
            elif connect_str.startswith('loganalytics://'):
                engine = LoganalyticsEngine
            else:
                raise ConnectionError('invalid connection_str, unknown schema. valid schemas are: "kusto://", "appinsights://" and "loganalytics://"')
            conn = engine(connect_str, Connection.current)
        except Exception as e: # TODO: bare except; but what's an ArgumentError?
            if engine:
                msg = Connection.tell_format(engine)
            else:
                msg = Connection.tell_format(KustoEngine, AppinsightsEngine, LoganalyticsEngine)
            Display.showDangerMessage(str(e))
            Display.showInfoMessage(msg)

        Connection.current = conn
        if conn.bind_url:
            if self.connections.get(conn.bind_url):
                Connection.current = self.connections[conn.bind_url]
            else:
                name = self.assign_name(conn)
                # rename the name according the asiigned name
                conn.name = name
                self.connections[name] = conn
                self.connections[conn.bind_url] = conn

    @classmethod
    def get_connection(cls, descriptor):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                # either exist or create a new one
                cls.current = cls.connections.get(descriptor) or Connection(descriptor).current
        else:
            if not cls.current:
                if not os.getenv('CONNECTION_STR'):
                    raise ConnectionError('Environment variable $CONNECTION_STR not set, and no connect string given.')
                cls.current = Connection(os.getenv('CONNECTION_STR')).current
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        "Assign a unique name for the connection"

        incrementer = 1
        name = core_name = engine.name
        while name in cls.connections:
            name = '{0}_{1}'.format(core_name, incrementer)
            incrementer += 1
        return name

    @classmethod
    def connection_list(cls):
        return [k for k in sorted(cls.connections) if cls.connections[k].bind_url != k]


    @classmethod
    def connection_schema_list(cls, schema):
        return [k for k in sorted(cls.connections) if cls.connections[k].bind_url != k and cls.connections[k].bind_url.startswith(schema)]


    @classmethod
    def connection_list_formatted(cls):
        result = []
        for key in Connection.connection_list():
            if cls.connections[key] == cls.current:
                template = ' * {}'
            else:
                template = '   {}'
            result.append(template.format(key))
        return result
