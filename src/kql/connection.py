import os
from kql.kql_engine import KqlEngineError
from kql.kusto_engine import KustoEngine
from kql.ai_engine import AppinsightsEngine
from kql.la_engine import LoganalyticsEngine
from kql.display import Display


class ConnectionError(Exception):
    pass


class Connection(object):
    current = None
    connections = {}
    last_current_by_engine = {}

    @classmethod
    def _tell_format(cls, engine1=None, engine2=None, engine3=None):
        str1 = engine1.tell_format() if engine1 else ""
        str2 = engine2.tell_format() if engine2 else ""
        str3 = engine2.tell_format() if engine3 else ""
        lst1 = Connection.get_connection_list_by_schema(engine1.schema) if engine1 else []
        lst2 = Connection.get_connection_list_by_schema(engine2.schema) if engine2 else []
        lst3 = Connection.get_connection_list_by_schema(engine3.schema) if engine3 else []
        msg = """kql magic format requires connection info, examples:{0}{1}{2}
               or an existing connection: {3}
                   """.format(
            str1, str2, str3, str(lst1 + lst2 + lst3)
        )
        return msg

    @classmethod
    def tell_format(cls, connect_str=None):
        if connect_str.startswith("kusto://"):
            return Connection.tell_format(KustoEngine)
        elif connect_str.startswith("appinsights://"):
            return Connection.tell_format(AppinsightsEngine)
        elif connect_str.startswith("loganalytics://"):
            return Connection.tell_format(LoganalyticsEngine)
        else:
            return Connection.tell_format(KustoEngine, AppinsightsEngine, LoganalyticsEngine)

    # Object constructor
    def __init__(self, connect_str=None):
        if connect_str.startswith("appinsights://"):
            engine = AppinsightsEngine
        elif connect_str.startswith("loganalytics://"):
            engine = LoganalyticsEngine
        elif connect_str.startswith("kusto://"):
            engine = KustoEngine
        # wasn't found in connection list, but maybe a kusto database connection
        elif "@" in connect_str:
            engine = KustoEngine
        else:
            raise KqlEngineError('invalid connection_str, unknown schema. valid schemas are: "kusto://", "appinsights://" and "loganalytics://"')

        last_current = self.last_current_by_engine.get(engine.__name__)

        if engine in (AppinsightsEngine, LoganalyticsEngine):
            conn_engine = engine(connect_str, last_current)
        else:
            if connect_str.startswith("kusto://"):
                if last_current:
                    last_cluster_name = last_current.get_cluster()
                    last_current = cls.connections.get("@" + last_cluster_name)
                cluster_conn_engine = engine(connect_str, last_current)
                cluster_name = cluster_conn_engine.get_cluster()
                Connection._set_current(cluster_conn_engine, conn_name="@" + cluster_name)
                conn_name = cluster_conn_engine.get_conn_name()
            else:
                conn_name = connect_str
            conn_engine = Connection._get_kusto_database_engine(conn_name)

        Connection._set_current(conn_engine)

    @classmethod
    def _get_kusto_database_engine(cls, conn_name):
        parts = conn_name.split("@")
        if len(parts) != 2:
            raise KqlEngineError(
                'invalid connection_str, valid connection_str patternes are: "kusto://...", "appinsights://...", "loganalytics://..." and "database@cluster"'
            )
        if parts[1] in ["appinsights", "loganalytics"]:
            raise KqlEngineError(
                'invalid connection_str, connection_str pattern "database@cluster" cannot be used for "appinsights" and "loganalytics"'
            )
        cluster_conn_name = "@" + parts[1]
        cluster_conn = cls.connections.get(cluster_conn_name)
        if cluster_conn is None:
            raise KqlEngineError(
                'invalid connection_str, connection_str pattern "database@cluster" can be used only after a connection_str pattern "kusto://..." to the same cluster was previously defined'
            )
        return KustoEngine(parts, conn_class=Connection)

    @classmethod
    def _set_current(cls, conn_engine, conn_name=None):
        # already exist
        if cls.connections.get(conn_engine.bind_url):
            Connection.current = cls.connections[conn_engine.bind_url]
        # new connection
        else:
            Connection.current = conn_engine
            name = conn_name or conn_engine.get_conn_name()
            cls.connections[name] = conn_engine
            cls.connections[conn_engine.bind_url] = conn_engine

    @classmethod
    def get_connection_by_name(cls, name):
        return cls.connections.get(name)

    @classmethod
    def get_connection(cls, descriptor):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                # either exist or create a new one
                cls.current = cls.connections.get(descriptor) or Connection(descriptor).current
        elif not cls.current:
            raise ConnectionError("No current connection set yet.")
        cls.last_current_by_engine[cls.current.__class__.__name__] = cls.current
        return cls.current

    @classmethod
    def connection_list(cls):
        return [k for k in sorted(cls.connections) if not k.startswith("@") and cls.connections[k].bind_url != k]

    @classmethod
    def get_connection_list_by_schema(cls, schema):
        return [k for k in Connection.connection_list() if cls.connections[k].bind_url.startswith(schema)]

    @classmethod
    def get_connection_list_formatted(cls):
        result = []
        for key in Connection.connection_list():
            if cls.connections[key] == cls.current:
                template = " * {}"
            else:
                template = "   {}"
            result.append(template.format(key))
        return result

    @classmethod
    def get_current_connection_formatted(cls):
        return " * " + cls.current.get_conn_name()
