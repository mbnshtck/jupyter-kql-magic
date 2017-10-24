import os
from kql.kusto_proxy import KustoProxy
from kql.kusto_engine import KustoEngine

class ConnectionError(Exception):
    pass


class Connection(object):
    current = None
    connections = {}

    @classmethod
    def tell_format(cls):
        return """Connection info needed in KQL Magic format, example:
               kusto://username('username').password('password').cluster('clustername').database('databasename')
               kusto://username('username').password('password').cluster('clustername')
               kusto://username('username').password('password')
               kusto://cluster('clustername').database('databasename')
               kusto://cluster('clustername')
               kusto://database('databasename')
               or an existing connection: %s""" % str(cls.connections.keys())

    # Object constructor
    def __init__(self, connect_str=None):
        try:
            engine = KustoEngine(connect_str, Connection.current)
        except: # TODO: bare except; but what's an ArgumentError?
            print(Connection.tell_format())
            raise
        Connection.current = engine
        if engine.bind_url:
            if self.connections.get(engine.bind_url):
                Connection.current = self.connections[engine.bind_url]
            else:
                name = self.assign_name(engine)
                engine.set_name(name)
                self.connections[name] = engine
                self.connections[engine.bind_url] = engine

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
                print(cls.connection_list())
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
        result = []
        for key in sorted(cls.connections):
            if cls.connections[key].bind_url != key:
                if cls.connections[key] == cls.current:
                    template = ' * {}'
                else:
                    template = '   {}'
                result.append(template.format(key))
        return '\n'.join(result)
