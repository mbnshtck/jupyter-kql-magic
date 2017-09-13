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
               kusto://username:password@cluster_url/dbname
               or an existing connection: %s""" % str(cls.connections.keys())

    # Object constructor
    def __init__(self, connect_str=None):
        try:
            engine = KustoEngine(connect_str)
        except: # TODO: bare except; but what's an ArgumentError?
            print(Connection.tell_format())
            raise
        name = self.assign_name(engine)
        engine.set_name(name)
        bind_url = str(engine.bind_url)
        self.connections[name] = engine
        self.connections[bind_url] = engine
        Connection.current = engine

    @classmethod
    def set(cls, descriptor):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                existing = cls.connections.get(descriptor) or \
                           cls.connections.get(descriptor.lower())
            cls.current = existing or Connection(descriptor).current
        else:
            if cls.connections:
                print(cls.connection_list())
            else:
                if os.getenv('CONNECTION_STR'):
                    cls.current = Connection(os.getenv('CONNECTION_STR'))
                else:
                    raise ConnectionError('Environment variable $CONNECTION_STR not set, and no connect string given.')
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        core_name = '%s@%s' % (engine.database_name, engine.cluster_name)
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
            if cls.connections[key] == cls.current:
                template = ' * {}'
            else:
                template = '   {}'
            result.append(template.format(key))
        return '\n'.join(result)
