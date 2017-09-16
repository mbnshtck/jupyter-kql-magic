import os
from kql.parser import Parser
from six.moves import configparser
try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable

empty_config = Configurable()
default_flags = {'result_var': None}
def test_parse_no_kql():
    assert Parser.parse("will:longliveliz@localhost/shakes", empty_config) == \
           {'connection': "will:longliveliz@localhost/shakes",
            'kql': '',
            'flags': default_flags}

query1 = "let T = view () { datatable(n:long, name:string)[1,'foo',2,'bar'] }; T"
def test_parse_with_kql():
    assert Parser.parse("will:longliveliz@localhost/shakes {}".format(query1),
                 empty_config) == \
           {'connection': "will:longliveliz@localhost/shakes",
            'kql': query1,
            'flags': default_flags}

def test_parse_kql_only():
    parsed = Parser.parse(query1, empty_config)
    print(parsed)
    assert parsed == \
           {'connection': "",
            'kql': query1,
            'flags': default_flags}

def test_parse_kusto_socket_connection():
    assert Parser.parse("kusto:///shakes {}".format(query1), empty_config) == \
           {'connection': "kusto:///shakes",
            'kql': query1,
            'flags': default_flags}

def test_expand_environment_variables_in_connection():
    os.environ['DATABASE_URL'] = 'kusto:///shakes'
    assert Parser.parse("$DATABASE_URL {}".format(query1), empty_config) == \
            {'connection': "kusto:///shakes",
            'kql': query1,
            'flags': default_flags}
