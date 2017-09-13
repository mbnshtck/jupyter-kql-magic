import re
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.display import display_javascript
try:
    from traitlets.config.configurable import Configurable
    from traitlets import Bool, Int, Unicode
except ImportError:
    from IPython.config.configurable import Configurable # depricated since IPython 4.0
    from IPython.utils.traitlets import Bool, Int, Unicode

try:
    from pandas.core.frame import DataFrame, Series
except ImportError:
    DataFrame = None
    Series = None

from runner import Runner
from connection import Connection
from kusto_client import KustoError

from kql.parser import Parser
from kql.kqlmagic import KqlMagic


@magics_class
class TestKqlMagic(Magics, Configurable):
    """Runs KQL statement on Kusto, specified by a connect string.

    Provides the %%kql magic."""

    autolimit = Int(0, config=True, allow_none=True, help="Automatically limit the size of the returned result sets")
    style = Unicode('DEFAULT', config=True, help="Set the table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)")
    short_errors = Bool(True, config=True, help="Don't display the full traceback on KQL Programming Error")
    displaylimit = Int(None, config=True, allow_none=True, help="Automatically limit the number of rows displayed (full result set is still stored)")
    autopandas = Bool(False, config=True, help="Return Pandas DataFrames instead of regular result sets")
    column_local_vars = Bool(False, config=True, help="Return data into local variables from column names")
    feedback = Bool(True, config=True, help="Print number of rows affected by DML")
    dsn_filename = Unicode('odbc.ini', config=True, help="Path to DSN file. "
                           "When the first argument is of the form [section], "
                           "a sqlalchemy connection string is formed from the "
                           "matching section in the DSN file.")


    # Object constructor
    def __init__(self):
        pass
    def execute1(self, line, cell='', local_ns={}):
        pass

    @needs_local_scope
    def execute(self, line, cell='', local_ns={}):
        """Runs KQL statement against Kusto, specified by a connect string.

        If no connection has been established, first word
        should be a connection string, or the user@db name
        of an established connection.

        Examples::

          %%kql kusto://me:mypw@localhost/mycluster/mydb
          KQL statement

          %%kql me@mydb
          KQL statement

          %%kql
          KQL statement

        Connect string syntax examples:

          kusto://me:mypw@localhost/mycluster/mydb

        """
        # save globals and locals so they can be referenced in bind vars

        parsed = Parser.parse('%s\n%s' % (line, cell), self)
        flags = parsed['flags']
        try:
            conn = Connection.set(parsed['connection'])
        except Exception as e:
            print(e)
            print(Connection.tell_format())
            return None

        try:
            result = Runner.run(conn, parsed['kql'], self, None)

            if result is not None and not isinstance(result, str):
                keys = result.keys

                if self.autopandas:
                    result = result.DataFrame()

                if self.column_local_vars:
                    #Instead of returning values, set variables directly in the
                    #users namespace. Variable names given by column names


                    if not self.autopandas:
                        result = result.dict()

                    if self.feedback:
                        print('Returning data to local variables [{}]'.format(', '.join(keys)))
                    self.shell.user_ns.update(result)

                    return None
                else:

                    if flags.get('result_var'):
                        result_var = flags['result_var']
                        if self.feedback:
                            print("Returning data to local variable {}".format(result_var))
                        self.shell.user_ns.update({result_var: result})
                        return None

            # Return results into the default ipython _ variable
            return result

        except (KustoError) as e:
            if self.short_errors:
                print(e)
            else:
                raise


def load_ipython_extension(ip):
    """Load the extension in Jupyter."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_kql'] = {'reg':[/^%%kql/]};"
    # display_javascript(js, raw=True)
    ip.register_magics(KqlMagic)

ip = get_ipython()
kqlmagic = KqlMagic(shell=ip)
ip.register_magics(kqlmagic)
magic = TestKqlMagic();
x = magic.execute('kusto://mypw@localhost:pw/mycluster/mydb\n\revent | project timestamp, cloud_RoleName, event_Size, event_Id | limit 20')
x.csv('csvfile')
print(x)
print(y)

