import time
import re
import logging
# to avoid "No handler found" warnings.
from kql.log  import KQLMAGIC_LOGGER_NAME
logging.getLogger(KQLMAGIC_LOGGER_NAME).addHandler(logging.NullHandler())

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

from kql.connection import Connection
from kusto_client import KustoError

from kql.runner import Runner
from kql.parser import Parser

from kql.log  import Logger, logger, set_logger, create_log_context, set_logging_options

@magics_class
class KqlMagic(Magics, Configurable):
    """Runs KQL statement on Kusto, specified by a connect string.

    Provides the %%kql magic."""

    autolimit = Int(0, config=True, allow_none=True, help="Automatically limit the size of the returned result sets")
    style = Unicode('DEFAULT', config=True, help="Set the table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)")
    short_errors = Bool(True, config=True, help="Don't display the full traceback on KQL Programming Error")
    displaylimit = Int(None, config=True, allow_none=True, help="Automatically limit the number of rows displayed (full result set is still stored)")
    autopandas = Bool(False, config=True, help="Return Pandas DataFrames instead of regular result sets")
    column_local_vars = Bool(False, config=True, help="Return data into local variables from column names")
    feedback = Bool(True, config=True, help="Print number of records returned, and assigned variables")
    show_conn_list = Bool(True, config=True, help="Print connection list, when connection not specified")
    dsn_filename = Unicode('odbc.ini', config=True, help="Path to DSN file. "
                           "When the first argument is of the form [section], "
                           "a sqlalchemy connection string is formed from the "
                           "matching section in the DSN file.")

    # [KUSTO]
    # Driver          = Easysoft ODBC-SQL Server
    # Server          = my_machine\SQLEXPRESS
    # User            = my_domain\my_user
    # Password        = my_password
    # If the database you want to connect to is the default
    # for the SQL Server login, omit this attribute
    # Database        = Northwind

    # Object constructor
    def __init__(self, shell):
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        set_logger(Logger())

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)

    @needs_local_scope
    @line_magic('kql')
    @cell_magic('kql')
    def execute(self, line, cell='', local_ns={}):
        """Runs KQL statement against Kusto, specified by a connect string.

        If no connection has been established, first word
        should be a connection string, or the user@db name
        of an established connection.

        Examples::

          %%kql kusto://username('me').password('pw').cluster('mycluster').database('mydb')
          KQL statement

          %%kql mydb@mycluster
          KQL statement

          %%kql
          KQL statement

        Connect string syntax examples:

          kusto://username('me').password('pw').cluster('mycluster').database('mydb')

        """
        set_logger(Logger(None, create_log_context()))

        # save globals and locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        logger().debug("To Parsed: \n\rline: {}\n\rcell:\n\r{}".format(line, cell))
        parsed = Parser.parse('%s\n%s' % (line, cell), self)
        logger().debug("Parsed: {}".format(parsed))
        flags = parsed['flags']
        try:
            if not parsed['connection'] and Connection.connections and self.show_conn_list:
                print(Connection.connection_list())
            Connection.set(parsed['connection'])
            conn = Connection.current
        except Exception as e:
            logger().error(str(e))
            print(e)
            print(Connection.tell_format())
            return None

        try:
            start_time = time.time()
            result = Runner.run(conn, parsed['kql'], self, user_ns)
            elapsed_time = time.time() - start_time
            saved_result = result
            if result is not None and not isinstance(result, str):
                if self.feedback:
                    print('Done ({}): {} records'.format(str(elapsed_time), result.records_count))

                logger().debug("Results: {} x {}".format(len(result), len(result.keys)))
                keys = result.keys

                if self.autopandas:
                    if self.feedback:
                        print('Returning data converted to pandas dataframe')
                    result = result.DataFrame()

                if self.column_local_vars:
                    #Instead of returning values, set variables directly in the
                    #users namespace. Variable names given by column names


                    if self.feedback:
                        print('Returning data to local variables [{}]'.format(
                            ', '.join(keys)))

                    if not self.autopandas:
                        result = result.dict()

                    self.shell.user_ns.update(result)
                    result = None
                else:

                    if flags.get('result_var'):
                        result_var = flags['result_var']
                        if self.feedback:
                            print("Returning data to local variable {}".format(result_var))
                        self.shell.user_ns.update({result_var: result})
                        result = None
            else:
                logger().debug("Results: {}".format(result))
                return result


            # Return results into the default ipython _ variable
            # if not self.autopandas:
            visulaized_chart = saved_result.visualization_chart()
            if visulaized_chart:
                return None
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

def unload_ipython_extension(ip):
    """Unoad the extension in Jupyter."""
    del ip.magics_manager.magics['cell']['kql']
    del ip.magics_manager.magics['line']['kql']
