from os.path import expandvars
import six
from six.moves import configparser as CP
from kql.log  import Logger, logger


class Parser(object):

    @staticmethod
    def parse(cell, config):
        """Separate input into (connection info, KQL statements, flags)"""

        parts = [part.strip() for part in cell.split(None, 1)]
        print(parts)
        if not parts:
            return {'connection': '', 'kql': '', 'flags': {}}

        #
        # replace substring of the form $name or ${name} in windows also %name% if found in env variabes
        #
        parts[0] = expandvars(parts[0])  # for environment variables

        #
        # connection taken from a section in config file (file name have to be define in config.dsn_filename
        #
        if parts[0].startswith('[') and parts[0].endswith(']'):
            section = parts[0].lstrip('[').rstrip(']')
            parser = CP.ConfigParser()
            parser.read(config.dsn_filename)
            cfg_dict = dict(parser.items(section))

            connection = str(URL(**cfg_dict))
            code = parts[1] if len(parts) > 1 else ''
        #
        # connection taken from first line
        #
        elif '@' in parts[0] or '://' in parts[0]:
            connection = parts[0]
            code = parts[1] if len(parts) > 1 else ''
        #
        # connection not specified
        #
        else:
            connection = ''
            code = cell

        #
        # connection not specified
        #
        kql, flags = Parser.parse_kql_flags(code.strip())

        return {'connection': connection.strip(),
                'kql': kql,
                'flags': flags}


    @staticmethod
    def parse_kql_flags(code):
        words = code.split()
        flags = {
            'result_var': None
        }
        if not words:
            return ('', flags)
        num_words = len(words)
        trimmed_kql = code
        if num_words >= 2 and words[1] == '<<':
            flags['result_var'] = words[0]
            trimmed_kql = ' '.join(words[2:])
        return (trimmed_kql.strip(), flags)
