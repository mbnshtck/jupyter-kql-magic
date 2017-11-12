from os.path import expandvars
import six
from six.moves import configparser as CP
from kql.log  import Logger, logger


class Parser(object):

    @staticmethod
    def parse(cell, config):
        """Separate input into (connection info, KQL statements, flags)"""

        parts = [part.strip() for part in cell.split(None, 1)]
        # print(parts)
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
            cfg_dict_lower = dict()
            # for k,v in cfg_dict:
            #     cfg_dict_lower[k.lower()] = v
            cfg_dict_lower = {k.lower(): v for (k,v) in cfg_dict.items()}
            if cfg_dict_lower.get('appid') or cfg_dict_lower.get('appkey'):
                connection_list = []
                for key in ['appid','appkey']:
                    if cfg_dict_lower.get(key):
                        connection_list.append(str.format("{0}('{1}')", key, cfg_dict_lower.get(key)))
                connection = 'appinsights://' + '.'.join(connection_list)
            else:
                if cfg_dict_lower.get('user'):
                    cfg_dict_lower['username'] = cfg_dict_lower.get('user')
                connection_list = []
                for key in ['username','password','cluster','database']:
                    if cfg_dict_lower.get(key):
                        connection_list.append(str.format("{0}('{1}')", key, cfg_dict_lower.get(key)))
                connection = 'kusto://' + '.'.join(connection_list)
            print (connection)

            code = parts[1] if len(parts) > 1 else ''
        #
        # connection taken from first line, new full connection
        #
        elif parts[0].startswith('kusto://'):
            connection = parts[0]
            code = parts[1] if len(parts) > 1 else ''
        elif parts[0].startswith('appinsights://'):
            connection = parts[0]
            code = parts[1] if len(parts) > 1 else ''
        #
        # connection taken from first line, established connection
        #
        elif '@' in parts[0]:
            connection = parts[0]
            code = parts[1] if len(parts) > 1 else ''
        #
        # connection not specified
        #
        else:
            connection = ''
            code = cell

        #
        # parse code to kql and flags
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


