try:
    import configparser
except ImportError:
    import ConfigParser as configparser
from os.path import join, exists, expanduser
import csv


class ConfigParser(configparser.ConfigParser):

    def __csv_parse_value(self, value, list=True):
        parser = csv.reader([value], delimiter=',', quotechar='"', skipinitialspace=True)
        for fields in parser:
            return fields if list else fields[0]

    def get(self, *args, **kwargs):
        kwargs['raw'] = True
        # Python 2 ConfigParser is an old school class type - cannot use super.
        value = configparser.ConfigParser.get(self, *args, **kwargs)
        return self.__csv_parse_value(value, False)

    def getlist(self, *args, **kwargs):
        kwargs['raw'] = True
        # Python 2 ConfigParser is an old school class type - cannot use super.
        value = configparser.ConfigParser.get(self, *args, **kwargs)
        return self.__csv_parse_value(value, True)


def get_config(user_config):
    config = ConfigParser()
    config.read('default.ini')

    user_config_path = config.get('user-config', 'path')
    user_filepath = expanduser(join(user_config_path, user_config))
    if exists(user_filepath):
        config.read(user_filepath)

    return config
