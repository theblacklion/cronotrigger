try:
    import configparser
except ImportError:
    import ConfigParser as configparser
from os.path import join, exists, expanduser


USER_CONFIG_DIR = '~/.config/cronotrigger'


def get_config(user_config):
    config = configparser.ConfigParser()
    config.read('default.ini')

    user_filepath = expanduser(join(USER_CONFIG_DIR, user_config))
    if exists(user_filepath):
        config.read(user_filepath)

    return config
