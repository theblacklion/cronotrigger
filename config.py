import logging


SOURCE_PATHS = ['~']
BACKUP_PATH = '/media/backup/cronotrigger'
EXCLUDE_PATHS = ['.cache', '.gvfs', '.tmp']

LOG_LEVEL = logging.INFO


try:
    from config_local import *
except ImportError:
    raise Exception('No config_local.py found! Please provide one containing '
                    'at least the vars SOURCE_PATHS and BACKUP_PATH.')
