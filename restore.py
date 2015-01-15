#!/usr/bin/env python

import logging
from os.path import join, exists
from time import time
import gzip
import sys
from gi.repository import Gio

from lib.config import get_config
# from lib.dtree import scan
from lib.index import Index
from lib.restore import Restore
from lib.human_size import human_size
from lib.util import expandvars
from lib import volume


def main():
    start = time()

    # Determine profile to use.
    profile, timestamp, restore_path = sys.argv[1:4]
    source_paths = sys.argv[4:]
    # print(profile, timestamp, restore_path, source_paths)

    # Load and extract our config.
    config = get_config('%s.ini' % profile)
    BACKUP_PATH = config.get('destination', 'path')
    LOG_LEVEL = config.get('logging', 'level')
    LOG_FORMAT = config.get('logging', 'format')
    DISABLE_TIMEOUTS = config.get('power-management', 'disable_sleep_timeouts')

    # Support ~, ~user and other constructions.
    BACKUP_PATH = expandvars(BACKUP_PATH)
    restore_path = expandvars(restore_path)
    source_paths = map(expandvars, source_paths)

    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    logger = logging.getLogger('process')

    if not exists(restore_path):
        raise Exception('Path to restore to does not exist: %s' % restore_path)

    BACKUP_PATH_REAL = BACKUP_PATH
    mounted_volume = None
    try:
        if BACKUP_PATH.startswith('volume://'):
            mounted_volume, BACKUP_PATH_REAL = volume.mount(BACKUP_PATH)
        restore = Restore(BACKUP_PATH_REAL, restore_path)
    except Exception as reason:
        logger.error(reason)
        # logger.error('Perhaps you forgot to mount your backup medium first?')
        sys.exit(1)

    logger.info('Preparing restoration.')

    # Backup and disable sleep timeout settings.
    if DISABLE_TIMEOUTS:
        logging.info('Disabling system sleep mode timeouts.')
        power_settings = Gio.Settings('org.gnome.settings-daemon.plugins.power')
        ac_timeout = power_settings.get_int('sleep-inactive-ac-timeout')
        bat_timeout = power_settings.get_int('sleep-inactive-battery-timeout')
        power_settings.set_int('sleep-inactive-ac-timeout', 0)
        power_settings.set_int('sleep-inactive-battery-timeout', 0)

    try:
        restore.select(timestamp)

        db_path = join(restore_path, 'index.sqlite3')

        logger.info('Restoring database.')
        db_backup_path = join(restore.get_path(), 'index.sqlite3.gz')
        f_in = gzip.open(db_backup_path, 'rb')
        f_out = open(db_path, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()

        index = Index(db_path)
        for path in source_paths:
            logger.info('Selecting backup directory tree: %s' % path)
            index.select(path)

        dirs_found, files_found = index.get_cur_stats()
        logger.info('Selected %d dirs and %d files.' % (dirs_found, files_found))

        bytes = index.get_selected_bytes()
        logger.info('%s to copy.' % human_size(bytes))

        restore.set_bytes(bytes)

        logger.info('Restoring tree structure.')
        restore.create_tree(index.get_selected_dirs())

        # TODO Collect errors also in extra log file.
        # TODO Try to add some nice sleeps not to hug the cpu and io too much.
        # TODO Try to collect 1MB chunks even with small files etc.
        logger.info('Restoring files.')
        restore.copy_files(index.get_selected_files())

        logger.info('Restoring dir stats.')
        restore.copy_dir_stats()
    finally:
        # Restore sleep timeout settings.
        if DISABLE_TIMEOUTS:
            logger.info('Restoring system sleep mode timeouts.')
            power_settings.set_int('sleep-inactive-ac-timeout', ac_timeout)
            power_settings.set_int('sleep-inactive-battery-timeout', bat_timeout)

        if mounted_volume:
            volume.umount(mounted_volume)

    secs = time() - start
    logger.info('Restoration finished after %.2f secs.' % secs)


if __name__ == '__main__':
    main()
