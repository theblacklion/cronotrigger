#!/usr/bin/env python

import logging
from os.path import join, expanduser
from time import time
import gzip
import sys
import csv

from lib.config import get_config
from lib.dtree import scan
from lib.index import Index
from lib.backup import Backup
from lib.human_size import human_size


def _config_parse_value(value, list=True):
    parser = csv.reader([value], delimiter=',', quotechar='"', skipinitialspace=True)
    for fields in parser:
        return fields if list else fields[0]


def main():
    start = time()

    # Determine profile to use.
    try:
        profile = sys.argv[1]
    except IndexError:
        profile = 'default'

    # Load and extract our config.
    config = get_config('%s.ini' % profile)
    SOURCE_PATHS = _config_parse_value(config.get('source', 'paths'), True)
    BACKUP_PATH = _config_parse_value(config.get('destination', 'path'), False)
    LOG_LEVEL = _config_parse_value(config.get('logging', 'level'), False)
    LOG_FORMAT = _config_parse_value(config.get('logging', 'format'), False)

    # Support ~ and ~user constructions.
    SOURCE_PATHS = map(expanduser, SOURCE_PATHS)
    BACKUP_PATH = expanduser(BACKUP_PATH)

    # TODO How can we block the OS from going to sleep due to being idle?

    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    logger = logging.getLogger('process')

    try:
        backup = Backup(BACKUP_PATH)
    except Exception as reason:
        logger.error(reason)
        logger.error('Perhaps you forgot to mount your backup medium first?')
        sys.exit(1)

    logger.info('Preparing backup.')

    db_path = join(BACKUP_PATH, 'index.sqlite3')
    index = Index(db_path)
    for path in SOURCE_PATHS:
        logger.info('Scanning directory tree: %s' % path)
        # TODO Implement regex excludes (i.e. .gvfs, .tmp, /home/.+/.cache, etc.)
        index.update(scan(path))

    dirs_found, files_found = index.get_cur_stats()
    logger.info('Found %d dirs and %d files.' % (dirs_found, files_found))

    bytes = index.get_added_bytes() + index.get_modified_bytes()
    logger.info('%s to copy.' % human_size(bytes))

    # Only create new backup if files or dirs have changed or been added.
    if index.get_num_added_or_modified_dirs_or_files():
        backup.create(sum_bytes=bytes)

        logger.info('Backing up tree structure.')
        # backup.create_tree(index.get_all_dirs())
        backup.create_tree(index.get_added_or_modified_dirs())

        # TODO Collect errors also in extra log file.
        # TODO Try to add some nice sleeps not to hug the cpu and io too much.
        # TODO Try to collect 1MB chunks even with small files etc.
        logger.info('Backing up files.')
        backup.copy_files(index.get_added_or_modified_files())

        # logger.info('Linking unmodified files.')
        # backup.link_old_files(index.get_unmodified_files())

        missing_bytes = backup.get_sum_missing_bytes()
        if missing_bytes:
            logger.info('Backing up missing files.')
            logger.info('%s to copy.' % human_size(missing_bytes))
            backup.copy_missing_files()

        # TODO Test with only adding dirs. Perhaps we have to reverse the order?
        logger.info('Backing up dir stats.')
        backup.copy_dir_stats()

        logger.info('Updating database.')
        index.commit()

        # Disconnect from index database.
        del index

        logger.info('Backing up database.')
        db_backup_path = join(backup.get_path(), 'index.sqlite3.gz')
        f_in = open(db_path, 'rb')
        f_out = gzip.open(db_backup_path, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()

        # Rename backup directory and finalize backup.
        backup.commit()

    secs = time() - start
    logger.info('Backup finished after %.2f secs.' % secs)


if __name__ == '__main__':
    main()
