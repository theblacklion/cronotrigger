#!/usr/bin/env python

import logging
from os.path import join, expanduser
from time import time

from lib.dtree import scan
from lib.index import Index
from lib.backup import Backup
from lib.human_size import human_size

from config import LOG_LEVEL, SOURCE_PATHS, BACKUP_PATH


# Support ~ and ~user constructions.
BACKUP_PATH = expanduser(BACKUP_PATH)
SOURCE_PATHS = map(expanduser, SOURCE_PATHS)


def main():
    start = time()

    # TODO How can we block the OS from going to sleep due to being idle?

    logging.getLogger().setLevel(LOG_LEVEL)

    logging.info('Preparing backup.')

    backup = Backup(BACKUP_PATH)

    # TODO Implement regex excludes (i.e. .gvfs, .tmp, /home/.+/.cache, etc.)
    index = Index(join(BACKUP_PATH, 'index.sqlite3'))
    for path in SOURCE_PATHS:
        logging.info('Scanning directory tree: %s' % path)
        index.update(scan(path))

    dirs_found, files_found = index.get_cur_stats()
    logging.info('Found %d dirs and %d files.' % (dirs_found, files_found))

    bytes = index.get_added_bytes() + index.get_modified_bytes()
    logging.info('%s to copy.' % human_size(bytes))

    # Only create new backup if files or dirs have changed or been added.
    if index.get_num_added_or_modified_dirs_or_files():
        backup.create(sum_bytes=bytes)

        logging.info('Backing up tree structure.')
        # backup.create_tree(index.get_all_dirs())
        backup.create_tree(index.get_added_or_modified_dirs())

        # TODO Collect errors also in extra log file.
        # TODO Try to add some nice sleeps not to hug the cpu and io too much.
        # TODO Try to collect 1MB chunks even with small files etc.
        logging.info('Backing up files.')
        backup.copy_files(index.get_added_or_modified_files())

        # logging.info('Linking unmodified files.')
        # backup.link_old_files(index.get_unmodified_files())

        missing_bytes = backup.get_sum_missing_bytes()
        if missing_bytes:
            logging.info('Backing up missing files.')
            logging.info('%s to copy.' % human_size(missing_bytes))
            backup.copy_missing_files()

        # TODO Test with only adding dirs. Perhaps we have to reverse the order?
        logging.info('Backing up dir stats.')
        backup.copy_dir_stats()

        backup.commit()

        logging.info('Updating database.')
        index.commit()

    secs = time() - start
    logging.info('Backup finished after %.2f secs.' % secs)


if __name__ == '__main__':
    main()
