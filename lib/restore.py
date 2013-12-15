from os.path import exists, lexists, join, dirname, sep
from os import makedirs, listdir
import time
import logging
import re
from collections import OrderedDict

from lib.dtree import copystat
from lib.copy import Reader, Writer, Queue, QUEUE_SIZE


class Restore(object):

    def __init_base_path(self, base_path):
        # TODO 1. generate uuid which matches this machine.
        #      2. search for backup folder matching the uuid.
        # NOTE: UUID is the blkid of the root mount point.
        if not exists(base_path):
            raise Exception('Backup path not found: %s' % base_path)

    def __init_backup_paths(self):
        pattern = re.compile(r'^\d+\.\d+$')
        timestamps = listdir(self._base_path)
        timestamps = list(filter(lambda item: pattern.match(item), timestamps))
        timestamps.sort(key=lambda v: float(v))
        self._backup_paths = OrderedDict(map(lambda timestamp: (timestamp, join(self._base_path, timestamp)), timestamps))

    def __init__(self, base_path, restore_path):
        super(Restore, self).__init__()
        self._base_path = base_path
        self._restore_path = restore_path
        self._logger = logging.getLogger('restore')
        self._reader, self._writer = None, None
        self._dirs_need_stats = []
        self._backup_path = None
        self._backup_paths = None
        self.__init_base_path(base_path)
        self.__init_backup_paths()

    def __init_threads(self, sum_bytes):
        self._input_queue = Queue.Queue(maxsize=QUEUE_SIZE * 4)
        self._output_queue = Queue.Queue(maxsize=QUEUE_SIZE)
        self._reader = Reader(self._input_queue, self._output_queue, sum_bytes)
        self._reader.start()
        self._writer = Writer(self._output_queue, self._dirs_need_stats)
        self._writer.start()

    def select(self, timestamp):
        backup_path = join(self._base_path, timestamp)
        self._logger.info('Selecting backup dir: %s' % backup_path)
        if not exists(backup_path):
            raise Exception('Specified backup not found: %s' % timestamp)
        self._backup_path = backup_path

    def __join_threads(self):
        if self._reader:
            self._reader.stop()
            self._reader.join()
        if self._writer:
            self._writer.stop()
            self._writer.join()

    def __del__(self):
        self.__join_threads()

    def get_path(self):
        return self._backup_path

    def set_bytes(self, sum_bytes):
        # Not optimal. Perhaps we could find a better call style?!
        self.__init_threads(sum_bytes)

    def create_tree(self, dirs):
        num_dirs = 0
        for src_dir, mtime, inode in dirs:
            dst_dir = src_dir.lstrip('./')
            dst_dir = join(self._restore_path, dst_dir)
            try:
                makedirs(dst_dir)
            except OSError as error:
                if error.errno != 17:
                    raise
            self._dirs_need_stats.append(src_dir)
            num_dirs += 1
        self._logger.info('Created %d dirs.' % num_dirs)

    def copy_files(self, files):
        base_path = self._base_path
        backup_paths = self._backup_paths
        keys = list(backup_paths.keys())
        _logger = self._logger

        def _find_older_file(cur_timestamp, filepath):
            index = keys.index(cur_timestamp)
            for timestamp in reversed(keys[:index]):
                test_filepath = join(backup_paths[timestamp], filepath)
                if lexists(test_filepath):
                    _logger.debug('Grabbed from older backup: %s' % timestamp)
                    return test_filepath
            raise Exception('No copy found: %s' % filepath)

        def src_resolver(filepath):
            # print('test for:' + filepath)
            if not lexists(filepath):
                filepath = filepath[len(base_path):].lstrip('/')
                cur_timestamp, filepath = filepath.split(sep, 1)
                return _find_older_file(cur_timestamp, filepath)
            return filepath

        # src_resolver = self._src_resolver
        num_files, num_symlinks = 0, 0
        for dst_path, name, mtime, size, is_link, is_file, inode in files:
            dst_file = join(dst_path, name)
            src_file = dst_file.lstrip('./')
            src_file = join(self._backup_path, src_file)
            dst_file = join(self._restore_path, dst_file.lstrip('/'))
            # print(src_file, exists(src_file))
            # print(dst_file, exists(dst_file))
            self._input_queue.put(dict(
                src_dir=dirname(src_file),
                src_file=src_file,
                src_resolver=src_resolver,
                dst_file=dst_file,
                size=size,
                is_link=is_link,
                is_file=is_file,
            ))
        try:
            while not (self._reader._is_idle and self._writer._is_idle and
                       self._input_queue.empty() and
                       self._output_queue.empty() and
                       self._reader.is_alive() and self._writer.is_alive()):
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        self._logger.info('Copied %d files and %d symlinks.' %
                          (self._writer._num_files,
                           self._writer._num_symlinks))

    def _copy_dir_stats(self, path):
        # TODO remember already seen dirs and skip them below.
        base_path = self._base_path
        cur_timestamp = self._backup_path[len(base_path):].lstrip('./')
        backup_paths = self._backup_paths
        keys = list(backup_paths.keys())
        _logger = self._logger

        def _find_older_dir(path):
            index = keys.index(cur_timestamp)
            for timestamp in reversed(keys[:index]):
                test_filepath = join(backup_paths[timestamp], path)
                if lexists(test_filepath):
                    _logger.debug('Grabbed from older backup: %s' % timestamp)
                    return test_filepath
            raise Exception('No copy found: %s' % path)

        parts = path.lstrip('./').split('/')
        while parts:
            src_dir = join(self._backup_path, '/'.join(parts))
            if not lexists(src_dir):
                src_dir = _find_older_dir('/'.join(parts))
            # print(src_dir)
            dst_dir = join(self._restore_path, '/'.join(parts))
            # print(dst_dir)
            try:
                copystat(src_dir, dst_dir, follow_symlinks=False)
            except KeyboardInterrupt:
                raise
            except Exception as reason:
                self._logger.exception(reason)
            del parts[-1]

    def copy_dir_stats(self):
        # TODO should be gathered from the database.
        for path in self._dirs_need_stats:
            self._copy_dir_stats(path)
