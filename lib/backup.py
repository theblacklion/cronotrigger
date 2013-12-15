from os.path import exists, join
from os import (makedirs, link, rename, stat as os_stat, mknod)
import stat
import time
import logging

from lib.dtree import scan, copystat
from lib.copy import Reader, Writer, Queue, QUEUE_SIZE


class Backup(object):

    def __init_base_path(self, base_path):
        # TODO 1. generate uuid which matches this machine.
        #      2. search for backup folder matching the uuid.
        #      3. create one if necessary
        # NOTE: UUID is the blkid of the root mount point.
        if not exists(base_path):
            raise Exception('Backup path not found: %s' % base_path)

    def __init__(self, base_path):
        super(Backup, self).__init__()
        self._base_path = base_path
        self._logger = logging.getLogger('backup')
        self._reader, self._writer = None, None
        self._dirs_need_stats = []
        self._missing_files = []
        self._missing_bytes = 0
        self.__init_base_path(base_path)

    def __init_threads(self, sum_bytes):
        self._input_queue = Queue.Queue(maxsize=QUEUE_SIZE * 4)
        self._output_queue = Queue.Queue(maxsize=QUEUE_SIZE)
        self._reader = Reader(self._input_queue, self._output_queue, sum_bytes)
        self._reader.start()
        self._writer = Writer(self._output_queue, self._dirs_need_stats)
        self._writer.start()

    def create(self, sum_bytes):
        hash_ = str(time.time())
        backup_path = join(self._base_path, hash_ + '-in-progress')
        backup_path_final = join(self._base_path, hash_)
        self._logger.info('Creating backup dir: %s' % backup_path)
        makedirs(backup_path)
        self._backup_path = backup_path
        self._backup_path_final = backup_path_final
        self.__init_threads(sum_bytes)

    def __join_threads(self):
        if self._reader:
            self._reader.stop()
            self._reader.join()
        if self._writer:
            self._writer.stop()
            self._writer.join()

    def __del__(self):
        self.__join_threads()

    def create_tree(self, dirs):
        num_dirs = 0
        for src_dir, mtime, inode in dirs:
            dst_dir = src_dir.lstrip('./')
            dst_dir = join(self._backup_path, dst_dir)
            makedirs(dst_dir)
            self._dirs_need_stats.append(src_dir)
            num_dirs += 1
        self._logger.info('Created %d dirs.' % num_dirs)

    def copy_files(self, files):
        num_files, num_symlinks = 0, 0
        for src_path, name, mtime, size, is_link, is_file, inode in files:
            src_file = join(src_path, name)
            dst_file = src_file.lstrip('./')
            dst_file = join(self._backup_path, dst_file)
            self._input_queue.put(dict(
                src_dir=src_path,
                src_file=src_file,
                src_resolver=None,
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

    def link_old_files(self, files):
        # TODO replace with more low-level cached variant like in restore process.
        backup_dirs = sorted(next(scan(self._base_path))[3], key=lambda item: item.name)
        del backup_dirs[-1]  # Remove our current backup dir.
        if not backup_dirs:
            self._logger.info('No previous backup found. Skipped linking.')
            return
        prev_backup_path = join(backup_dirs[-1]._path, backup_dirs[-1].name)
        for src_path, name, mtime, size, is_link, is_file, inode in files:
            org_file = join(src_path, name)
            src_file = org_file.lstrip('./')
            src_file = join(prev_backup_path, src_file)
            dst_file = org_file.lstrip('./')
            dst_file = join(self._backup_path, dst_file)
            # print(org_file, src_file, dst_file)
            try:
                if is_link:
                    link(src_file, dst_file)
                elif not is_file:
                    # TODO the indexer should have this information in the database.
                    mode = os_stat(src_file).st_mode
                    if stat.S_ISCHR(mode):
                        self._logger.warning('Char file is not supported.')
                    elif stat.S_ISBLK(mode):
                        self._logger.warning('Block file is not supported.')
                    elif stat.S_ISFIFO(mode):
                        mknod(dst_file, stat.S_IFIFO)
                        copystat(src_file, dst_file)
                    elif stat.S_ISSOCK(mode):
                        mknod(dst_file, stat.S_IFSOCK)
                        copystat(src_file, dst_file)
                else:
                    link(src_file, dst_file)
            except FileNotFoundError as reason:  # TODO Does not work in Python < 3.3!
                # TODO also check if we can read the file and if not, throw an error message.
                # TODO perhaps mark unreadable files as such so that they won't be tried next time?
                if exists(org_file):
                    self._logger.warn('File not found in previous backup. Queued file for copying: %s' % org_file)
                    self._missing_files.append((src_path, name, mtime, size, is_link, is_file, inode))
                    self._missing_bytes += size
                else:  # This should not happen. Perhaps a race condition might trigger this.
                    self._logger.error('Linking failed: "%s" -> "%s"' % (src_file, dst_file))
                    self._logger.error(reason)
            except (OSError, IOError) as reason:
                self._logger.error('Linking failed: "%s" -> "%s"' % (src_file, dst_file))
                self._logger.error(reason)

    def get_sum_missing_bytes(self):
        return self._missing_bytes

    def copy_missing_files(self):
        self._reader.add_more_bytes(self._missing_bytes)
        self.copy_files(self._missing_files)

    def _copy_dir_stats(self, path):
        # TODO remember already seen dirs and skip them below.
        parts = path.lstrip('./').split('/')
        while parts:
            src_dir = '/'.join([''] + parts)
            # print src_dir
            dst_dir = join(self._backup_path, '/'.join(parts))
            # print dst_dir
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

    def commit(self):
        self._logger.info('Renaming finished backup dir to: %s' % self._backup_path_final)
        rename(self._backup_path, self._backup_path_final)

    def get_path(self):
        return self._backup_path

    def get_final_path(self):
        return self._backup_path_final
