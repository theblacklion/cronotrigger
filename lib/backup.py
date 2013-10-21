from os.path import exists, join, basename, dirname
from os import (makedirs, readlink, symlink, link, rename, stat as os_stat,
                mknod)
import stat
import time
import logging
from threading import Thread
try:
    import Queue  # Python 2
except ImportError:
    import queue as Queue  # Python 3

from lib.human_size import human_size
from lib.dtree import scan, copystat


# Define output chunk and queue size. E.g. 1 MB * 100 = 100 MB
CHUNK_SIZE = 1024 * 1024 * 5  # Bytes
QUEUE_SIZE = 25  # Length


class Reader(Thread):

    def __init__(self, input_queue, output_queue, sum_bytes):
        super(Reader, self).__init__()
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._sum_bytes = sum_bytes
        self._running = True
        self._is_idle = True
        self._logger = logging.getLogger('backup.reader')

    def add_more_bytes(self, count):
        self._sum_bytes += count

    def run(self):
        self._logger.debug('Started thread.')
        sum_bytes = self._sum_bytes
        sum_bytes_transferred = 0
        while self._running:
            try:
                item = self._input_queue.get(timeout=0.1)
                src_dir = item['src_dir']
                src_file = item['src_file']
                dst_file = item['dst_file']
                size = item['size']
                is_link = item['is_link']
                is_file = item['is_file']

                type_ = 'file'
                if is_link:
                    type_ = 'symlink'
                elif not is_file:
                    type_ = 'special'
                self._logger.debug('%s|%s' % (type_, src_file))
                self._is_idle = False

                try:
                    if is_link:
                        self._output_queue.put(dict(
                            type='symlink',
                            src_dir=src_dir,
                            dst_file=dst_file,
                            data=readlink(src_file),
                            status=None,
                        ))
                        self._output_queue.put(dict(
                            type='meta',
                            src_dir=src_dir,
                            dst_file=dst_file,
                            data=src_file,
                            status=None,
                        ))
                    elif not is_file:
                        type_ = None
                        mode = os_stat(src_file).st_mode
                        if stat.S_ISCHR(mode):
                            type_ = 'char file'
                        elif stat.S_ISBLK(mode):
                            type_ = 'block file'
                        elif stat.S_ISFIFO(mode):
                            type_ = 'fifo'
                        elif stat.S_ISSOCK(mode):
                            type_ = 'socket/pipe'
                        self._output_queue.put(dict(
                            type='special',
                            src_dir=src_dir,
                            dst_file=dst_file,
                            data=type_,
                            status=type_,
                        ))
                        self._output_queue.put(dict(
                            type='meta',
                            src_dir=src_dir,
                            dst_file=dst_file,
                            data=src_file,
                            status=None,
                        ))
                    else:
                        with open(src_file, 'rb') as handle:
                            chunk = handle.read(CHUNK_SIZE)  # read chunk
                            bytes_transferred = 0
                            if len(chunk) == 0:
                                percent = 100.0
                                hsize = human_size(size)
                                sum_percent = ((100.0 / sum_bytes *
                                                sum_bytes_transferred)
                                               if sum_bytes else 0)
                                sum_hsize = human_size(sum_bytes)
                                self._output_queue.put(dict(
                                    type='file',
                                    src_dir=src_dir,
                                    dst_file=dst_file,
                                    data=chunk,
                                    status='file %.2f%% of %s; '
                                           'global %.2f%% of %s' %
                                           (percent, hsize, sum_percent,
                                            sum_hsize),
                                ))
                            else:
                                while len(chunk) and self._running:
                                    bytes_transferred += len(chunk)
                                    percent = 100.0 / size * bytes_transferred
                                    hsize = human_size(size)
                                    sum_bytes_transferred += len(chunk)
                                    sum_percent = ((100.0 / sum_bytes *
                                                    sum_bytes_transferred)
                                                   if sum_bytes else 0)
                                    sum_hsize = human_size(sum_bytes)
                                    self._output_queue.put(dict(
                                        type='file',
                                        src_dir=src_dir,
                                        dst_file=dst_file,
                                        data=chunk,
                                        status='file %.2f%% of %s; '
                                               'global %.2f%% of %s' %
                                               (percent, hsize, sum_percent,
                                                sum_hsize),
                                    ))
                                    chunk = handle.read(CHUNK_SIZE)  # read more
                        self._output_queue.put(dict(
                            type='meta',
                            src_dir=src_dir,
                            dst_file=dst_file,
                            data=src_file,
                            status=None,
                        ))
                except Queue.Empty:
                    raise
                except KeyboardInterrupt:
                    raise
                except Exception as reason:
                    self._logger.error(reason)

                self._input_queue.task_done()
            except Queue.Empty:
                time.sleep(0.1)
                self._is_idle = True
        self._logger.debug('Stopped thread.')
        self._is_idle = True

    def stop(self):
        if self.is_alive():
            while not self._input_queue.empty() and not self._is_idle:
                time.sleep(0.1)
        self._running = False


class Writer(Thread):

    def __init__(self, input_queue, dirs_need_stats):
        super(Writer, self).__init__()
        self._input_queue = input_queue
        self._dirs_need_stats = dirs_need_stats
        self._running = True
        self._is_idle = True
        self._num_files = 0
        self._num_symlinks = 0
        self._logger = logging.getLogger('backup.writer')

    def run(self):
        self._logger.debug('Started thread.')
        handle = None
        while self._running:
            try:
                item = self._input_queue.get(timeout=0.1)
                type_ = item['type']
                src_dir = item['src_dir']
                dst_file = item['dst_file']
                data = item['data']
                status = item['status']

                msg = '%s|%s' % (type_, basename(dst_file))
                if status:
                    msg += ' (%s)' % status
                if type_ == 'meta':
                    self._logger.debug(msg)
                else:
                    self._logger.info(msg)
                self._is_idle = False

                try:
                    if not exists(dirname(dst_file)):
                        makedirs(dirname(dst_file))
                        self._dirs_need_stats.append(src_dir)

                    if type_ == 'symlink':
                        symlink(data, dst_file)
                        self._num_symlinks += 1
                        self._logger.debug('Created symlink: %s -> %s' %
                                          (dst_file, data))
                    elif type_ == 'special':
                        if data == 'char file':
                            self._logger.warning('Char file is not supported.')
                        elif data == 'block file':
                            self._logger.warning('Block file is not supported.')
                        elif data == 'fifo':
                            mknod(dst_file, stat.S_IFIFO)
                            self._num_files += 1
                            self._logger.debug('Created fifo: %s' % dst_file)
                        elif data == 'socket/pipe':
                            mknod(dst_file, stat.S_IFSOCK)
                            self._num_files += 1
                            self._logger.debug('Created socket: %s' % dst_file)
                    elif type_ == 'file':
                        if handle is None:
                            handle = open(dst_file, 'wb')
                            self._num_files += 1
                            self._logger.debug('Created file: %s' % dst_file)
                        elif dst_file != handle.name:
                            handle.close()
                            handle = open(dst_file, 'wb')
                            self._num_files += 1
                            self._logger.debug('Created file: %s' % dst_file)
                        handle.write(data)
                    elif type_ == 'meta':
                        if handle and handle.name == dst_file:
                            handle.close()
                        copystat(data, dst_file, follow_symlinks=False)
                except KeyboardInterrupt:
                    raise
                except Exception as reason:
                    self._logger.error(reason)

                self._input_queue.task_done()
            except Queue.Empty:
                self._is_idle = True
                time.sleep(0.1)
        if handle:
            handle.close()
        self._logger.debug('Stopped thread.')
        self._is_idle = True

    def stop(self):
        if self.is_alive():
            while not self._input_queue.empty() and not self._is_idle:
                time.sleep(0.1)
        self._running = False


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
                self._logger.error(reason)
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
