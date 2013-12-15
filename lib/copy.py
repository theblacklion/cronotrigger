from os.path import exists, basename, dirname
from os import (makedirs, readlink, symlink, stat as os_stat,
                mknod, fstat as os_fstat)
import stat
import time
import logging
from threading import Thread
try:
    import Queue  # Python 2
except ImportError:
    import queue as Queue  # Python 3

from lib.human_size import human_size
from lib.dtree import copystat


# Define output chunk and queue size. E.g. 1 MB * 100 = 100 MB
CHUNK_SIZE = 1024 * 1024 * 5  # Bytes
QUEUE_SIZE = 25  # Length

CHUNK_TYPE_EMPTY = 0

CHUNK_PART_SIZE = 64 * 1024  # Bytes
CHUNK_PART_SPARSE_DATA = b'\0' * CHUNK_PART_SIZE
CHUNK_PART_TYPE_SPARSE = None


class Reader(Thread):

    def __init__(self, input_queue, output_queue, sum_bytes):
        super(Reader, self).__init__()
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._sum_bytes = sum_bytes
        self._running = True
        self._is_idle = True
        self._logger = logging.getLogger('copy.reader')

    def add_more_bytes(self, count):
        self._sum_bytes += count

    def _read_chunk(self, handle, detect_sparse=False):
        cur_size = 0
        chunk = []
        chunk_append = chunk.append
        handle_read = handle.read
        while cur_size < CHUNK_SIZE:
            part = handle_read(CHUNK_PART_SIZE)
            part_len = len(part)
            if not part_len:
                break
            if detect_sparse and part_len == CHUNK_PART_SIZE and part == CHUNK_PART_SPARSE_DATA:
                part = CHUNK_PART_TYPE_SPARSE
            chunk_append(part)
            cur_size += part_len
        return chunk, cur_size

    def run(self):
        self._logger.debug('Started thread.')
        sum_bytes = self._sum_bytes
        sum_bytes_transferred = 0
        read_chunk = self._read_chunk
        while self._running:
            try:
                item = self._input_queue.get(timeout=0.1)
                src_dir = item['src_dir']  # Only for makedirs later on.
                src_file = item['src_file']
                dst_file = item['dst_file']
                size = item['size']
                is_link = item['is_link']
                is_file = item['is_file']

                # Currently only used by the restore process.
                if item['src_resolver']:
                    src_file = item['src_resolver'](src_file)
                    src_dir = dirname(src_file)

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
                    elif size == 0:  # Empty file.
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
                            data=CHUNK_TYPE_EMPTY,
                            status='file %.2f%% of %s; '
                                   'global %.2f%% of %s' %
                                   (percent, hsize, sum_percent,
                                    sum_hsize),
                        ))
                        self._output_queue.put(dict(
                            type='meta',
                            src_dir=src_dir,
                            dst_file=dst_file,
                            data=src_file,
                            status=None,
                        ))
                    else:  # Normal file.
                        with open(src_file, 'rb') as handle:
                            detect_sparse = False
                            if size >= CHUNK_SIZE:
                                if os_fstat(handle.fileno()).st_blocks * 512 < size:
                                    detect_sparse = True
                            chunk, chunk_len = read_chunk(handle, detect_sparse=detect_sparse)  # read chunk
                            bytes_transferred = 0
                            while chunk_len and self._running:
                                bytes_transferred += chunk_len
                                percent = 100.0 / size * bytes_transferred
                                hsize = human_size(size)
                                sum_bytes_transferred += chunk_len
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
                                chunk, chunk_len = read_chunk(handle, detect_sparse=detect_sparse)  # read more
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
                    self._logger.exception(reason)

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
        self._logger = logging.getLogger('copy.writer')

    def _write_chunk(self, handle, chunk):
        # print('got %d parts in chunk' % len(chunk))
        for part in chunk:
            if part is not CHUNK_PART_TYPE_SPARSE:
                # print('NORMAL')
                handle.write(part)
            else:
                # print('SPARSE')
                handle.seek(CHUNK_PART_SIZE, 1)  # 1 means cur file pos.
                # print(handle.tell())

    def run(self):
        self._logger.debug('Started thread.')
        handle = None
        write_chunk = self._write_chunk
        while self._running:
            try:
                item = self._input_queue.get(timeout=0.1)
                type_ = item['type']
                src_dir = item['src_dir']  # Only for makedirs later on.
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
                            handle.truncate()
                            handle.close()
                            handle = open(dst_file, 'wb')
                            self._num_files += 1
                            self._logger.debug('Created file: %s' % dst_file)
                        if data is CHUNK_TYPE_EMPTY:
                            pass  # Nothing to write.
                        else:
                            write_chunk(handle, data)
                    elif type_ == 'meta':
                        if handle and handle.name == dst_file:
                            handle.truncate()
                            handle.close()
                            handle = None
                        copystat(data, dst_file, follow_symlinks=False)
                except KeyboardInterrupt:
                    raise
                except Exception as reason:
                    self._logger.exception(reason)

                self._input_queue.task_done()
            except Queue.Empty:
                self._is_idle = True
                time.sleep(0.1)
        if handle:
            handle.truncate()
            handle.close()
        self._logger.debug('Stopped thread.')
        self._is_idle = True

    def stop(self):
        if self.is_alive():
            while not self._input_queue.empty() and not self._is_idle:
                time.sleep(0.1)
        self._running = False
