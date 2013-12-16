from os.path import join, exists
from os import access, R_OK, X_OK
import sqlite3
import logging
from threading import Thread
import time
try:
    import Queue  # Python 2
except ImportError:
    import queue as Queue  # Python 3


class Feeder(Thread):

    def __init__(self, input_queue, db_path):
        super(Feeder, self).__init__()
        self._input_queue = input_queue
        self._running = True
        self._is_idle = True
        self._logger = logging.getLogger('index.feeder')
        self._db_path = db_path

    def run(self):
        self._logger.debug('Started thread.')
        self._db_conn = sqlite3.connect(self._db_path)
        cur = self._db_conn.cursor()
        while self._running:
            try:
                item = self._input_queue.get(timeout=0.1)
                self._is_idle = False

                try:
                    dir_data = item['dir_data']
                    file_data = item['file_data']

                    cur.execute('''INSERT INTO cur_dirs(path, mtime, inode)
                                   values (?, ?, ?)''', dir_data)

                    cur.executemany('''INSERT INTO cur_files
                                       (path, name, mtime, size, islink, isfile, inode)
                                       values (?, ?, ?, ?, ?, ?, ?)''', file_data)
                except KeyboardInterrupt:
                    raise
                except Exception as reason:
                    self._logger.error(reason)

                self._input_queue.task_done()
            except Queue.Empty:
                time.sleep(0.1)
                self._is_idle = True
        self._logger.debug('Stopped thread.')
        self._db_conn.commit()
        self._is_idle = True

    def stop(self):
        if self.is_alive():
            while not self._input_queue.empty() and not self._is_idle:
                time.sleep(0.1)
        self._running = False


class Index(object):

    def __init_db(self):
        with self._db_conn as cur:
            cur.execute('''CREATE TABLE dirs
                           (path text, mtime integer, inode integer)''')
            cur.execute('''CREATE TABLE files
                           (path text, name text, mtime integer,
                            size integer, islink integer, isfile integer,
                            inode integer)''')
            cur.execute('''CREATE TABLE cur_dirs
                           (path text, mtime integer, inode integer)''')
            cur.execute('''CREATE TABLE cur_files
                           (path text, name text, mtime integer,
                            size integer, islink integer, isfile integer,
                            inode integer)''')
            cur.execute('''CREATE INDEX 'dirs_INDEX_inode' ON 'dirs' ('inode' ASC)''')
            cur.execute('''CREATE INDEX 'files_INDEX_inode' ON 'files' ('inode' ASC)''')
            cur.execute('''CREATE INDEX 'cur_dirs_INDEX_inode' ON 'cur_dirs' ('inode' ASC)''')
            cur.execute('''CREATE INDEX 'cur_files_INDEX_inode' ON 'cur_files' ('inode' ASC)''')
            cur.execute('''CREATE INDEX 'files_INDEX_size' ON 'files' ('size' ASC)''')
            cur.execute('''CREATE INDEX 'cur_files_INDEX_size' ON 'cur_files' ('size' ASC)''')
            cur.execute('''CREATE INDEX 'files_INDEX_mtime' ON 'files' ('mtime' ASC)''')
            cur.execute('''CREATE INDEX 'cur_files_INDEX_mtime' ON 'cur_files' ('mtime' ASC)''')

    def __truncate_tmp_tables(self):
        with self._db_conn as cur:
            cur.execute('''DELETE FROM cur_dirs''')
            cur.execute('''DELETE FROM cur_files''')
            cur.execute('''VACUUM''')

    def __init__(self, db_path):
        super(Index, self).__init__()
        self._logger = logging.getLogger('index')
        self._db_path = db_path
        if exists(db_path):
            self._db_conn = sqlite3.connect(db_path)
        else:
            self._db_conn = sqlite3.connect(db_path)
            self.__init_db()
        self._db_conn.text_factory = str
        self.__truncate_tmp_tables()

    def update(self, nodes):
        # TODO we should save rights, timestamp and owners in the db. Restore should use these.
        def entry_list(files):
            results = []
            for entry in files:
                # This is flawed on i.e. broken links. We should just catch up errors on lstat below, I suppose...
                # if not access(join(entry._path, entry.name), R_OK):
                #     self._logger.warning('Could not access file: %s' % join(entry._path, entry.name))
                #     continue
                try:
                    lstat = entry.lstat()
                    results.append((entry._path, entry.name, lstat.st_mtime,
                                    lstat.st_size, entry.is_symlink(), entry.is_file(),
                                    lstat.st_ino))
                except (OSError, IOError) as reason:
                    self._logger.error(reason)
            return results
        queue = Queue.Queue(maxsize=100000)
        feeder = Feeder(queue, self._db_path)
        feeder_started = False
        # print('scanning')
        # start = time.time()
        for root, root_mtime, root_inode, subdirs, files in nodes:
            # print root, root_mtime, subdirs, files
            if len(files) > 1:
                files.sort(key=lambda item: item.lstat().st_ino)
            try:
                queue.put_nowait(dict(
                    dir_data=(join(root._path, root.name), root_mtime, root_inode),
                    file_data=entry_list(files),
                ))
            except Queue.Full:
                # print('Queue is full.')
                if not feeder_started:
                    feeder.start()
                    feeder_started = True
                    # print('Waiting for feeder to come up.')
                queue.put(dict(
                    dir_data=(join(root._path, root.name), root_mtime, root_inode),
                    file_data=entry_list(files),
                ))
                time.sleep(1)
                # print('Continuing scan.')
        # print(time.time() - start)
        # start = time.time()
        # print('waiting')
        if not feeder_started:
            feeder.start()
            # print('Waiting for feeder to come up.')
            time.sleep(1)
        feeder.stop()
        feeder.join()
        # print(time.time() - start)

    def get_cur_stats(self):
        cur = self._db_conn.cursor()
        cur.execute('''SELECT count(inode) FROM cur_dirs limit 1''')
        dirs_found = cur.fetchone()[0]
        cur.execute('''SELECT count(inode) FROM cur_files limit 1''')
        files_found = cur.fetchone()[0]
        return dirs_found, files_found

    def get_all_dirs(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_dirs.* FROM cur_dirs'''
            return cur.execute(sql)

    def get_added_dirs(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_dirs.* FROM cur_dirs
                     LEFT JOIN dirs USING (path)
                     WHERE dirs.mtime IS NULL'''
            return cur.execute(sql)

    def get_modified_dirs(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_dirs.* FROM cur_dirs
                     LEFT JOIN dirs USING (path)
                     WHERE dirs.mtime IS NOT NULL
                     AND dirs.mtime != cur_dirs.mtime'''
            return cur.execute(sql)

    def get_added_or_modified_dirs(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_dirs.* FROM cur_dirs
                     LEFT JOIN dirs USING (path)
                     WHERE (dirs.mtime IS NULL) OR (dirs.mtime IS NOT NULL
                     AND dirs.mtime != cur_dirs.mtime)'''
            return cur.execute(sql)

    get_selected_dirs = get_all_dirs

    def get_added_files(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_files.* FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE files.mtime IS NULL
                     ORDER BY cur_files.inode asc'''
            return cur.execute(sql)

    def get_modified_files(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_files.* FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE files.mtime IS NOT NULL
                     AND files.mtime != cur_files.mtime
                     ORDER BY cur_files.inode asc'''
            return cur.execute(sql)

    def get_added_or_modified_files(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_files.* FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE (files.mtime IS NULL) OR (files.mtime IS NOT NULL
                     AND files.mtime != cur_files.mtime)
                     ORDER BY cur_files.inode asc'''
            return cur.execute(sql)

    def get_unmodified_files(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_files.* FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE files.mtime IS NOT NULL
                     AND files.mtime == cur_files.mtime
                     ORDER BY cur_files.inode asc'''
            return cur.execute(sql)

    def get_selected_files(self):
        with self._db_conn as cur:
            sql = '''SELECT cur_files.* FROM cur_files'''
            return cur.execute(sql)

    def get_added_bytes(self):
        with self._db_conn as cur:
            sql = '''SELECT sum(cur_files.size) FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE files.mtime IS NULL
                     LIMIT 1'''
            return cur.execute(sql).fetchone()[0] or 0

    def get_modified_bytes(self):
        with self._db_conn as cur:
            sql = '''SELECT sum(cur_files.size) FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE files.mtime IS NOT NULL
                     AND files.mtime != cur_files.mtime
                     LIMIT 1'''
            return cur.execute(sql).fetchone()[0] or 0

    def get_selected_bytes(self):
        with self._db_conn as cur:
            sql = '''SELECT sum(cur_files.size) FROM cur_files
                     LIMIT 1'''
            return cur.execute(sql).fetchone()[0] or 0

    def get_added_or_modified_bytes(self):
        with self._db_conn as cur:
            sql = '''SELECT sum(cur_files.size) FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE (files.mtime IS NULL) OR (files.mtime IS NOT NULL
                     AND files.mtime != cur_files.mtime)
                     LIMIT 1'''
            return cur.execute(sql).fetchone()[0] or 0

    def get_num_added_or_modified_dirs_or_files(self):
        with self._db_conn as cur:
            sql = '''SELECT count(cur_files.inode) FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE (files.mtime IS NULL) OR (files.mtime IS NOT NULL
                     AND files.mtime != cur_files.mtime)
                     LIMIT 1'''
            num_files = cur.execute(sql).fetchone()[0]
        with self._db_conn as cur:
            sql = '''SELECT count(cur_dirs.inode) FROM cur_dirs
                     LEFT JOIN dirs USING (path)
                     WHERE (dirs.mtime IS NULL) OR (dirs.mtime IS NOT NULL
                     AND dirs.mtime != cur_dirs.mtime)
                     LIMIT 1'''
            num_dirs = cur.execute(sql).fetchone()[0]
        return num_files + num_dirs

    def __truncate_base_tables(self):
        with self._db_conn as cur:
            cur.execute('''DELETE FROM dirs''')
            cur.execute('''DELETE FROM files''')

    def __migrate_table_data(self):
        with self._db_conn as cur:
            sql = '''INSERT INTO dirs SELECT * FROM cur_dirs'''
            cur.execute(sql)
            sql = '''INSERT INTO files SELECT * FROM cur_files'''
            cur.execute(sql)

    def commit(self):
        self.__truncate_base_tables()
        self.__migrate_table_data()
        self.__truncate_tmp_tables()

    def select(self, path):
        with self._db_conn as cur:
            sql = '''INSERT INTO cur_dirs SELECT * FROM dirs
                     WHERE dirs.path like ?'''
            cur.execute(sql, ['%s%%' % path])
            sql = '''INSERT INTO cur_files SELECT * FROM files
                     WHERE files.path like ?'''
            cur.execute(sql, ['%s%%' % path])
