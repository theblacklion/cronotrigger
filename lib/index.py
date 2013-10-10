from os.path import join, exists
from os import access, R_OK, X_OK
import sqlite3
import logging


class Index(object):

    def __init_db(self):
        with self._db_conn as cur:
            # TODO Add some indexes on columns used in querries below.
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

    def __add_dir(self, cur, path, mtime, inode):
        cur.execute('''INSERT INTO cur_dirs(path, mtime, inode)
                       values (?, ?, ?)''', (path, mtime, inode))

    def __add_files(self, cur, files):
        cur.executemany('''INSERT INTO cur_files
                           (path, name, mtime, size, islink, isfile, inode)
                           values (?, ?, ?, ?, ?, ?, ?)''', files)

    def update(self, nodes):
        def entry_list(files):
            for entry in files:
                # This is flawed on i.e. broken links. We should just catch up errors on lstat below, I suppose...
                # if not access(join(entry._path, entry.name), R_OK):
                #     self._logger.warning('Could not access file: %s' % join(entry._path, entry.name))
                #     continue
                try:
                    lstat = entry.lstat()
                    yield (entry._path, entry.name, lstat.st_mtime,
                           lstat.st_size, entry.islink(), entry.isfile(),
                           entry.dirent.d_ino)
                except (OSError, IOError) as reason:
                    self._logger.error(reason)
        with self._db_conn as cur:
            for root, root_mtime, root_inode, subdirs, files in nodes:
                # TODO perhaps we can put things into a queue which then fills the db within a second thread?
                # print root, root_mtime, subdirs, files
                self.__add_dir(cur, join(root._path, root.name), root_mtime, root_inode)
                if len(files) > 1:
                    files.sort(key=lambda item: item.dirent.d_ino)
                self.__add_files(cur, entry_list(files))

    def get_cur_stats(self):
        cur = self._db_conn.cursor()
        cur.execute('''SELECT count(path) FROM cur_dirs limit 1''')
        dirs_found = cur.fetchone()[0]
        cur.execute('''SELECT count(path) FROM cur_files limit 1''')
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
                     AND (cur_files.islink OR cur_files.isfile)
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

    def get_added_bytes(self):
        with self._db_conn as cur:
            sql = '''SELECT sum(cur_files.size) FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE files.mtime IS NULL'''
            return cur.execute(sql).fetchone()[0] or 0

    def get_modified_bytes(self):
        with self._db_conn as cur:
            sql = '''SELECT sum(cur_files.size) FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE files.mtime IS NOT NULL
                     AND files.mtime != cur_files.mtime'''
            return cur.execute(sql).fetchone()[0] or 0

    def get_num_added_or_modified_dirs_or_files(self):
        with self._db_conn as cur:
            sql = '''SELECT count(cur_files.inode) FROM cur_files
                     LEFT JOIN files USING (path, name)
                     WHERE (files.mtime IS NULL) OR (files.mtime IS NOT NULL
                     AND files.mtime != cur_files.mtime)'''
            num_files = cur.execute(sql).fetchone()[0]
        with self._db_conn as cur:
            sql = '''SELECT count(cur_dirs.inode) FROM cur_dirs
                     LEFT JOIN dirs USING (path)
                     WHERE (dirs.mtime IS NULL) OR (dirs.mtime IS NOT NULL
                     AND dirs.mtime != cur_dirs.mtime)'''
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
