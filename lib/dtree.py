from sys import exit, version_info
from os.path import join, split, islink
from shutil import copystat as shutil_copystat
from os import access, R_OK, X_OK
import logging
import re


try:
    from scandir import scandir  # , DirEntry
except ImportError:
    print('ERROR: Please install the scandir package from: '
          'https://github.com/benhoyt/scandir')
    print('       You can do so by e.g. invoking the following command:')
    print('       pip install '
          'https://github.com/benhoyt/scandir/archive/master.zip')
    exit(1)


logger = logging.getLogger('dtree')


if version_info < (3, 3):
    logger.warn('WARNING: Python version older than 3.3 does not support '
                'copystat on symlinks! (chmod, timestamp, etc.)')

    def copystat(src, dst, follow_symlinks=True):
        if follow_symlinks or not islink(dst):
            shutil_copystat(src, dst)
else:
    copystat = shutil_copystat


def _walk(top, excludes):
    dirs = []
    files = []
    if excludes:
        def is_excluded(path):
            for regex in excludes:
                if regex.search(path):
                    return True
            return False
    else:
        is_excluded = lambda path: False  # Faster if no excludes given.
    path = join(top._path, top.name)
    for entry in scandir(path):
        entry_path = join(entry._path, entry.name)
        if is_excluded(entry_path):
            logger.info('Excluded path: %s' % entry_path)
            continue
        if entry.is_dir():
            dirs.append(entry)
        else:
            files.append(entry)
    yield top, dirs, files
    for entry in dirs:
        if not entry.is_symlink():
            if access(join(entry._path, entry.name), R_OK | X_OK):
                for x in _walk(entry, excludes):
                    yield x
            else:
                logger.warning('Could not access dir: %s' % join(entry._path, entry.name))


def walk(path, excludes):
    dir_path, dir_name = split(path)
    for entry in scandir(dir_path):
        if entry.name == dir_name:
            return _walk(entry, excludes)
    raise Exception('Directory "%s" not found in "%s".' % (dir_name, dir_path))


def scan(path, excludes=[]):
    excludes = tuple(map(re.compile, excludes))
    for root, dirs, files in walk(path, excludes):
        stat = root.lstat()
        mtime = stat.st_mtime
        inode = stat.st_ino
        if len(dirs) > 1:
            dirs.sort(key=lambda item: item.lstat().st_ino)
        yield root, mtime, inode, dirs, files
