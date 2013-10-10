from sys import exit, version_info
from os.path import join, split, islink
from shutil import copystat as shutil_copystat
from os import access, R_OK, X_OK
import logging


if version_info < (3, 3):
    print('WARNING: Python version older than 3.3 does not support copystat on '
          'symlinks! (chown, timestamp etc.)')

    def copystat(src, dst, follow_symlinks=True):
        if islink(dst):
            return
        shutil_copystat(src, dst)
else:
    copystat = lambda src, dst, follow_symlinks=True: shutil_copystat(
        src, dst, follow_symlinks=follow_symlinks)


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


def _walk(top):
    dirs = []
    files = []
    path = join(top._path, top.name)
    for entry in scandir(path):
        if entry.isdir():
            dirs.append(entry)
        else:
            files.append(entry)
    yield top, dirs, files
    for entry in dirs:
        if not entry.islink():
            if access(join(entry._path, entry.name), R_OK | X_OK):
                for x in _walk(entry):
                    yield x
            else:
                logger.warning('Could not access dir: %s' % join(entry._path, entry.name))


def walk(path):
    dir_path, dir_name = split(path)
    for entry in scandir(dir_path):
        if entry.name == dir_name:
            return _walk(entry)
    # top = DirEntry(dir_path, dir_name, None, None)
    # return _walk(top)
    raise Exception('Directory "%s" not found in "%s".' % (dir_name, dir_path))


def scan(path):
    for root, dirs, files in walk(path):
        mtime = root.lstat().st_mtime
        inode = root.dirent.d_ino
        if len(dirs) > 1:
            dirs.sort(key=lambda item: item.dirent.d_ino)
        yield root, mtime, inode, dirs, files
