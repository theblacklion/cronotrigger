# TODO Implement same functionality using gi.repository etc. and only use
#      subprocess as fallback.
#      http://www.micahcarrick.com/gsettings-python-gnome-3.html
#      http://www.vitavonni.de/blog/201103/
#        2011031501-gnome3-in-debian-experimental.html

import subprocess


def get_int(path, key):
    return int(subprocess.check_output(['gsettings', 'get', path, key]))


def set(path, key, value):
    subprocess.call(['gsettings', 'set', path, key, str(value)])
