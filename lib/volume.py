from os.path import join
from gi.repository import Gio, GObject
import logging


logger = logging.getLogger('volume')


def __mount_done_cb(obj, res, user_data):
    user_data['status'] = obj.mount_finish(res)
    user_data['loop'].quit()


def mount(uri):
    protocol, path = uri.split('://', 1)
    # print(protocol, path)
    if protocol == 'volume':
        volume_name, path = path.split('/', 1)
        mo = Gio.MountOperation()
        mo.set_anonymous(True)
        vm = Gio.VolumeMonitor.get()
        loop = GObject.MainLoop()
        user_data = dict(loop=loop, status=None)
        volume_found = False
        volume_mounted = False
        for volume in vm.get_volumes():
            name = volume.get_name()
            if name == volume_name:
                mount = volume.get_mount()
                if not mount:
                    volume.mount(0, mo, None, __mount_done_cb, user_data)
                    volume_mounted = True
                volume_found = volume
        if volume_mounted:
            loop.run()
        elif not volume_found:
            raise Exception('Volume not found: %s' % volume_name)
        if not user_data['status'] and volume_mounted:
            raise Exception('Could not mount volume: %s' % volume_name)
        path = join(volume_found.get_mount().get_root().get_path(), path)
        return volume_found if volume_mounted else False, path
    else:
        raise Exception('Unknown protocol: %s' % protocol)


def __unmount_done_cb(obj, res, user_data):
    user_data['status'] = obj.unmount_with_operation_finish(res)
    user_data['loop'].quit()


def umount(volume):
    mount = volume.get_mount()
    loop = GObject.MainLoop()
    user_data = dict(loop=loop, status=None)
    mount.unmount_with_operation(0, None, None, __unmount_done_cb, user_data)
    loop.run()
    if not user_data['status']:
        logger.warning('Could not unmount volume: %s' % volume.get_name())
