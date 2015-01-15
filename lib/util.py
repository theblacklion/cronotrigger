from os.path import expanduser
from string import Template
import socket


_custom_vars = {
    'hostname': socket.gethostname(),
}


def expandvars(template):
    template = Template(template).safe_substitute(_custom_vars)
    template = expanduser(template)
    return template
