from fabric.api import env    

import errno
import os

config_root = os.getenv('WORKSPACE', '~')
env.config_directory = os.path.join(os.path.expanduser(config_root), '.fabric')
try:
    os.makedirs(env.config_directory)
except OSError as ex:
    if ex.errno != errno.EEXIST:
        raise

env.roledefs = {
        'coordinator': [],
        'worker': [],
        'all': [],
}

