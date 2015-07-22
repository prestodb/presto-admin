import logging
from fabric.operations import put, sudo
from fabric.decorators import task
from os import path

_LOGGER = logging.getLogger(__name__)
__all__ = ['run']


@task
def run(script, remote_dir='/tmp'):
    """
    Run an arbitrary script on all nodes in the cluster.

    Parameters:
        script - The path to the script
        remote_dir - Where to put the script on the cluster.  Default is /tmp.
    """
    script_name = path.basename(script)
    remote_path = path.join(remote_dir, script_name)
    put(script, remote_path)
    sudo('chmod u+x %s' % remote_path)
    sudo(remote_path)
    sudo('rm %s' % remote_path)
