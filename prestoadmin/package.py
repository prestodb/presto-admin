import logging
from fabric.decorators import task

from fabric.operations import sudo, put, os
from prestoadmin.topology import requires_topology
from prestoadmin.util import constants

_LOGGER = logging.getLogger(__name__)
__all__ = ['install']


@task
@requires_topology
def install(local_path):
    deploy(local_path)
    rpm_install(os.path.basename(local_path))


def deploy(local_path=None):
    _LOGGER.debug("Deploying rpm to nodes")
    sudo('mkdir -p ' + constants.REMOTE_PACKAGES_PATH)
    try:
        put(local_path, constants.REMOTE_PACKAGES_PATH, use_sudo=True)
    except Exception as e:
        _LOGGER.warn("Failure during put. Now using /tmp as temp dir...", e)
        put(local_path, constants.REMOTE_PACKAGES_PATH, use_sudo=True,
            temp_dir='/tmp')


def rpm_install(rpm_name):
    _LOGGER.info("Installing the rpm")
    sudo('rpm -i ' + constants.REMOTE_PACKAGES_PATH + "/" + rpm_name)
