# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for rpm package deploy and install using presto-admin
"""
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
    """
    Install the rpm package on the cluster

    Parameters:
        ocal_path - Absolute path to the rpm to be installed
    """
    deploy(local_path)
    rpm_install(os.path.basename(local_path))


def deploy(local_path=None):
    _LOGGER.debug("Deploying rpm to nodes")
    sudo('mkdir -p ' + constants.REMOTE_PACKAGES_PATH)
    try:
        put(local_path, constants.REMOTE_PACKAGES_PATH, use_sudo=True)
    except BaseException as e:
        _LOGGER.warn("Failure during put. Now using /tmp as temp dir...", e)
        put(local_path, constants.REMOTE_PACKAGES_PATH, use_sudo=True,
            temp_dir='/tmp')


def rpm_install(rpm_name):
    _LOGGER.info("Installing the rpm")
    sudo('rpm -i ' + constants.REMOTE_PACKAGES_PATH + "/" + rpm_name)
