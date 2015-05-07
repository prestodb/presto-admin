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
from fabric.decorators import task, runs_once
from fabric.operations import sudo, put, os
from prestoadmin import topology
from prestoadmin.util import constants
from prestoadmin.util.fabricapi import execute_fail_on_error, get_host_list

_LOGGER = logging.getLogger(__name__)
__all__ = ['install']


@task
@runs_once
def install(local_path):
    """
    Install the rpm package on the cluster

    Parameters:
        local_path - Absolute path to the rpm to be installed
    """
    topology.set_topology_if_missing()
    execute_fail_on_error(deploy_install, local_path,
                          hosts=get_host_list())


def deploy_install(local_path):
    deploy(local_path)
    rpm_install(os.path.basename(local_path))


def deploy(local_path=None):
    _LOGGER.info("Deploying rpm to nodes")
    sudo('mkdir -p ' + constants.REMOTE_PACKAGES_PATH)
    ret_list = put(local_path, constants.REMOTE_PACKAGES_PATH, use_sudo=True)
    if not ret_list.succeeded:
        _LOGGER.warn("Failure during put. Now using /tmp as temp dir...")
        put(local_path, constants.REMOTE_PACKAGES_PATH, use_sudo=True,
            temp_dir='/tmp')


def rpm_install(rpm_name):
    _LOGGER.info("Installing the rpm")
    sudo('rpm -i ' + constants.REMOTE_PACKAGES_PATH + "/" + rpm_name,
         )
