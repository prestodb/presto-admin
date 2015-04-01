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

import os

from fabric.api import roles, task, sudo, put, env

from prestoadmin import topology
from prestoadmin.util.fabricapi import execute_fail_on_error

__all__ = ["server"]

PRESTO_ADMIN_PACKAGES_PATH = "/opt/presto-admin/packages"
LOCAL_ARCHIVE_PATH = '/tmp'
PRESTO_RPM = 'presto-*.rpm'
PRESTO_RPM_PATH = PRESTO_ADMIN_PACKAGES_PATH + "/" + PRESTO_RPM


@task
def server(local_path=None):
    """
    This task copies the presto-server rpm to all nodes in the cluster
    and installs it on the nodes

    :param local_path: Path to local archive to be deployed
    """
    if local_path is None:
        local_path = os.path.join(LOCAL_ARCHIVE_PATH, PRESTO_RPM)

    set_hosts()
    execute_fail_on_error(deploy_install_configure, local_path,
                          hosts=env.hosts)


def set_hosts():
        if 'failed_topology_error' in env and env.failed_topology_error \
                is not None:
            topology.set_conf_interactive()
            topology.set_roledefs_from_conf()


def deploy_install_configure(local_path):
    deploy_package(local_path)
    rpm_install()
    update_configs()


def deploy_package(local_path=None):
    sudo('mkdir -p ' + PRESTO_ADMIN_PACKAGES_PATH)
    put(local_path, PRESTO_ADMIN_PACKAGES_PATH, use_sudo=True)


def rpm_install():
    sudo('rpm -i ' + PRESTO_RPM_PATH)


def update_configs():
    update_coordinator_config()
    update_worker_config()


@roles('coordinator')
def update_coordinator_config():
    pass


@roles('worker')
def update_worker_config():
    pass
