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
import logging
import os

from fabric.api import task, sudo, put, env
from fabric.decorators import runs_once
from fabric.utils import abort

from prestoadmin import connector
from prestoadmin import configure
from prestoadmin import topology
from prestoadmin.configuration import ConfigFileNotFoundError
from prestoadmin.util.constants import CONNECTORS_CONFIG_FILE
from prestoadmin.util.fabricapi import execute_fail_on_error

__all__ = ['install', 'uninstall', 'start', 'stop', 'restart']

PRESTO_ADMIN_PACKAGES_PATH = "/opt/presto-admin/packages"
INIT_SCRIPTS = '/etc/init.d/presto'

_LOGGER = logging.getLogger(__name__)


@task
@runs_once
def install(local_path=None):
    """
    Copy and install the presto-server rpm to all the nodes in the cluster and
    configure the nodes. Executed on all nodes unless some are excluded using
    -x/--exclude-hosts.

    The topology information will be read from the config.json file. If this
    file is missing, then the co-ordinator and workers will be obtained
    interactively.

    The connector configuration will be read from connectors.json. If this
    file is missing or empty then no connector configuration is deployed.
    Install will fail for invalid json configuration.

    :param local_path: Absolute path to local rpm to be deployed
    """
    if local_path is None:
        abort("Missing argument local_path: Absolute path to "
              "local rpm to be deployed")

    host_list = set_hosts()
    execute_fail_on_error(deploy_install_configure, local_path,
                          hosts=host_list)


def set_hosts():
        if 'topology_config_not_found' in env and env.topology_config_not_found \
                is not None:
            topology.set_conf_interactive()
            topology.set_roledefs_from_conf()
        return [host for host in env.hosts if host not in env.exclude_hosts]


def deploy_install_configure(local_path):
    deploy_package(local_path)
    rpm_install(os.path.basename(local_path))
    update_configs()


def deploy_package(local_path=None):
    _LOGGER.debug("Deploying presto rpm to nodes")
    sudo('mkdir -p ' + PRESTO_ADMIN_PACKAGES_PATH)
    put(local_path, PRESTO_ADMIN_PACKAGES_PATH, use_sudo=True)


def rpm_install(rpm_name):
    _LOGGER.info("Installing the rpm")
    sudo('rpm -i ' + PRESTO_ADMIN_PACKAGES_PATH + "/" + rpm_name)


def update_configs():
    configure.all()
    try:
        connector.add()
    except ConfigFileNotFoundError:
        _LOGGER.debug("Connector configuration %s not found. No connector "
                      "configuration will be deployed."
                      % CONNECTORS_CONFIG_FILE)
        pass


@task
def uninstall():
    """
    Uninstall Presto after stopping the services on all nodes, unless some are
    excluded using -x/--exclude-hosts.
    """
    stop()
    sudo('rpm -e presto')


def service(control=None):
    _LOGGER.info("Executing %s on presto server" % control)
    sudo(INIT_SCRIPTS + control, pty=False)


@task
@runs_once
def start():
    """
    Start the Presto server on all nodes, unless some are excluded using
    -x/--exclude-hosts.
    """
    execute_fail_on_error(service, ' start', roles=env.roles)


@task
@runs_once
def stop():
    """
    Stop the Presto server on all nodes, unless some are excluded using
    -x/--exclude-hosts.
    """
    execute_fail_on_error(service, ' stop', roles=env.roles)


@task
@runs_once
def restart():
    """
    Restart the Presto server on all nodes, unless some are excluded using
    -x/--exclude-hosts.
    """
    execute_fail_on_error(service, ' restart', roles=env.roles)
