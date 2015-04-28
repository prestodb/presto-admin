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

from fabric.api import task, sudo, env
from fabric.context_managers import settings, hide
from fabric.decorators import runs_once
from fabric.operations import run
from fabric.tasks import execute
from fabric.utils import abort, warn

from prestoadmin import configure_cmds
from prestoadmin import connector
from prestoadmin import topology
from prestoadmin.config import ConfigFileNotFoundError
from prestoadmin.prestoclient import PrestoClient
from prestoadmin.topology import requires_topology
from prestoadmin.util.fabricapi import execute_fail_on_error
import package


__all__ = ['install', 'uninstall', 'start', 'stop', 'restart', 'status']

INIT_SCRIPTS = '/etc/init.d/presto'
RETRY_TIMEOUT = 60
SLEEP_INTERVAL = 5
SERVER_CHECK_SQL = "select * from system.runtime.nodes"
STATUS_INFO_SQL = "select http_uri, node_version, active from " \
                  "system.runtime.nodes"
CONNECTOR_INFO_SQL = "select catalog_name from system.metadata.catalogs"
PRESTO_RPM_VERSION = 100
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

    with settings(parallel=False):
        host_list = set_hosts()
    execute_fail_on_error(deploy_install_configure, local_path,
                          hosts=host_list)


def set_hosts():
    if 'topology_config_not_found' in env and env.topology_config_not_found \
            is not None:
        topology.set_conf_interactive()
        topology.set_env_from_conf()
    return [host for host in env.hosts if host not in env.exclude_hosts]


def deploy_install_configure(local_path):
    package.install(local_path)
    update_configs()


def update_configs():
    configure_cmds.deploy()
    try:
        connector.add()
    except ConfigFileNotFoundError:
        _LOGGER.info("No connector directory found, not adding connectors.")


@task
@requires_topology
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


def check_status_for_control_commands():
    check_presto_version()
    if check_server_status():
        print("Server started successfully on: " + env.host)
    else:
        warn("Server failed to start on: " + env.host)


@task
@requires_topology
def start():
    """
    Start the Presto server on all nodes, unless some are excluded using
    -x/--exclude-hosts.

    A status check is performed on the entire cluster and a list of
    servers that did not start, if any, are reported at the end.
    """
    service(' start')
    check_status_for_control_commands()


@task
@requires_topology
def stop():
    """
    Stop the Presto server on all nodes, unless some are excluded using
    -x/--exclude-hosts.
    """
    service(' stop')


@task
@requires_topology
def restart():
    """
    Restart the Presto server on all nodes, unless some are excluded using
    -x/--exclude-hosts.
    """
    service(' restart')
    check_status_for_control_commands()


def check_presto_version():
    with settings(hide('warnings', 'stdout'), warn_only=True):
        version = run("rpm -q --qf \"%{VERSION}\\n\" presto")
        _LOGGER.debug("Presto rpm version: " + version)
    try:
        float(version)
        version_number = version.strip().split('.')
        if int(version_number[1]) < PRESTO_RPM_VERSION:
            warn("%s: Status check requires Presto version >= 0.%d"
                 % (env.host, PRESTO_RPM_VERSION))
    except ValueError:
        warn("%s: No suitable presto version found" % env.host)
        pass


def check_server_status():
    result = True
    time = 0
    while time < RETRY_TIMEOUT:
        client = PrestoClient(env.host, env.user)
        result = client.execute_query(SERVER_CHECK_SQL)
        if not result:
            run('sleep %d' % SLEEP_INTERVAL)
            _LOGGER.debug("Status retrieval for the server failed after "
                          "waiting for %d seconds. Retrying..." % time)
            time += SLEEP_INTERVAL
        else:
            break
    return result


def run_sql(host, sql):
    client = PrestoClient(host, env.user)
    status = client.execute_query(sql, host, env.user)
    if status:
        return client.get_rows()
    else:
        # TODO: Check if we can get some error cause from server response and
        # log that to the user
        _LOGGER.error("Querying server failed")
        return []


def get_status_info(host):
    """
    Returns [[http_uri, node_version, active], [http_uri2, ..]..]
    from nodes system table
    :param host:
    :return:
    """
    return run_sql(host, STATUS_INFO_SQL)


def get_connector_info(host):
    """
    Returns [[catalog_name], [catalog_2]..] from catalogs system table
    :param host:
    :return:
    """
    return run_sql(host, CONNECTOR_INFO_SQL)


def is_server_up(status):
    if status:
        return "Running"
    else:
        return "Not Running"


def get_status_for(host):
    """
    Returns presto server status
    :param host:
    :return:server status eg format:
    {"http://node1/statement": [presto-main:0.97-SNAPSHOT, Running]}
    """
    output = {}
    status_info = get_status_info(host)
    for status in status_info:
        if status:
            output[status[0]] = [status[1], is_server_up(status[2])]

    _LOGGER.info("Server status: %s ", output)
    return output


def get_conn_for(host):
    """
    Returns installed connectors
    :param host:
    :return: : comma delimited connectors eg: tpch, hive, system
    """
    syscatalog = []
    connector_info = get_connector_info(host)
    for conn_info in connector_info:
        if conn_info:
            syscatalog.append(conn_info[0])
    return ', '.join(syscatalog)


def status_show():
    server_status = get_status_for(env.host)
    connector_status = get_conn_for(env.host)

    for k in server_status:
        print('Node status:')
        print('\tNode URI(http) :' + str(k) +
              '\n\tPresto Version :' + str(server_status[k][0]) +
              '\n\tServer Status  :' + str(server_status[k][1]))
        if connector_status:
            print('\tConnectors     :' + connector_status)


@task
@requires_topology
@runs_once
def status():
    execute(check_presto_version, roles=env.roles)
    with settings(parallel=False):
        status_show()
