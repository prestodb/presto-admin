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
Module for installing, monitoring and controlling presto server
using presto-admin
"""
import logging
import re

from fabric.api import task, sudo, env, serial
from fabric.context_managers import settings, hide
from fabric.decorators import runs_once
from fabric.operations import run, os
from fabric.tasks import execute
from fabric.utils import warn

from prestoadmin import configure_cmds
from prestoadmin import connector
from prestoadmin import package
from prestoadmin import topology
from prestoadmin.util.constants import REMOTE_PRESTO_LOG_DIR
from prestoadmin.prestoclient import PrestoClient
from prestoadmin.topology import requires_topology
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigFileNotFoundError, \
    ConfigurationError
from prestoadmin.util.fabricapi import get_host_list
from prestoadmin.util.service_util import lookup_port
import util.filesystem

__all__ = ['install', 'uninstall', 'start', 'stop', 'restart', 'status']

INIT_SCRIPTS = '/etc/rc.d/init.d/presto'
RETRY_TIMEOUT = 120
SLEEP_INTERVAL = 10
SERVER_CHECK_SQL = 'select * from system.runtime.nodes'
NODE_INFO_PER_URI_SQL = 'select http_uri, node_version, active from ' \
                        'system.runtime.nodes where ' \
                        'url_extract_host(http_uri) = \'%s\''
EXTERNAL_IP_SQL = 'select url_extract_host(http_uri) from system.runtime.nodes' \
                  ' WHERE node_id = \'%s\''
CONNECTOR_INFO_SQL = 'select catalog_name from system.metadata.catalogs'
PRESTO_RPM_MIN_REQUIRED_VERSION = 100
PRESTO_TD_RPM = ['101t']
_LOGGER = logging.getLogger(__name__)


@task
@runs_once
def install(local_path):
    """
    Copy and install the presto-server rpm to all the nodes in the cluster and
    configure the nodes.

    The topology information will be read from the config.json file. If this
    file is missing, then the coordinator and workers will be obtained
    interactively. Install will fail for invalid json configuration.

    The connector configurations will be read from the directory
    /etc/opt/prestoadmin/connectors. If this directory is missing or empty
    then no connector configuration is deployed.

    Install will fail for incorrectly formatted configuration files. Expected
    format is key=value for .properties files and one option per line for
    jvm.config

    Parameters:
        local_path - Absolute path to the presto rpm to be installed
    """

    topology.set_topology_if_missing()
    execute(deploy_install_configure, local_path, hosts=get_host_list())


def deploy_install_configure(local_path):
    package.deploy_install(local_path)
    update_configs()


def add_tpch_connector():
    tpch_connector_config = os.path.join(constants.CONNECTORS_DIR,
                                         'tpch.properties')
    util.filesystem.write_to_file_if_not_exists('connector.name=tpch',
                                                tpch_connector_config)


def update_configs():
    configure_cmds.deploy()

    add_tpch_connector()
    try:
        connector.add()
    except ConfigFileNotFoundError:
        _LOGGER.info('No connector directory found, not adding connectors.')


@task
@requires_topology
def uninstall():
    """
    Uninstall Presto after stopping the services on all nodes
    """
    stop()
    ret = sudo('rpm -e presto')
    if ret.succeeded:
        print('Package uninstalled successfully on: ' + env.host)


def service(control=None):
    if check_presto_version() != '':
        return False
    if control.strip() == 'start' and is_port_in_use(env.host):
        return False
    _LOGGER.info('Executing %s on presto server' % control)
    ret = sudo('set -m; ' + INIT_SCRIPTS + control)
    return ret.succeeded


def check_status_for_control_commands():
    client = PrestoClient(env.host, env.port)
    print('Waiting to make sure we can connect to the Presto server on %s, '
          'please wait. This check will time out after %d minutes if the '
          'server does not respond.'
          % (env.host, (RETRY_TIMEOUT / 60)))
    if check_server_status(client):
        print('Server started successfully on: ' + env.host)
    else:
        warn('Server failed to start on: ' + env.host
             + '\nPlease check ' + REMOTE_PRESTO_LOG_DIR + '/server.log')


def is_port_in_use(host):
    _LOGGER.info("Checking if port used by Prestoserver is already in use..")
    try:
        portnum = lookup_port(host)
    except Exception:
        _LOGGER.info("Cannot find port from config.properties. "
                     "Skipping check for port already being used")
        return 0
    with settings(hide('warnings', 'stdout'), warn_only=True):
        output = run('netstat -an |grep %s |grep LISTEN' % str(portnum))
    if output:
        _LOGGER.info("Presto server port already in use. Skipping "
                     "server start...")
        warn('Server failed to start on %s. Port %s already in use'
             % (env.host, str(portnum)))
    return output


@task
@requires_topology
def start():
    """
    Start the Presto server on all nodes

    A status check is performed on the entire cluster and a list of
    servers that did not start, if any, are reported at the end.
    """
    if service(' start'):
        check_status_for_control_commands()


@task
@requires_topology
def stop():
    """
    Stop the Presto server on all nodes
    """
    service(' stop')


def stop_and_start():
    if check_presto_version() != '':
        return False
    sudo('set -m; ' + INIT_SCRIPTS + ' stop')
    if is_port_in_use(env.host):
        return False
    _LOGGER.info('Executing start on presto server')
    ret = sudo('set -m; ' + INIT_SCRIPTS + ' start')
    return ret.succeeded


@task
@requires_topology
def restart():
    """
    Restart the Presto server on all nodes.

    A status check is performed on the entire cluster and a list of
    servers that did not start, if any, are reported at the end.
    """
    if stop_and_start():
        check_status_for_control_commands()


def check_presto_version():
    """
    Checks that the Presto version is suitable.

    Returns:
        Error string if applicable
    """
    version = get_presto_version()
    if version in PRESTO_TD_RPM:
        return ''
    try:
        # remove -SNAPSHOT or .SNAPSHOT from the version string
        version = re.sub(r'[-\.]SNAPSHOT.*', '', version)
        float(version)
        version_number = version.strip().split('.')
        if int(version_number[1]) < PRESTO_RPM_MIN_REQUIRED_VERSION:
            incorrect_version_str = 'Presto version is %s, version >= 0.%d ' \
                                    'required.' % \
                                    (version, PRESTO_RPM_MIN_REQUIRED_VERSION)
            warn(incorrect_version_str)
            return incorrect_version_str
        return ''
    except ValueError:
        not_installed_str = 'Presto is not installed.'
        warn(not_installed_str)
        return not_installed_str


def get_presto_version():
    with settings(hide('warnings', 'stdout'), warn_only=True):
        version = run('rpm -q --qf \"%{VERSION}\\n\" presto')
        _LOGGER.debug('Presto rpm version: ' + version)
        return version


def check_server_status(client):
    """
    Checks if server is running for env.host. Retries connecting to server
    until server is up or till RETRY_TIMEOUT is reached

    Parameters:
        client - client that executes the query

    Returns:
        True or False
    """
    result = True
    time = 0
    while time < RETRY_TIMEOUT:
        result = client.execute_query(SERVER_CHECK_SQL)
        if not result:
            run('sleep %d' % SLEEP_INTERVAL)
            _LOGGER.debug('Status retrieval for the server failed after '
                          'waiting for %d seconds. Retrying...' % time)
            time += SLEEP_INTERVAL
        else:
            break
    return result


def run_sql(client, sql):
    status = client.execute_query(sql)
    if status:
        return client.get_rows()
    else:
        # TODO: Check if we can get some error cause from server response and
        # log that to the user
        _LOGGER.error('Querying server failed')
        return []


def execute_connector_info_sql(client):
    """
    Returns [[catalog_name], [catalog_2]..] from catalogs system table

    Parameters:
        client - client that executes the query
    """
    return run_sql(client, CONNECTOR_INFO_SQL)


def execute_external_ip_sql(client, uuid):
    """
    Returns external ip of the host with uuid after parsing the http_uri column
    from nodes system table

    Parameters:
        client - client that executes the query
        uuid - node_id of the node
    """
    return run_sql(client, EXTERNAL_IP_SQL % uuid)


def get_sysnode_info_from(node_info_row):
    """
    Returns system node info dict from node info row for a node

    Parameters:
        node_info_row -

    Returns:
        Node info dict in format:
        {'http://node1/statement': [presto-main:0.97-SNAPSHOT, True]}
    """
    output = {}
    for row in node_info_row:
        if row:
            output[row[0]] = [row[1], row[2]]

    _LOGGER.info('Node info: %s ', output)
    return output


def get_connector_info_from(client):
    """
    Returns installed connectors

    Parameters:
        client - client that executes the query

    Returns:
        comma delimited connectors eg: tpch, hive, system
    """
    syscatalog = []
    connector_info = execute_connector_info_sql(client)
    for conn_info in connector_info:
        if conn_info:
            syscatalog.append(conn_info[0])
    return ', '.join(syscatalog)


def get_server_status(client):
    """
    Check if the server is running for host.

    Parameters:
        client - client that executes the query

    Returns:
        True or False
    """
    result = client.execute_query(SERVER_CHECK_SQL)
    return result


def is_server_up(status):
    if status:
        return 'Running'
    else:
        return 'Not Running'


def get_roles_for(host):
    roles = []
    for role in ['coordinator', 'worker']:
        if host in env.roledefs[role]:
            roles.append(role)
    return roles


def print_node_info(node_status, connector_status):
    for k in node_status:
        print('\tNode URI(http): ' + str(k) +
              '\n\tPresto Version: ' + str(node_status[k][0]) +
              '\n\tNode is active: ' + str(node_status[k][1]))
        if connector_status:
            print('\tConnectors:     ' + connector_status)


def get_ext_ip_of_node(client):
    node_properties_file = os.path.join(constants.REMOTE_CONF_DIR,
                                        'node.properties')
    with settings(hide('stdout')):
        node_uuid = run('sed -n s/^node.id=//p ' + node_properties_file)
    external_ip_row = execute_external_ip_sql(client, node_uuid)
    external_ip = ''
    if len(external_ip_row) > 1:
        warn_more_than_one_ip = 'More than one external ip found for ' \
                                + env.host + '. There could be multiple nodes ' \
                                             'associated with the same node.id'
        _LOGGER.debug(warn_more_than_one_ip)
        warn(warn_more_than_one_ip)
        return external_ip
    for row in external_ip_row:
        if row:
            external_ip = row[0]
    if not external_ip:
        _LOGGER.debug('Cannot get external IP for ' + env.host)
        external_ip = 'Unknown'
    return external_ip


def print_status_header(external_ip, server_status):
    print('Server Status:')
    print('\t%s(IP: %s roles: %s): %s' % (env.host, external_ip,
                                          ', '.join(get_roles_for(env.host)),
                                          is_server_up(server_status)))


def get_status():
    client = PrestoClient(env.host, env.user)
    external_ip = get_ext_ip_of_node(client)

    server_status = get_server_status(client)
    print_status_header(external_ip, server_status)
    if server_status:
        # just get the node_info row for the host if server is up
        node_info_row = run_sql(client, NODE_INFO_PER_URI_SQL % external_ip)
        node_status = get_sysnode_info_from(node_info_row)
        if node_status:
            connector_status = get_connector_info_from(client)
            print_node_info(node_status, connector_status)
        else:
            print('\tNo information available')
    else:
        print('\tNo information available')


@task
@serial
@requires_topology
def status():
    """
    Print the status of presto in the cluster
    """
    with settings(hide('warnings')):
        presto_version_warning = check_presto_version()

    if not presto_version_warning:
        try:
            get_status()
        except ConfigurationError as e:
            print_status_header(external_ip='Unknown', server_status=None)
            print('\t' + e.message)
    else:
        print_status_header(external_ip='Unknown', server_status=None)
        print('\t' + presto_version_warning)
