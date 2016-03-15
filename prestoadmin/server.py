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
import sys

from fabric.api import task, sudo, env, quiet
from fabric.context_managers import settings, hide
from fabric.decorators import runs_once, with_settings, parallel
from fabric.operations import run, os
from fabric.tasks import execute
from fabric.utils import warn, error
from retrying import retry

from prestoadmin import configure_cmds
from prestoadmin import connector
from prestoadmin import package
from prestoadmin.util.version_util import VersionRange, VersionRangeList, \
    split_version, strip_tag
from prestoadmin.prestoclient import PrestoClient
from prestoadmin.standalone.config import StandaloneConfig
from prestoadmin.util.base_config import requires_config
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigFileNotFoundError
from prestoadmin.util.fabricapi import get_host_list, get_coordinator_role
from prestoadmin.util.remote_config_util import lookup_port, \
    lookup_server_log_file, lookup_launcher_log_file

from tempfile import mkdtemp
import util.filesystem

__all__ = ['install', 'uninstall', 'upgrade', 'start', 'stop', 'restart',
           'status']

INIT_SCRIPTS = '/etc/init.d/presto'
RETRY_TIMEOUT = 120
SLEEP_INTERVAL = 10
SYSTEM_RUNTIME_NODES = 'select * from system.runtime.nodes'


def old_sysnode_processor(node_info_rows):
    def old_transform(node_is_active):
        return 'active' if node_is_active else 'inactive'
    return get_sysnode_info_from(node_info_rows, old_transform)


def new_sysnode_processor(node_info_rows):
    return get_sysnode_info_from(node_info_rows, lambda x: x)


NODE_INFO_PER_URI_SQL = VersionRangeList(
    VersionRange((0, 0), (0, 128),
                 ('select http_uri, node_version, active from '
                  'system.runtime.nodes where '
                  'url_extract_host(http_uri) = \'%s\'',
                 old_sysnode_processor)),
    VersionRange((0, 128), (sys.maxsize,),
                 ('select http_uri, node_version, state from '
                  'system.runtime.nodes where '
                  'url_extract_host(http_uri) = \'%s\'',
                 new_sysnode_processor))
)

EXTERNAL_IP_SQL = 'select url_extract_host(http_uri) from ' \
                  'system.runtime.nodes WHERE node_id = \'%s\''
CONNECTOR_INFO_SQL = 'select catalog_name from system.metadata.catalogs'
PRESTO_RPM_MIN_REQUIRED_VERSION = 103
PRESTO_TD_RPM = ['101t']
_LOGGER = logging.getLogger(__name__)


@task
@runs_once
@requires_config(StandaloneConfig)
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
    package.check_if_valid_rpm(local_path)
    return execute(deploy_install_configure, local_path, hosts=get_host_list())


def deploy_install_configure(local_path):
    package.deploy_install(local_path)
    update_configs()
    wait_for_presto_user()


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


@retry(stop_max_delay=3000, wait_fixed=250)
def wait_for_presto_user():
    ret = sudo('getent passwd presto', quiet=True)
    if not ret.succeeded:
        raise Exception('Presto package was not installed successfully. '
                        'Presto user was not created.')


@task
@requires_config(StandaloneConfig)
def uninstall():
    """
    Uninstall Presto after stopping the services on all nodes
    """
    stop()

    # currently we have two rpm names out so we need this retry
    with quiet():
        ret = sudo('rpm -e presto')
        if ret.succeeded:
            print('Package uninstalled successfully on: ' + env.host)
            return

    ret = sudo('rpm -e presto-server-rpm')
    if ret.succeeded:
        print('Package uninstalled successfully on: ' + env.host)


@task
@requires_config(StandaloneConfig)
def upgrade(local_package_path, local_config_dir=None):
    """
    Copy and upgrade a new presto-server rpm to all of the nodes in the
    cluster. Retains existing node configuration.

    The existing topology information is read from the config.json file.
    Unlike install, there is no provision to supply topology information
    interactively.

    The existing cluster configuration is collected from the nodes on the
    cluster and stored on the host running presto-admin. After the
    presto-server packages have been upgraded, presto-admin pushes the
    collected configuration back out to the hosts on the cluster.

    Note that the configuration files in /etc/opt/prestoadmin are not updated
    during upgrade.

    :param local_package_path - Absolute path to the presto rpm to be
                                installed
    :param local_config_dir -   (optional) Directory to store the cluster
                                configuration in. If not specified, a temp
                                directory is used.
    """
    stop()

    if not local_config_dir:
        local_config_dir = mkdtemp()
        print('Saving cluster configuration to %s' % local_config_dir)

    configure_cmds.gather_directory(local_config_dir)
    filenames = connector.gather_connectors(local_config_dir)

    package.deploy_upgrade(local_package_path)

    configure_cmds.deploy_all(local_config_dir)
    connector.deploy_files(
        filenames,
        os.path.join(local_config_dir, env.host, 'catalog'),
        constants.REMOTE_CATALOG_DIR
    )


def service(control=None):
    if check_presto_version() != '':
        return False
    if control == 'start' and is_port_in_use(env.host):
        return False
    _LOGGER.info('Executing %s on presto server' % control)
    ret = sudo('set -m; ' + INIT_SCRIPTS + ' ' + control)
    return ret.succeeded


def check_status_for_control_commands():
    client = PrestoClient(env.host, env.user)
    print('Waiting to make sure we can connect to the Presto server on %s, '
          'please wait. This check will time out after %d minutes if the '
          'server does not respond.'
          % (env.host, (RETRY_TIMEOUT / 60)))
    if check_server_status(client):
        print('Server started successfully on: ' + env.host)
    else:
        error('Server failed to start on: ' + env.host +
              '\nPlease check ' + lookup_server_log_file(env.host) + ' and ' +
              lookup_launcher_log_file(env.host))


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
        error('Server failed to start on %s. Port %s already in use'
              % (env.host, str(portnum)))
    return output


@task
@requires_config(StandaloneConfig)
def start():
    """
    Start the Presto server on all nodes

    A status check is performed on the entire cluster and a list of
    servers that did not start, if any, are reported at the end.
    """
    if service('start'):
        check_status_for_control_commands()


@task
@requires_config(StandaloneConfig)
def stop():
    """
    Stop the Presto server on all nodes
    """
    service('stop')


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
@requires_config(StandaloneConfig)
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
    if not presto_installed():
        not_installed_str = 'Presto is not installed.'
        warn(not_installed_str)
        return not_installed_str

    version = get_presto_version()
    if version in PRESTO_TD_RPM:
        return ''

    matched = re.search('(\d+)\.(\d+)t?([-\.]SNAPSHOT)?', version)
    if not matched:
        incorrect_version_str = 'Incorrect presto version:  %s,' % version
        warn(incorrect_version_str)
        return incorrect_version_str

    minor_version = matched.group(2)

    if int(minor_version) < PRESTO_RPM_MIN_REQUIRED_VERSION:
        incorrect_version_str = 'Presto version is %s, version >= 0.%d ' \
                                'required.' % \
                                (version, PRESTO_RPM_MIN_REQUIRED_VERSION)
        warn(incorrect_version_str)
        return incorrect_version_str

    return ''


def presto_installed():
    with settings(hide('warnings', 'stdout'), warn_only=True):
        package_search = run('rpm -q presto')
        if not package_search.succeeded:
            package_search = run('rpm -q presto-server-rpm')
        return package_search.succeeded


def get_presto_version():
    with settings(hide('warnings', 'stdout'), warn_only=True):
        version = run('rpm -q --qf \"%{VERSION}\\n\" presto')
        # currently we have two rpm names out so we need this retry
        if not version.succeeded:
            version = run('rpm -q --qf \"%{VERSION}\\n\" presto-server-rpm')
        version = version.strip()
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
        result = client.execute_query(SYSTEM_RUNTIME_NODES)
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


def get_sysnode_info_from(node_info_row, state_transform):
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
            output[row[0]] = [row[1], state_transform(row[2])]

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
              '\n\tNode status:    ' + str(node_status[k][1]))
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
                                + env.host + '. There could be multiple ' \
                                'nodes associated with the same node.id'
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


def print_status_header(external_ip, server_status, host):
    print('Server Status:')
    print('\t%s(IP: %s, Roles: %s): %s' % (host, external_ip,
                                           ', '.join(get_roles_for(host)),
                                           is_server_up(server_status)))


@parallel
def collect_node_information():
    client = PrestoClient(get_coordinator_role()[0], env.user)
    with settings(hide('warnings')):
        error_message = check_presto_version()
    if error_message:
        external_ip = 'Unknown'
        is_running = False
    else:
        with settings(hide('warnings', 'aborts', 'stdout')):
            try:
                external_ip = get_ext_ip_of_node(client)
            except:
                external_ip = 'Unknown'
            try:
                is_running = service('status')
            except:
                is_running = False
    return external_ip, is_running, error_message


def get_status_from_coordinator():
    client = PrestoClient(get_coordinator_role()[0], env.user)
    try:
        coordinator_status = run_sql(client, SYSTEM_RUNTIME_NODES)
        connector_status = get_connector_info_from(client)
    except BaseException as e:
        # Just log errors that come from a missing port or anything else; if
        # we can't connect to the coordinator, we just want to print out a
        # minimal status anyway.
        _LOGGER.warn(e.message)
        coordinator_status = []
        connector_status = []

    with settings(hide('running')):
        node_information = execute(collect_node_information,
                                   hosts=get_host_list())

    for host in get_host_list():
        if isinstance(node_information[host], Exception):
            external_ip = 'Unknown'
            is_running = False
            error_message = node_information[host].message
        else:
            (external_ip, is_running, error_message) = node_information[host]

        print_status_header(external_ip, is_running, host)
        if error_message:
            print('\t' + error_message)
        elif not coordinator_status:
            print('\tNo information available: unable to query coordinator')
        elif not is_running:
            print('\tNo information available')
        else:
            version_string = get_presto_version()
            version = strip_tag(split_version(version_string))
            query, processor = NODE_INFO_PER_URI_SQL.for_version(version)
            # just get the node_info row for the host if server is up
            node_info_row = run_sql(client, query % external_ip)
            node_status = processor(node_info_row)
            if node_status:
                print_node_info(node_status, connector_status)
            else:
                print('\tNo information available: the coordinator has not yet'
                      ' discovered this node')


@task
@runs_once
@requires_config(StandaloneConfig)
@with_settings(hide('warnings'))
def status():
    """
    Print the status of presto in the cluster
    """
    get_status_from_coordinator()
