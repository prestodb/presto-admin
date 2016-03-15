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
Tests the presto install
"""
import os

from fabric.api import env
from fabric.operations import _AttributeString
from mock import patch, call, MagicMock

from prestoadmin import server
from prestoadmin.util.fabricapi import get_host_list
from prestoadmin.prestoclient import PrestoClient
from prestoadmin.server import INIT_SCRIPTS, SLEEP_INTERVAL, \
    PRESTO_RPM_MIN_REQUIRED_VERSION
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigFileNotFoundError, \
    ConfigurationError

from tests.unit.base_unit_case import BaseUnitCase


class TestInstall(BaseUnitCase):
    SERVER_FAIL_MSG = 'Server failed to start on: failed_node1' \
                      '\nPlease check ' \
                      + constants.DEFAULT_PRESTO_SERVER_LOG_FILE + ' and ' + \
                      constants.DEFAULT_PRESTO_LAUNCHER_LOG_FILE

    def setUp(self):
        self.remove_runs_once_flag(server.status)
        self.maxDiff = None
        super(TestInstall, self).setUp(capture_output=True)

    @patch('prestoadmin.server.package.check_if_valid_rpm')
    @patch('prestoadmin.server.execute')
    def test_install_server(self, mock_execute, mock_check_rpm):
        local_path = "/any/path/rpm"
        server.install(local_path)
        mock_check_rpm.assert_called_with(local_path)
        mock_execute.assert_called_with(server.deploy_install_configure,
                                        local_path, hosts=get_host_list())

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.package.deploy_install')
    @patch('prestoadmin.server.update_configs')
    def test_deploy_install_configure(self, mock_update, mock_install,
                                      mock_sudo):
        local_path = "/any/path/rpm"
        mock_sudo.side_effect = self.mock_fail_then_succeed()

        server.deploy_install_configure(local_path)
        mock_install.assert_called_with(local_path)
        self.assertTrue(mock_update.called)
        mock_sudo.assert_called_with('getent passwd presto', quiet=True)

    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.sudo')
    def test_uninstall_is_called(self, mock_sudo, mock_version_check):
        env.host = "any_host"
        mock_sudo.side_effect = self.mock_fail_then_succeed()

        server.uninstall()

        mock_version_check.assert_called_with()
        mock_sudo.assert_any_call('rpm -e presto')
        mock_sudo.assert_called_with('rpm -e presto-server-rpm')

    @patch('prestoadmin.util.remote_config_util.lookup_in_config')
    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.sudo')
    @patch.object(PrestoClient, 'execute_query')
    @patch('prestoadmin.server.error')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_start_fail(self, mock_port_in_use,
                               mock_version_check, mock_error,
                               mock_execute, mock_sudo, mock_run, mock_config):
        old_retry_timeout = server.RETRY_TIMEOUT
        server.RETRY_TIMEOUT = 1
        mock_execute.return_value = False
        env.host = "failed_node1"
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 0
        mock_config.return_value = None
        server.start()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        mock_error.assert_called_with(self.SERVER_FAIL_MSG)
        server.RETRY_TIMEOUT = old_retry_timeout

    @patch('prestoadmin.server.sudo')
    @patch.object(PrestoClient, 'execute_query')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_start(self, mock_port_in_use, mock_version_check,
                          mock_execute, mock_sudo):
        mock_execute.return_value = True
        env.host = 'good_node'
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 0
        server.start()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        self.assertEqual('Waiting to make sure we can connect to the Presto '
                         'server on good_node, please wait. This check will '
                         'time out after 2 minutes if the server does not '
                         'respond.\nServer started successfully on: '
                         'good_node\n', self.test_stdout.getvalue())

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_start_bad_presto_version(self, mock_port_in_use,
                                             mock_version_check, mock_sudo):
        env.host = "good_node"
        mock_version_check.return_value = 'Presto not installed'
        server.start()
        mock_version_check.assert_called_with()
        self.assertEqual(False, mock_sudo.called)

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_start_port_in_use(self, mock_port_in_use,
                                      mock_version_check, mock_sudo):
        env.host = "good_node"
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 1
        server.start()
        mock_version_check.assert_called_with()
        mock_port_in_use.assert_called_with('good_node')
        self.assertEqual(False, mock_sudo.called)

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_status_for_control_commands')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_restart_port_in_use(self, mock_port_in_use,
                                        mock_version_check, mock_check_status,
                                        mock_sudo):
        env.host = "good_node"
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 1
        server.restart()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' stop')
        mock_version_check.assert_called_with()
        self.assertEqual(False, mock_check_status.called)

    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    @patch('prestoadmin.server.sudo')
    def test_server_stop(self, mock_sudo, mock_port_in_use,
                         mock_version_check):
        mock_version_check.return_value = ''
        server.stop()
        mock_version_check.assert_called_with()
        self.assertEqual(False, mock_port_in_use.called)
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' stop')

    @patch('prestoadmin.util.remote_config_util.lookup_in_config')
    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.error')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_restart_fail(self, mock_port_in_use, mock_version_check,
                                 mock_error, mock_status, mock_sudo,
                                 mock_config):
        mock_status.return_value = False
        mock_config.return_value = None
        env.host = "failed_node1"
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 0
        server.restart()
        mock_sudo.assert_any_call('set -m; ' + INIT_SCRIPTS + ' stop')
        mock_sudo.assert_any_call('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()

        mock_error.assert_called_with(self.SERVER_FAIL_MSG)

    @patch('prestoadmin.util.remote_config_util.lookup_port')
    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_restart(self, mock_port_in_use, mock_version_check,
                            mock_status, mock_sudo, mock_lookup_host):
        mock_status.return_value = True
        env.host = 'good_node'
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 0
        server.restart()
        mock_sudo.assert_any_call('set -m; ' + INIT_SCRIPTS + ' stop')
        mock_sudo.assert_any_call('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        self.assertEqual('Waiting to make sure we can connect to the Presto '
                         'server on good_node, please wait. This check will '
                         'time out after 2 minutes if the server does not '
                         'respond.\nServer started successfully on: '
                         'good_node\n', self.test_stdout.getvalue())

    @patch('prestoadmin.server.connector')
    @patch('prestoadmin.server.configure_cmds.deploy')
    @patch('prestoadmin.server.os.path.exists')
    @patch('prestoadmin.server.os.makedirs')
    @patch('prestoadmin.server.util.filesystem.os.fdopen')
    @patch('prestoadmin.server.util.filesystem.os.open')
    def test_update_config(self, mock_open, mock_fdopen, mock_makedir,
                           mock_path_exists, mock_config, mock_connector):
        e = ConfigFileNotFoundError(
            message='problems', config_path='config_path')
        mock_connector.add.side_effect = e
        mock_path_exists.side_effect = [False, False]

        server.update_configs()

        mock_config.assert_called_with()
        mock_makedir.assert_called_with(constants.CONNECTORS_DIR)
        mock_open.assert_called_with(os.path.join(constants.CONNECTORS_DIR,
                                                  'tpch.properties'),
                                     os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        file_manager = mock_fdopen.return_value.__enter__.return_value
        file_manager.write.assert_called_with("connector.name=tpch")

    @patch('prestoadmin.server.run')
    def test_check_success_status(self, mock_sleep):
        client_mock = MagicMock(PrestoClient)
        client_mock.execute_query.side_effect = [False, True]
        self.assertEqual(server.check_server_status(client_mock), True)

    @patch('prestoadmin.server.run')
    def test_check_success_fail(self,  mock_sleep):
        old_retry_timeout = server.RETRY_TIMEOUT
        server.RETRY_TIMEOUT = SLEEP_INTERVAL + 1
        client_mock = MagicMock(PrestoClient)
        client_mock.execute_query.side_effect = [False, False]
        self.assertEqual(server.check_server_status(client_mock), False)
        server.RETRY_TIMEOUT = old_retry_timeout

    @patch('prestoadmin.server.execute')
    @patch('prestoadmin.server.run_sql')
    @patch('prestoadmin.server.get_presto_version')
    def test_status_from_each_node(
            self, mock_get_presto_version, mock_run_sql, mock_execute):
        env.roledefs = {
            'coordinator': ['Node1'],
            'worker': ['Node1', 'Node2', 'Node3', 'Node4'],
            'all': ['Node1', 'Node2', 'Node3', 'Node4']
        }
        env.hosts = env.roledefs['all']

        mock_get_presto_version.return_value = '0.97-SNAPSHOT'
        mock_run_sql.side_effect = [
            [['select * from system.runtime.nodes']],
            [['hive'], ['system'], ['tpch']],
            [['http://active/statement', 'presto-main:0.97-SNAPSHOT', True]],
            [['http://inactive/stmt', 'presto-main:0.99-SNAPSHOT', False]],
            [[]],
            [['http://servrdown/statement', 'any', True]]
        ]
        mock_execute.side_effect = [{
            'Node1': ('IP1', True, ''),
            'Node2': ('IP2', True, ''),
            'Node3': ('IP3', True, ''),
            'Node4': Exception('Timed out trying to connect to Node4')
        }]
        env.host = 'Node1'
        server.status()

        expected = self.read_file_output('/resources/server_status_out.txt')
        self.assertEqual(
            expected.splitlines(),
            self.test_stdout.getvalue().splitlines()
        )

    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.service')
    @patch('prestoadmin.server.get_ext_ip_of_node')
    def test_collect_node_information(self, mock_ext_ip, mock_service,
                                      mock_version):
        env.roledefs = {
            'coordinator': ['Node1'],
            'all': ['Node1']
        }
        mock_ext_ip.side_effect = ['IP1', 'IP3', 'IP4']
        mock_service.side_effect = [True, False, Exception('Not running')]
        mock_version.side_effect = ['', 'Presto not installed', '', '']

        self.assertEqual(('IP1', True, ''), server.collect_node_information())
        self.assertEqual(('Unknown', False, 'Presto not installed'),
                         server.collect_node_information())
        self.assertEqual(('IP3', False, ''), server.collect_node_information())
        self.assertEqual(('IP4', False, ''),
                         server.collect_node_information())

    @patch('prestoadmin.server.run')
    def test_get_external_ip(self, mock_nodeuuid):
        client_mock = MagicMock(PrestoClient)
        client_mock.execute_query.return_value = True
        client_mock.get_rows = lambda: [['IP']]
        self.assertEqual(server.get_ext_ip_of_node(client_mock), 'IP')

    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.warn')
    def test_warn_external_ip(self, mock_warn, mock_nodeuuid):
        env.host = 'node'
        client_mock = MagicMock(PrestoClient)
        client_mock.execute_query.return_value = True
        client_mock.get_rows = lambda: [['IP1'], ['IP2']]
        server.get_ext_ip_of_node(client_mock)
        mock_warn.assert_called_with("More than one external ip found for "
                                     "node. There could be multiple nodes "
                                     "associated with the same node.id")

    def read_file_output(self, filename):
        dir = os.path.abspath(os.path.dirname(__file__))
        result_file = open(dir + filename, 'r')
        file_content = "".join(result_file.readlines())
        result_file.close()
        return file_content

    @patch('prestoadmin.server.run_sql')
    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.warn')
    def test_warning_presto_version_wrong(self, mock_warn, mock_run,
                                          mock_run_sql):
        env.host = 'node1'
        env.roledefs['coordinator'] = ['node1']
        env.roledefs['worker'] = ['node1']
        env.roledefs['all'] = ['node1']
        env.hosts = env.roledefs['all']

        old_version = '0.97'
        output = _AttributeString(old_version)
        output.succeeded = True
        mock_run.return_value = output
        server.collect_node_information()
        version_warning = 'Presto version is %s, version >= 0.%d required.'\
                          % (old_version, PRESTO_RPM_MIN_REQUIRED_VERSION)
        mock_warn.assert_called_with(version_warning)

    @patch('prestoadmin.server.run_sql')
    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.warn')
    def test_warning_presto_version_not_installed(self, mock_warn, mock_run,
                                                  mock_run_sql):
        env.host = 'node1'
        env.roledefs['coordinator'] = ['node1']
        env.roledefs['worker'] = ['node1']
        env.roledefs['all'] = ['node1']
        env.hosts = env.roledefs['all']
        output = _AttributeString('package presto is not installed')
        output.succeeded = False
        mock_run.return_value = output
        env.host = 'node1'
        server.collect_node_information()
        installation_warning = 'Presto is not installed.'
        mock_warn.assert_called_with(installation_warning)

    @patch('prestoadmin.server.run')
    def test_td_presto_version(self,  mock_run):
        td_version = '101t'
        output = _AttributeString(td_version)
        output.succeeded = True
        mock_run.return_value = output
        expected = server.check_presto_version()
        self.assertEqual(expected, '')

    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.lookup_port')
    @patch('prestoadmin.server.error')
    def test_fail_if_port_is_in_use(self, mock_error, mock_port, mock_run):
        mock_port.return_value = 1010
        env.host = 'any_host'
        mock_run.return_value = 'some_string'
        server.is_port_in_use(env.host)
        mock_error.assert_called_with('Server failed to start on any_host. '
                                      'Port 1010 already in use')

    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.lookup_port')
    @patch('prestoadmin.server.warn')
    def test_no_warn_if_port_free(self, mock_warn, mock_port, mock_run):
        mock_port.return_value = 1010
        env.host = 'any_host'
        mock_run.return_value = ''
        server.is_port_in_use(env.host)
        self.assertEqual(False, mock_warn.called)

    @patch('prestoadmin.server.lookup_port')
    @patch('prestoadmin.server.warn')
    def test_no_warn_if_port_lookup_fail(self, mock_warn, mock_port):
        e = ConfigurationError()
        mock_port.side_effect = e
        env.host = 'any_host'
        self.assertFalse(server.is_port_in_use(env.host))
        self.assertEqual(False, mock_warn.called)

    @patch('prestoadmin.server.run')
    def test_version_with_snapshot(self, mock_run):
        snapshot_version = '0.107.SNAPSHOT'
        output = _AttributeString(snapshot_version)
        output.succeeded = True
        mock_run.return_value = output

        expected = server.check_presto_version()
        self.assertEqual(expected, '')

        snapshot_version = '0.107.SNAPSHOT-1.x86_64'
        output = _AttributeString(snapshot_version)
        output.succeeded = True
        mock_run.return_value = output
        expected = server.check_presto_version()
        self.assertEqual(expected, '')

        snapshot_version = '0.107-SNAPSHOT'
        output = _AttributeString(snapshot_version)
        output.succeeded = True
        mock_run.return_value = output
        expected = server.check_presto_version()
        self.assertEqual(expected, '')

    @patch('prestoadmin.server.run')
    def test_multiple_version_rpms(self, mock_run):

        output1 = _AttributeString('package presto is not installed')
        output1.succeeded = False
        output2 = _AttributeString('presto-server-rpm-0.115t-1.x86_64')
        output2.succeeded = True
        output3 = _AttributeString('Presto is not installed.')
        output3.succeeded = False
        output4 = _AttributeString('0.111.SNAPSHOT')
        output4.succeeded = True

        mock_run.side_effect = [output1, output2, output3, output4]

        expected = server.check_presto_version()
        mock_run.assert_has_calls([
            call('rpm -q presto'),
            call('rpm -q presto-server-rpm'),
            call('rpm -q --qf \"%{VERSION}\\n\" presto'),
            call('rpm -q --qf \"%{VERSION}\\n\" presto-server-rpm'),
        ])
        self.assertEqual(expected, '')

    def mock_fail_then_succeed(self):
        output1 = _AttributeString()
        output1.succeeded = False
        output2 = _AttributeString()
        output2.succeeded = True
        return [output1, output2]
