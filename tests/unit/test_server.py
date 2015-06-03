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
from mock import patch, MagicMock

from prestoadmin.prestoclient import PrestoClient
from prestoadmin import server
from prestoadmin.server import INIT_SCRIPTS, SLEEP_INTERVAL, \
    PRESTO_RPM_MIN_REQUIRED_VERSION
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigFileNotFoundError
import tests.utils as utils


class TestInstall(utils.BaseTestCase):
    @patch('prestoadmin.server.deploy_install_configure')
    def test_install_server(self, mock_install):
        local_path = os.path.join("/any/path/rpm")
        server.install(local_path)
        mock_install.assert_called_with(local_path)

    @patch('prestoadmin.server.package.install')
    @patch('prestoadmin.server.execute')
    def test_deploy_install(self, mock_execute, mock_install):
        local_path = "/any/path/rpm"
        env.hosts = []
        server.deploy_install_configure(local_path)
        mock_install.assert_called_with(local_path)
        mock_execute.assert_called_with(server.update_configs, hosts=[])

    def test_fail_install(self):
        local_path = None
        self.assertRaisesRegexp(SystemExit,
                                "Missing argument local_path: Absolute path "
                                "to the presto rpm to be installed",
                                server.install,
                                local_path)

    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.sudo')
    def test_uninstall_is_called(self, mock_sudo, mock_version_check):
        env.host = "any_host"
        server.uninstall()
        mock_version_check.assert_called_with()
        mock_sudo.assert_called_with('rpm -e presto')

    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.sudo')
    @patch.object(PrestoClient, 'execute_query')
    @patch('prestoadmin.server.warn')
    @patch('prestoadmin.server.check_presto_version')
    def test_server_start_fail(self, mock_version_check, mock_warn,
                               mock_execute, mock_sudo, mock_run):
        old_retry_timeout = server.RETRY_TIMEOUT
        server.RETRY_TIMEOUT = 1
        mock_execute.return_value = False
        env.host = "failed_node1"
        mock_version_check.return_value = ''
        server.start()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        mock_warn.assert_called_with("Server failed to start on: failed_node1")
        server.RETRY_TIMEOUT = old_retry_timeout

    @patch('prestoadmin.server.sudo')
    @patch.object(PrestoClient, 'execute_query')
    @patch('prestoadmin.server.check_presto_version')
    def test_server_start(self, mock_version_check, mock_execute,
                          mock_sudo):
        mock_execute.return_value = True
        env.host = 'good_node'
        mock_version_check.return_value = ''
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
    def test_server_start_bad_presto_version(self, mock_version_check,
                                             mock_sudo):
        env.host = "good_node"
        mock_version_check.return_value = 'Presto not installed'
        server.start()
        mock_version_check.assert_called_with()
        self.assertEqual(False, mock_sudo.called)

    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.sudo')
    def test_server_stop(self, mock_sudo, mock_version_check):
        mock_version_check.return_value = ''
        server.stop()
        mock_version_check.assert_called_with()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' stop')

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.warn')
    @patch('prestoadmin.server.check_presto_version')
    def test_server_restart_fail(self, mock_version_check, mock_warn,
                                 mock_status, mock_sudo):
        mock_status.return_value = False
        env.host = "failed_node1"
        mock_version_check.return_value = ''
        server.restart()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' restart')
        mock_version_check.assert_called_with()
        mock_warn.assert_called_with("Server failed to start on: failed_node1")

    @patch.object(PrestoClient, 'lookup_port')
    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.check_presto_version')
    def test_server_restart(self, mock_version_check, mock_status, mock_sudo,
                            lookup_host_mock):
        mock_status.return_value = True
        env.host = 'good_node'
        mock_version_check.return_value = ''
        server.restart()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' restart')
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
        e = ConfigFileNotFoundError
        mock_connector.add = e
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

    @patch('prestoadmin.server.get_presto_version')
    @patch("prestoadmin.server.execute_connector_info_sql")
    @patch("prestoadmin.server.get_server_status")
    @patch("prestoadmin.server.get_ext_ip_of_node")
    @patch("prestoadmin.server.run_sql")
    @patch('prestoadmin.server.run')
    def test_status_from_each_node(self, mock_run, mock_nodeinfo, mock_ext_ip,
                                   mock_server_up, mock_conninfo,
                                   mock_version):
        env.roledefs = {
            'coordinator': ["Node1"],
            'worker': ["Node1", "Node2", "Node3", "Node4"],
            'all': ["Node1", "Node2", "Node3", "Node4"]
        }
        mock_ext_ip.side_effect = ["IP1", "IP2", "IP3", ""]
        mock_server_up.side_effect = [True, True, True, False]
        mock_nodeinfo.side_effect = [
            [['http://active/statement', 'presto-main:0.97-SNAPSHOT', True]],
            [['http://inactive/stmt', 'presto-main:0.99-SNAPSHOT', False]],
            [[]],
            [['http://servrdown/statement', 'any', True]]
        ]
        mock_conninfo.side_effect = [[['hive'], ['system'], ['tpch']],
                                     [[]],
                                     [['any']],
                                     [['any']]]

        mock_version.return_value = '0.101'

        env.host = "Node1"
        server.status()
        env.host = "Node2"
        server.status()
        env.host = "Node3"
        server.status()
        env.host = "Node4"
        server.status()

        expected = self.read_file_output('/files/server_status_out.txt')
        self.assertEqual(sorted(expected), sorted(self.test_stdout.getvalue()))

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

    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.warn')
    def test_warning_presto_version(self, mock_warn, mock_run):
        env.host = 'node1'
        env.roledefs['coordinator'] = ['node1']
        env.roledefs['worker'] = ['node1']

        old_version = '0.97'
        mock_run.return_value = old_version
        server.status()
        version_warning = 'Presto version is %s, version >= 0.%d required.'\
                          % (old_version, PRESTO_RPM_MIN_REQUIRED_VERSION)
        mock_warn.assert_called_with(version_warning)

        mock_run.return_value = 'No presto installed'
        env.host = 'node1'
        server.status()
        installation_warning = 'Presto is not installed.'
        mock_warn.assert_called_with(installation_warning)

        self.assertEqual(
            'Server Status:\n\tnode1(IP: Unknown roles: coordinator, worker):'
            ' Not Running\n\t' + version_warning + '\nServer Status:\n\t'
            'node1(IP: Unknown roles: coordinator, worker): Not Running\n\t'
            + installation_warning + '\n',
            self.test_stdout.getvalue()
        )

    @patch('prestoadmin.server.run')
    def test_td_presto_version(self,  mock_run):
        td_version = '101t'
        mock_run.return_value = td_version
        expected = server.check_presto_version()
        self.assertEqual(expected, '')
