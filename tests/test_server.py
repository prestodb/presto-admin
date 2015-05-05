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

from mock import patch
from prestoadmin.prestoclient import PrestoClient
from prestoadmin.server import INIT_SCRIPTS, SLEEP_INTERVAL, \
    PRESTO_RPM_VERSION

from prestoadmin import server
from prestoadmin.config import ConfigurationError, \
    ConfigFileNotFoundError
from prestoadmin.server import deploy_install_configure
import utils


class TestInstall(utils.BaseTestCase):
    @patch('prestoadmin.server.execute_fail_on_error')
    def test_install_server(self, mock_execute):
        local_path = os.path.join("/any/path/rpm")
        server.install(local_path)
        mock_execute.assert_called_with(deploy_install_configure,
                                        local_path, hosts=[])

    @patch('prestoadmin.server.update_configs')
    @patch('prestoadmin.server.package.install')
    def test_deploy_install(self, mock_install, mock_update):
        local_path = "/any/path/rpm"
        server.deploy_install_configure(local_path)
        mock_install.assert_called_with(local_path)
        mock_update.assert_called_with()

    def test_fail_install(self):
        local_path = None
        self.assertRaisesRegexp(SystemExit,
                                "Missing argument local_path: Absolute path "
                                "to local rpm to be deployed",
                                server.install,
                                local_path)

    @patch('prestoadmin.topology.set_conf_interactive')
    @patch('prestoadmin.topology.get_conf')
    def test_interactive_install(self,  get_conf_mock,
                                 mock_set_interactive):
        env.topology_config_not_found = ConfigurationError()
        get_conf_mock.return_value = {'username': 'bob', 'port': '225',
                                      'coordinator': 'master',
                                      'workers': ['slave1', 'slave2']}
        server.set_hosts()
        self.assertEqual(server.env.user, 'bob'),
        self.assertEqual(server.env.port, '225')
        self.assertEqual(server.env.hosts, ['master', 'slave1', 'slave2'])
        self.assertEqual(server.env.roledefs['all'],
                         ['master', 'slave1', 'slave2'])
        self.assertEqual(server.env.roledefs['coordinator'], ['master'])
        self.assertEqual(server.env.roledefs['worker'], ['slave1', 'slave2'])

    def test_set_host_with_exclude(self):
        env.hosts = ['a', 'b', 'bad']
        env.exclude_hosts = ['bad']
        self.assertEqual(server.set_hosts(), ['a', 'b'])

    @patch('prestoadmin.server.sudo')
    def test_uninstall_is_called(self, mock_sudo):
        server.uninstall()
        mock_sudo.assert_called_with('rpm -e presto')

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.warn')
    @patch('prestoadmin.server.check_presto_version')
    def test_server_start_fail(self, mock_version_check, mock_warn,
                               mock_status, mock_sudo):
        mock_status.return_value = False
        env.host = "failed_node1"
        server.start()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        mock_warn.assert_called_with("Server failed to start on: failed_node1")

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.check_presto_version')
    def test_server_start(self, mock_version_check, mock_status,
                          mock_sudo):
        mock_status.return_value = True
        env.host = "good_node"
        server.start()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        self.assertEqual("Server started successfully on: good_node\n",
                         self.test_stdout.getvalue())

    @patch('prestoadmin.server.sudo')
    def test_server_stop(self, mock_sudo):
        server.stop()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' stop')

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.warn')
    @patch('prestoadmin.server.check_presto_version')
    def test_server_restart_fail(self, mock_version_check, mock_warn,
                                 mock_status, mock_sudo):
        mock_status.return_value = False
        env.host = "failed_node1"
        server.restart()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' restart')
        mock_version_check.assert_called_with()
        mock_warn.assert_called_with("Server failed to start on: failed_node1")

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.check_presto_version')
    def test_server_restart(self, mock_version_check, mock_status, mock_sudo):
        mock_status.return_value = True
        env.host = "good_node"
        server.restart()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' restart')
        mock_version_check.assert_called_with()
        self.assertEqual("Server started successfully on: good_node\n",
                         self.test_stdout.getvalue())

    @patch('prestoadmin.server.connector')
    @patch('prestoadmin.server.configure_cmds.deploy')
    def test_update_config(self, mock_config, mock_connector):
        e = ConfigFileNotFoundError
        mock_connector.add = e
        server.update_configs()
        mock_config.assert_called_with()

    @patch.object(PrestoClient, 'execute_query')
    @patch('prestoadmin.server.run')
    def test_check_success_status(self, mock_sleep, mock_execute_query):
        mock_execute_query.side_effect = [False, True]
        self.assertEqual(server.check_server_status(), True)

    @patch.object(PrestoClient, 'execute_query')
    @patch('prestoadmin.server.run')
    def test_check_success_fail(self,  mock_sleep,
                                mock_execute_query):
        server.RETRY_TIMEOUT = SLEEP_INTERVAL + 1
        mock_execute_query.side_effect = [False, False]
        self.assertEqual(server.check_server_status(), False)

    @patch("prestoadmin.server.get_status_info")
    @patch("prestoadmin.server.get_connector_info")
    def test_server_status(self, mock_conn, mock_status):
        mock_status.side_effect = [
            [['http://node1/statement', 'presto-main:0.97-SNAPSHOT', True],
             ['http://node12/statement', 'presto-main:0.100-SNAPSHOT', True]],
            [['http://node2/statement', 'presto-main:0.97-SNAPSHOT', True],
                []],
            [['http://down/statement', 'presto-main:0.97-SNAPSHOT', False]],
            [[]]]
        mock_conn.side_effect = [
            [['hive'], ['system'], ['tpch']],
            [[]],
            [['system'], []],
            [[]]]
        server.status_show()
        server.status_show()
        server.status_show()
        server.status_show()
        expected = self.read_file_output('/files/valid_server'
                                         '_status.txt')
        self.assertEqual(sorted(expected), sorted(self.test_stdout.getvalue()))

    def read_file_output(self, filename):
        dir = os.path.abspath(os.path.dirname(__file__))
        result_file = open(dir + filename, 'r')
        file_content = "".join(result_file.readlines())
        result_file.close()
        return file_content

    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.warn')
    def test_warning_presto_version(self, mock_warn, mock_run):
        mock_run.return_value = '0.97'
        env.host = 'node1'
        server.check_presto_version()
        mock_warn.assert_called_with("node1: Status check requires "
                                     "Presto version >= 0.%d"
                                     % PRESTO_RPM_VERSION)

        mock_run.return_value = 'No presto installed'
        env.host = 'node1'
        server.check_presto_version()
        mock_warn.assert_called_with("node1: No suitable presto version found")
