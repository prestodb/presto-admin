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
from prestoadmin.server import INIT_SCRIPTS, SLEEP_INTERVAL

from prestoadmin import server
from prestoadmin.configuration import ConfigurationError, \
    ConfigFileNotFoundError
from prestoadmin.server import PRESTO_ADMIN_PACKAGES_PATH,\
    deploy_install_configure
import utils


class TestInstall(utils.BaseTestCase):
    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.put')
    def test_deploy_is_called(self, mock_put, mock_sudo):
        server.deploy_package("/any/path/rpm")
        mock_sudo.assert_called_with("mkdir -p " + PRESTO_ADMIN_PACKAGES_PATH)
        mock_put.assert_called_with("/any/path/rpm",
                                    PRESTO_ADMIN_PACKAGES_PATH,
                                    use_sudo=True)

    @patch('prestoadmin.server.execute_fail_on_error')
    def test_install_server(self, mock_execute):
        local_path = os.path.join("/any/path/rpm")
        server.install(local_path)
        mock_execute.assert_called_with(deploy_install_configure,
                                        local_path, hosts=[])

    @patch('prestoadmin.server.update_configs')
    @patch('prestoadmin.server.deploy_package')
    @patch('prestoadmin.server.rpm_install')
    def test_deploy_install(self, mock_rpm, mock_deploy, mock_update):
        local_path = "/any/path/rpm"
        server.deploy_install_configure(local_path)

        mock_deploy.assert_called_with(local_path)
        mock_rpm.assert_called_with('rpm')
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
    def test_control_command_is_called(self, mock_warn, mock_status,
                                       mock_sudo):
        mock_status.return_value = ['failed_node1', 'bad_node2']
        server.start()
        mock_sudo.assert_called_with(INIT_SCRIPTS + ' start', pty=False)
        mock_warn.assert_called_with("Server failed to start on these nodes: "
                                     "failed_node1,bad_node2")

    @patch('prestoadmin.server.connector')
    @patch('prestoadmin.server.configure.all')
    def test_update_config(self, mock_config, mock_connector):
        e = ConfigFileNotFoundError
        mock_connector.add = e
        server.update_configs()
        mock_config.assert_called_with()

    @patch('prestoadmin.server.execute_query')
    @patch('prestoadmin.server.run')
    def test_check_success_status(self, mock_sleep, mock_execute_query):
        env.hosts = ['bad_server']
        mock_execute_query.side_effect = [False, True]
        self.assertEqual(server.check_server_status(), [])

    @patch('prestoadmin.server.execute_query')
    @patch('prestoadmin.server.run')
    def test_check_success_fail(self,  mock_sleep,
                                mock_execute_query):
        env.hosts = ['bad_server']
        server.RETRY_TIMEOUT = SLEEP_INTERVAL + 1
        mock_execute_query.side_effect = [False, False]
        self.assertEqual(server.check_server_status(), ['bad_server'])
