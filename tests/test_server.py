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
from prestoadmin.server import INIT_SCRIPTS

from prestoadmin import server
from prestoadmin.configuration import ConfigurationError
from prestoadmin.server import PRESTO_ADMIN_PACKAGES_PATH, \
    LOCAL_ARCHIVE_PATH, PRESTO_RPM, deploy_install_configure
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
    @patch('prestoadmin.server.deploy_package')
    @patch('prestoadmin.server.rpm_install')
    @patch('prestoadmin.server.update_configs')
    def test_install_server(self, mock_configure, mock_rpm_i, mock_deploy,
                            mock_execute):
        server.install()
        local_path = os.path.join(LOCAL_ARCHIVE_PATH, PRESTO_RPM)
        mock_execute.assert_called_with(deploy_install_configure,
                                        local_path, hosts=[])

    @patch('prestoadmin.topology.set_conf_interactive')
    @patch('prestoadmin.main.topology.get_coordinator')
    @patch('prestoadmin.main.topology.get_workers')
    def test_interactive_install(self,  workers_mock, coord_mock,
                                 mock_set_interactive):
        env.topology_config_not_found = ConfigurationError()
        coord_mock.return_value = 'a'
        workers_mock.return_value = ['b']
        server.set_hosts()
        self.assertEqual(server.env.hosts, ['b', 'a'])

    def test_set_host_with_exclude(self):
        env.hosts = ['a', 'b', 'bad']
        env.exclude_hosts = ['bad']
        self.assertEqual(server.set_hosts(), ['a', 'b'])

    @patch('prestoadmin.server.sudo')
    def test_uninstall_is_called(self, mock_sudo):
        server.uninstall()
        mock_sudo.assert_called_with('rpm -e presto')

    @patch('prestoadmin.server.sudo')
    def test_control_command_is_called(self, mock_sudo):
        server.start()
        mock_sudo.assert_called_with(INIT_SCRIPTS + ' start', pty=False)
