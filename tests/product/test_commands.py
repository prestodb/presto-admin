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
Product tests for presto-admin commands
"""
import os

from prestoadmin.util import constants
from tests.product import base_product_case
from tests.product.base_product_case import BaseProductTestCase


class TestCommands(BaseProductTestCase):
    def test_topology_show(self):
        self.install_presto_admin()
        self.upload_topology()
        actual = self.run_prestoadmin('topology show')
        expected = """{'coordinator': u'master',
 'port': '22',
 'username': 'root',
 'workers': [u'slave1',
             u'slave2',
             u'slave3']}
"""
        self.assertEqual(expected, actual)

    def test_topology_show_not_exists(self):
        self.install_presto_admin()
        self.assertRaisesRegexp(OSError,
                                'Missing topology configuration in '
                                '/etc/opt/prestoadmin/config.json.  '
                                'More detailed information can be found in'
                                ' /var/log/prestoadmin/presto-admin.log',
                                self.run_prestoadmin,
                                'topology show'
                                )

    def test_install_presto(self):
        self.install_presto_admin()
        self.upload_topology()

        cmd_output = self.server_install()
        expected = ['Deploying rpm...',
                    'Package deployed successfully on: slave3',
                    'Package installed successfully on: slave3',
                    'Package deployed successfully on: slave1',
                    'Package installed successfully on: slave1',
                    'Package deployed successfully on: master',
                    'Package installed successfully on: master',
                    'Package deployed successfully on: slave2',
                    'Package installed successfully on: slave2',
                    'Deploying configuration on: slave3',
                    'Deploying tpch.properties connector configurations '
                    'on: '
                    'slave3 ',
                    'Deploying configuration on: slave1',
                    'Deploying tpch.properties connector configurations '
                    'on: '
                    'slave1 ',
                    'Deploying configuration on: slave2',
                    'Deploying tpch.properties connector configurations on: '
                    'slave2 ',
                    'Deploying configuration on: master',
                    'Deploying tpch.properties connector configurations on: '
                    'master ']

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        for container in self.all_hosts():
            self.assert_installed(container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

    def test_uninstall_presto(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())
        cmd_output = self.run_prestoadmin('server uninstall').splitlines()
        self.assert_stopped(process_per_host)
        expected = ['Package uninstalled successfully on: slave1',
                    'Package uninstalled successfully on: slave2',
                    'Package uninstalled successfully on: slave3',
                    'Package uninstalled successfully on: master']
        expected += self.expected_stop()[:]
        self.assertRegexpMatchesLineByLine(cmd_output, expected)

        for container in self.all_hosts():
            self.assert_uninstalled(container)
            self.assert_path_removed(container, '/etc/presto')
            self.assert_path_removed(container, '/usr/lib/presto')
            self.assert_path_removed(container, '/var/lib/presto')
            self.assert_path_removed(container, '/usr/shared/doc/presto')
            self.assert_path_removed(container, '/etc/rc.d/init.d/presto')

    def test_configuration_deploy(self):
        self.install_presto_admin()
        self.upload_topology()
        output = self.run_prestoadmin('configuration show')
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_none.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(expected, output)
        self.run_prestoadmin('configuration deploy')
        for container in self.all_hosts():
            self.assert_has_default_config(container)
        output = self.run_prestoadmin('configuration show')
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_default.txt'), 'r') as f:
            expected = f.read()
        self.assertRegexpMatches(output, expected)

        filename = 'config.properties'
        path = os.path.join(constants.COORDINATOR_DIR, filename)
        dummy_property = 'a.dummy.property=\'single-quoted\''
        self.write_content_to_master(dummy_property,
                                     path)

        path = os.path.join(constants.WORKERS_DIR, filename)
        self.write_content_to_master(dummy_property, path)

        self.run_prestoadmin('configuration deploy coordinator')
        for container in self.slaves:
            self.assert_has_default_config(container)

        self.assert_file_content(self.master,
                                 os.path.join(constants.REMOTE_CONF_DIR,
                                              'config.properties'),
                                 dummy_property + '\n' +
                                 self.default_coordinator_config_)

        filename = 'node.properties'
        path = os.path.join(constants.WORKERS_DIR, filename)
        self.write_content_to_master('node.environment=test', path)
        path = os.path.join(constants.COORDINATOR_DIR, filename)
        self.write_content_to_master('node.environment=test', path)

        self.run_prestoadmin('configuration deploy workers')
        for container in self.slaves:
            self.assert_file_content(container,
                                     os.path.join(constants.REMOTE_CONF_DIR,
                                                  'config.properties'),
                                     dummy_property + '\n' +
                                     self.default_workers_config_)
            expected = """node.data-dir=/var/lib/presto/data
node.environment=test
plugin.config-dir=/etc/presto/catalog
plugin.dir=/usr/lib/presto/lib/plugin\n"""
            self.assert_node_config(container, expected)

        self.assert_node_config(self.master, self.default_node_properties_)
