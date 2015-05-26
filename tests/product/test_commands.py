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
