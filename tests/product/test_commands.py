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
import re
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
                                'An unexpected error occurred.  '
                                'More detailed information can be found in'
                                ' /var/log/presto-admin/presto-admin.log',
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
        expected.sort()
        actual.sort()
        self.assertEqual(expected, actual)

        for container in self.all_hosts():
            self.assert_installed(container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

    def test_server_start_stop(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        cmd_output = self.run_prestoadmin('server start')
        expected = [r'Server started successfully on: master',
                    r'Server started successfully on: slave1',
                    r'Server started successfully on: slave2',
                    r'Server started successfully on: slave3',
                    r'\[master\] out:',
                    r'\[(?P<host>master)\] out: Started as (?P<pid>.*)',
                    r'\[master\] out: Starting presto',
                    r'\[slave1\] out: ',
                    r'\[(?P<host>slave1)\] out: Started as (?P<pid>.*)',
                    r'\[slave1\] out: Starting presto',
                    r'\[slave2\] out: ',
                    r'\[(?P<host>slave2)\] out: Started as (?P<pid>.*)',
                    r'\[slave2\] out: Starting presto',
                    r'\[slave3\] out: ',
                    r'\[(?P<host>slave3)\] out: Started as (?P<pid>.*)',
                    r'\[slave3\] out: Starting presto']
        actual = cmd_output.splitlines()
        actual.sort()

        process_per_host = []
        for expected_regexp, actual_line in zip(expected, actual):
            self.assertRegexpMatches(actual_line, expected_regexp)
            match = re.search(expected_regexp, actual_line)
            try:
                host = match.group('host')
                pid = match.group('pid')
                process_per_host.append((host, pid))
            except IndexError:
                pass

        for host, pid in process_per_host:
            self.exec_create_start(host, 'kill -0 %s' %
                                   pid)

        cmd_output = self.run_prestoadmin('server stop')
        expected = [r'\[master\] out: ',
                    r'\[master\] out: Stopped .*',
                    r'\[master\] out: Stopping presto',
                    r'\[slave1\] out: ',
                    r'\[slave1\] out: Stopped .*',
                    r'\[slave1\] out: Stopping presto',
                    r'\[slave2\] out: ',
                    r'\[slave2\] out: Stopped .*',
                    r'\[slave2\] out: Stopping presto',
                    r'\[slave3\] out: ',
                    r'\[slave3\] out: Stopped .*',
                    r'\[slave3\] out: Stopping presto']
        actual = cmd_output.splitlines()
        actual.sort()
        for expected_regexp, actual_line in zip(expected, actual):
            self.assertRegexpMatches(actual_line, expected_regexp)

        for host, pid in process_per_host:
            self.assertRaisesRegexp(OSError,
                                    'No such process',
                                    self.exec_create_start,
                                    host, 'kill -0 %s' % pid)

    def assert_file_content(self, host, filepath, expected):
        config = self.exec_create_start(host, 'cat %s' % filepath)
        self.assertEqual(config, expected)

    def assert_installed(self, container):
        check_rpm = self.exec_create_start(container,
                                           'rpm -q presto-0.101-1.0')
        self.assertEqual('presto-0.101-1.0.x86_64\n', check_rpm)

    def assert_has_default_config(self, container):
        self.assert_file_content(container,
                                 '/etc/presto/jvm.config',
                                 """-server
-Xmx1G
-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent
-XX:+CMSClassUnloadingEnabled
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-XX:ReservedCodeCacheSize=150M\n""")

        node_properties = self.exec_create_start(
            container, 'cat /etc/presto/node.properties')
        split_properties = node_properties.split('\n', 1)
        self.assertRegexpMatches(split_properties[0], 'node.id=.*')
        self.assertEqual(split_properties[1],
                         """node.data-dir=/var/lib/presto/data
node.environment=presto
plugin.config-dir=/etc/presto/catalog
plugin.dir=/usr/lib/presto/lib/plugin\n""")

        if container in self.slaves:
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     """coordinator=false
discovery.uri=http://master:8080
http-server.http.port=8080
task.max-memory=1GB\n""")

        else:
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     """coordinator=true
discovery-server.enabled=true
discovery.uri=http://master:8080
http-server.http.port=8080
task.max-memory=1GB\n""")

    def assert_has_default_connector(self, container):
        self.assert_file_content(container,
                                 '/etc/presto/catalog/tpch.properties',
                                 'connector.name=tpch')
