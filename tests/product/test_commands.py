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
from tests.product.base_product_case import BaseProductTestCase, PRESTO_RPM


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
        expected.sort()
        cmd_output.sort()
        for actual, expected_regexp in zip(cmd_output, expected):
            self.assertRegexpMatches(actual, expected_regexp)

        for container in self.all_hosts():
            self.assert_uninstalled(container)
            self.assert_path_removed(container, '/etc/presto')
            self.assert_path_removed(container, '/usr/lib/presto')
            self.assert_path_removed(container, '/usr/shared/doc/presto')
            self.assert_path_removed(container, '/etc/rc.d/init.d/presto')

    def test_server_start_stop(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        cmd_output = self.run_prestoadmin('server start').splitlines()
        cmd_output.sort()
        for expected_regexp, actual_line in zip(self.expected_start(),
                                                cmd_output):
            self.assertRegexpMatches(actual_line, expected_regexp)

        process_per_host = self.get_process_per_host(cmd_output)
        self.assert_started(process_per_host)
        cmd_output = self.run_prestoadmin('server stop').splitlines()
        cmd_output.sort()
        for expected_regexp, actual_line in zip(self.expected_stop(),
                                                cmd_output):
            self.assertRegexpMatches(actual_line, expected_regexp)

        self.assert_stopped(process_per_host)

    def test_server_restart(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        start_output = self.run_prestoadmin('server start')

        restart_output = self.run_prestoadmin('server restart').splitlines()
        expected_output = list(
            set(self.expected_stop()[:] + self.expected_start()[:]))
        expected_output.sort()
        restart_output.sort()
        for actual, expected_regexp in zip(restart_output, expected_output):
            self.assertRegexpMatches(actual, expected_regexp)

        process_per_host = self.get_process_per_host(start_output)
        self.assert_stopped(process_per_host)
        process_per_host = self.get_process_per_host(restart_output)
        self.assert_started(process_per_host)

    def get_process_per_host(self, output_lines):
        process_per_host = []
        for line in output_lines:
            match = re.search(r'\[(?P<host>.*?)\] out: Started as (?P<pid>.*)',
                              line)
            if match:
                process_per_host.append((match.group('host'),
                                         match.group('pid')))
        return process_per_host

    def expected_start(self):
        return [r'Server started successfully on: master',
                r'Server started successfully on: slave1',
                r'Server started successfully on: slave2',
                r'Server started successfully on: slave3',
                r'\[master\] out: ',
                r'\[master\] out: Started as .*',
                r'\[master\] out: Starting presto',
                r'\[slave1\] out: ',
                r'\[slave1\] out: Started as .*',
                r'\[slave1\] out: Starting presto',
                r'\[slave2\] out: ',
                r'\[slave2\] out: Started as .*',
                r'\[slave2\] out: Starting presto',
                r'\[slave3\] out: ',
                r'\[slave3\] out: Started as .*',
                r'\[slave3\] out: Starting presto']

    def expected_stop(self):
        return [r'\[master\] out: ',
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

    def assert_started(self, process_per_host):
        for host, pid in process_per_host:
            self.exec_create_start(host, 'kill -0 %s' %
                                   pid)
        return process_per_host

    def assert_stopped(self, process_per_host):
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
                                           'rpm -q presto')
        self.assertEqual(PRESTO_RPM[:-4] + '\n', check_rpm)

    def assert_uninstalled(self, container):
        self.assertRaisesRegexp(OSError, 'package presto is not installed',
                                self.exec_create_start,
                                container, 'rpm -q presto')

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

    def assert_path_removed(self, container, directory):
        self.exec_create_start(container, ' [ ! -e %s ]' % directory)
