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
import os
import re

from prestoadmin.util import constants
from tests.product.base_product_case import BaseProductTestCase, PRESTO_RPM, \
    LOCAL_RESOURCES_DIR

install_interactive_out = ['Enter user name for SSH connection to all '
                           'nodes: [root] Enter port number for SSH '
                           'connections to all nodes: [22] Enter host name or '
                           'IP address for coordinator node.  Enter an '
                           'external host name or ip address if this is a '
                           'multi-node cluster: [localhost] Enter host names '
                           'or IP addresses for worker nodes separated by '
                           'spaces: [localhost] Deploying rpm...',
                           'Package deployed successfully on: slave1',
                           'Package installed successfully on: slave1',
                           'Package deployed successfully on: master',
                           'Package installed successfully on: master',
                           'Deploying configuration on: master',
                           'Deploying tpch.properties, jmx.properties '
                           'connector configurations on: master ',
                           'Deploying configuration on: slave1',
                           'Deploying tpch.properties, jmx.properties '
                           'connector configurations on: slave1 ']

install_with_ext_host_pa_master_out = ['Deploying rpm...',
                                       'Package deployed successfully on: '
                                       'slave3',
                                       'Package installed successfully on: '
                                       'slave3',
                                       'Package deployed successfully on: '
                                       'slave1',
                                       'Package installed successfully on: '
                                       'slave1',
                                       'Package deployed successfully on: '
                                       'slave2',
                                       'Package installed successfully on: '
                                       'slave2',
                                       'Deploying configuration on: slave3',
                                       'Deploying tpch.properties connector '
                                       'configurations on: slave3 ',
                                       'Deploying configuration on: slave1',
                                       'Deploying tpch.properties connector '
                                       'configurations on: slave1 ',
                                       'Deploying configuration on: slave2',
                                       'Deploying tpch.properties connector '
                                       'configurations on: slave2 ']

install_with_worker_pa_master_out = ['Deploying rpm...',
                                     'Package deployed successfully on: '
                                     'slave3',
                                     'Package installed successfully on: '
                                     'slave3',
                                     'Package deployed successfully on: '
                                     'slave1',
                                     'Package installed successfully on: '
                                     'slave1',
                                     'Package deployed successfully on: '
                                     'master',
                                     'Package installed successfully on: '
                                     'master',
                                     'Package deployed successfully on: '
                                     'slave2',
                                     'Package installed successfully on: '
                                     'slave2',
                                     'Deploying configuration on: slave3',
                                     'Deploying tpch.properties connector '
                                     'configurations on: slave3 ',
                                     'Deploying configuration on: slave1',
                                     'Deploying tpch.properties connector '
                                     'configurations on: slave1 ',
                                     'Deploying configuration on: slave2',
                                     'Deploying tpch.properties connector '
                                     'configurations on: slave2 ',
                                     'Deploying configuration on: master',
                                     'Deploying tpch.properties connector '
                                     'configurations on: master ']

installed_all_hosts_output = ['Deploying rpm...',
                              'Package deployed successfully on: slave3',
                              'Package installed successfully on: slave3',
                              'Package deployed successfully on: slave1',
                              'Package installed successfully on: slave1',
                              'Package deployed successfully on: master',
                              'Package installed successfully on: master',
                              'Package deployed successfully on: slave2',
                              'Package installed successfully on: slave2',
                              'Deploying configuration on: slave3',
                              'Deploying tpch.properties connector '
                              'configurations on: slave3 ',
                              'Deploying configuration on: slave1',
                              'Deploying tpch.properties connector '
                              'configurations on: slave1 ',
                              'Deploying configuration on: slave2',
                              'Deploying tpch.properties connector '
                              'configurations on: slave2 ',
                              'Deploying configuration on: master',
                              'Deploying tpch.properties connector '
                              'configurations on: master ']


class TestServerInstall(BaseProductTestCase):
    default_workers_config_with_slave1_ = """coordinator=false
discovery.uri=http://slave1:8080
http-server.http.port=8080
task.max-memory=1GB\n"""

    default_coord_config_with_slave1_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://slave1:8080
http-server.http.port=8080
task.max-memory=1GB\n"""

    default_workers_config_regex_ = """coordinator=false
discovery.uri=http:.*:8080
http-server.http.port=8080
task.max-memory=1GB\n"""

    default_coord_config_regex_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http:.*:8080
http-server.http.port=8080
task.max-memory=1GB\n"""

    def assert_common_configs(self, container):
        self.assert_installed(container)
        self.assert_file_content(container, '/etc/presto/jvm.config',
                                 self.default_jvm_config_)
        self.assert_node_config(container, self.default_node_properties_)
        self.assert_has_default_connector(container)

    def assert_installed_with_configs(self, master, slaves):
        self.assert_common_configs(master)
        self.assert_file_content(master,
                                 '/etc/presto/config.properties',
                                 self.default_coord_config_with_slave1_)
        for container in slaves:
            self.assert_common_configs(container)
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     self.default_workers_config_with_slave1_)

    def assert_installed_with_regex_configs(self, master, slaves):
        self.assert_common_configs(master)
        self.assert_file_content_regex(master,
                                       '/etc/presto/config.properties',
                                       self.default_coord_config_regex_)
        for container in slaves:
            self.assert_common_configs(container)
            self.assert_file_content_regex(container,
                                           '/etc/presto/config.properties',
                                           self.default_workers_config_regex_)

    def test_install(self):
        self.install_presto_admin()
        self.upload_topology()

        cmd_output = self.server_install()
        expected = installed_all_hosts_output

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        for container in self.all_hosts():
            self.assert_installed(container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

    def test_install_worker_is_pa_master(self):
        self.install_presto_admin()
        topology = {"coordinator": "slave1",
                    "workers": ["master", "slave2", "slave3"]}
        self.upload_topology(topology)
        self.copy_presto_rpm_to_master()

        cmd_output = self.server_install()
        expected = install_with_worker_pa_master_out

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        self.assert_installed_with_configs(self.slaves[0],
                                           [self.slaves[1], self.slaves[2],
                                            self.master])

    def test_install_ext_host_is_pa_master(self):
        self.install_presto_admin()
        topology = {"coordinator": "slave1",
                    "workers": ["slave2", "slave3"]}
        self.upload_topology(topology)
        self.copy_presto_rpm_to_master()

        cmd_output = self.server_install()
        expected = install_with_ext_host_pa_master_out

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        self.assert_installed_with_configs(self.slaves[0],
                                           [self.slaves[1], self.slaves[2]])

    def test_install_when_connector_json_exists(self):
        self.install_presto_admin()
        topology = {"coordinator": "master",
                    "workers": ["slave1"]}
        self.upload_topology(topology)
        self.write_content_to_master('connector.name=jmx',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'jmx.properties'))
        self.copy_presto_rpm_to_master()

        cmd_output = self.server_install()
        expected = ['Deploying rpm...',
                    'Package deployed successfully on: slave1',
                    'Package installed successfully on: slave1',
                    'Package deployed successfully on: master',
                    'Package installed successfully on: master',
                    'Deploying configuration on: master',
                    'Deploying tpch.properties, jmx.properties '
                    'connector configurations on: master ',
                    'Deploying configuration on: slave1',
                    'Deploying tpch.properties, jmx.properties '
                    'connector configurations on: slave1 ']

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        for container in [self.master, self.slaves[0]]:
            self.assert_installed(container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)
            self.assert_has_jmx_connector(container)

    def test_install_when_topology_has_ips(self):
        self.install_presto_admin()
        ips = self.get_ip_address_dict()
        topology = {"coordinator": ips[self.master],
                    "workers": [ips[self.slaves[0]]]}
        self.upload_topology(topology)
        self.write_content_to_master('connector.name=jmx',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'jmx.properties'))
        self.copy_presto_rpm_to_master()

        cmd_output = self.server_install().splitlines()
        expected = [r'Deploying rpm...',
                    r'Package deployed successfully on: ' + ips[self.master],
                    r'Package installed successfully on: ' + ips[self.master],
                    r'Package deployed successfully on: '
                    + ips[self.slaves[0]],
                    r'Package installed successfully on: ' +
                    ips[self.slaves[0]],
                    r'Deploying configuration on: ' + ips[self.master],
                    r'Deploying tpch.properties, jmx.properties '
                    r'connector configurations on: ' + ips[self.master],
                    r'Deploying configuration on: ' + ips[self.slaves[0]],
                    r'Deploying tpch.properties, jmx.properties '
                    r'connector configurations on: ' + ips[self.slaves[0]]]

        cmd_output.sort()
        expected.sort()
        for expected_regexp, actual_line in zip(expected, cmd_output):
            self.assertRegexpMatches(actual_line, expected_regexp)

        self.assert_installed_with_regex_configs(self.master, [self.slaves[0]])
        for container in [self.master, self.slaves[0]]:
            self.assert_has_jmx_connector(container)

    def test_install_interactive_with_hostnames(self):
        self.install_presto_admin()
        self.write_content_to_master('connector.name=jmx',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'jmx.properties'))
        self.copy_presto_rpm_to_master()

        cmd_output = self.run_prestoadmin_script(
            "echo -e 'root\n22\nmaster\nslave1\n' | "
            "./presto-admin server install /mnt/presto-admin/%s " % PRESTO_RPM)
        expected = install_interactive_out

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))
        for container in [self.master, self.slaves[0]]:
            self.assert_installed(container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)
            self.assert_has_jmx_connector(container)

    def test_install_interactive_with_ips(self):
        self.install_presto_admin()
        ips = self.get_ip_address_dict()
        self.copy_presto_rpm_to_master()

        cmd_output = self.run_prestoadmin_script(
            "echo -e 'root\n22\n%s\n%s\n' | "
            "./presto-admin server install /mnt/presto-admin/%s " %
            (ips[self.master], ips[self.slaves[0]], PRESTO_RPM)).splitlines()
        expected = [r'Enter user name for SSH connection to all nodes: '
                    r'\[root\] '
                    r'Enter port number for SSH connections to all nodes: '
                    r'\[22\] '
                    r'Enter host name or IP address for coordinator node.  '
                    r'Enter an external host name or ip address if this is a '
                    r'multi-node cluster: \[localhost\] '
                    r'Enter host names or IP addresses for worker nodes '
                    r'separated by spaces: '
                    r'\[localhost\] Deploying rpm...',
                    r'Package deployed successfully on: ' + ips[self.master],
                    r'Package installed successfully on: ' + ips[self.master],
                    r'Package deployed successfully on: '
                    + ips[self.slaves[0]],
                    r'Package installed successfully on: '
                    + ips[self.slaves[0]],
                    r'Deploying configuration on: ' + ips[self.master],
                    r'Deploying tpch.properties connector '
                    r'configurations on: ' + ips[self.master],
                    r'Deploying configuration on: ' + ips[self.slaves[0]],
                    r'Deploying tpch.properties connector '
                    r'configurations on: ' + ips[self.slaves[0]]]

        cmd_output.sort()
        expected.sort()
        for expected_regexp, actual_line in zip(expected, cmd_output):
            self.assertRegexpMatches(actual_line, expected_regexp)

        self.assert_installed_with_regex_configs(self.master, [self.slaves[0]])

    def test_install_with_wrong_topology(self):
        self.install_presto_admin()
        self.copy_presto_rpm_to_master()
        topology = {"coordinator": "dummy_master", "workers": ["slave1"]}
        self.upload_topology(topology)
        expected = 'u\'dummy_master\' is not a valid ip address or host name.' \
                   '  More detailed information can be found in ' \
                   '/var/log/prestoadmin/presto-admin.log\n'
        self.assertRaisesRegexp(OSError,
                                expected,
                                self.run_prestoadmin,
                                "server install /mnt/presto-admin/%s "
                                % PRESTO_RPM)

    def test_install_with_malformed_topology(self):
        self.install_presto_admin()
        self.copy_presto_rpm_to_master()
        topology = {"coordinator": "master",
                    "workers": "slave1" "slave2"}
        self.upload_topology(topology)
        expected = 'Workers must be of type list.  Found <type \'unicode\'>.' \
                   '  More detailed information can be found in ' \
                   '/var/log/prestoadmin/presto-admin.log'

        self.assertRaisesRegexp(OSError,
                                expected,
                                self.run_prestoadmin,
                                "server install /mnt/presto-admin/%s "
                                % PRESTO_RPM)

    def test_install_with_malformed_connector(self):
        self.install_presto_admin()
        self.copy_presto_rpm_to_master()
        self.upload_topology()
        self.write_content_to_master('connectr.typo:invalid',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'jmx.properties'))
        actual_out = self.server_install()
        expected = 'Underlying exception:\n    Catalog configuration ' \
                   'jmx.properties does not contain connector.name'
        self.assertRegexpMatches(actual_out, expected)

        for container in self.all_hosts():
            self.assert_installed(container)
            self.assert_has_default_config(container)

    def test_connection_to_coord_lost(self):
        self.install_presto_admin()
        self.copy_presto_rpm_to_master()
        topology = {"coordinator": "slave1",
                    "workers": ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.client.stop(self.slaves[0])

        actual_out = self.server_install()
        expected = re.compile(r'Process slave1:.*?\nNetworkError: '
                              r'(Low level socket error connecting to'
                              r' host slave1 on port 22: No route to'
                              r' host \(tried 1 time\)|Timed out '
                              r'trying to connect to slave1 '
                              r'\(tried 1 time\))',
                              flags=re.DOTALL)
        self.assertRegexpMatches(actual_out, expected)

        for container in [self.master, self.slaves[1], self.slaves[2]]:
            self.assert_common_configs(container)
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     self.default_workers_config_with_slave1_)

    def test_install_with_no_perm_to_local_path(self):
        self.install_presto_admin()
        self.copy_presto_rpm_to_master()
        self.upload_topology()
        self.run_prestoadmin("configuration deploy")

        script = 'chmod 600 /mnt/presto-admin/%s; su app-admin -c ' \
                 '"./presto-admin server install /mnt/presto-admin/%s "' \
                 % (PRESTO_RPM, PRESTO_RPM)
        expected = 'Fatal error: error: ' \
                   '/mnt/presto-admin/presto-0.101-1.0.x86_64.rpm: ' \
                   'open failed: Permission denied\n\nAborting.\n'
        self.assertRaisesRegexp(OSError,
                                expected,
                                self.run_prestoadmin_script,
                                script)

    def test_install_twice(self):
        self.test_install()
        output = self.server_install()

        with open(os.path.join(LOCAL_RESOURCES_DIR, 'install_twice.txt'), 'r') \
                as f:
            expected = f.read()

        self.assertEqualIgnoringOrder(expected, output)
        for container in self.all_hosts():
            self.assert_installed(container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)
