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

from nose.plugins.attrib import attr

from prestoadmin.util import constants
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.standalone.presto_installer import StandalonePrestoInstaller
from tests.product.constants import LOCAL_RESOURCES_DIR


install_with_ext_host_pa_master_out = ['Deploying rpm on slave1...',
                                       'Deploying rpm on slave2...',
                                       'Deploying rpm on slave3...',
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

install_with_worker_pa_master_out = ['Deploying rpm on master...',
                                     'Deploying rpm on slave1...',
                                     'Deploying rpm on slave2...',
                                     'Deploying rpm on slave3...',
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

installed_all_hosts_output = ['Deploying rpm on master...',
                              'Deploying rpm on slave1...',
                              'Deploying rpm on slave2...',
                              'Deploying rpm on slave3...',
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
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    default_coord_config_with_slave1_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://slave1:8080
http-server.http.port=8080
node-scheduler.include-coordinator=false
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    default_workers_config_regex_ = """coordinator=false
discovery.uri=http:.*:8080
http-server.http.port=8080
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    default_coord_config_regex_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http:.*:8080
http-server.http.port=8080
node-scheduler.include-coordinator=false
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    def setUp(self):
        super(TestServerInstall, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)

    def assert_common_configs(self, container):
        installer = StandalonePrestoInstaller(self)
        installer.assert_installed(self, container)
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

    def test_install_with_java8_home(self):
        installer = StandalonePrestoInstaller(self)
        for container in self.cluster.all_hosts():
            self.cluster.exec_cmd_on_host(container,
                                          "mv /usr/java/jdk1.8.0_40 /usr/")
        topology = {"coordinator": "master",
                    "workers": ["slave1", "slave2", "slave3"],
                    "java8_home": "/usr/jdk1.8.0_40/jre"}
        self.upload_topology(topology)

        cmd_output = installer.install()
        expected = installed_all_hosts_output

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        for container in self.cluster.all_hosts():
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

    @attr('smoketest')
    def test_install(self, installer=None):
        if installer is None:
            installer = StandalonePrestoInstaller(self)

        self.upload_topology()

        cmd_output = installer.install()
        expected = installed_all_hosts_output

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        for container in self.cluster.all_hosts():
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

    def test_install_worker_is_pa_master(self):
        installer = StandalonePrestoInstaller(self)
        topology = {"coordinator": "slave1",
                    "workers": ["master", "slave2", "slave3"]}
        self.upload_topology(topology)

        cmd_output = installer.install(coordinator='slave1')
        expected = install_with_worker_pa_master_out

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        self.assert_installed_with_configs(
            self.cluster.slaves[0],
            [self.cluster.slaves[1],
             self.cluster.slaves[2],
             self.cluster.master])

    def test_install_ext_host_is_pa_master(self):
        installer = StandalonePrestoInstaller(self)
        topology = {"coordinator": "slave1",
                    "workers": ["slave2", "slave3"]}
        self.upload_topology(topology)

        cmd_output = installer.install(coordinator='slave1')
        expected = install_with_ext_host_pa_master_out

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        self.assert_installed_with_configs(
            self.cluster.slaves[0],
            [self.cluster.slaves[1],
             self.cluster.slaves[2]])

    def test_install_when_connector_json_exists(self):
        installer = StandalonePrestoInstaller(self)
        topology = {"coordinator": "master",
                    "workers": ["slave1"]}
        self.upload_topology(topology)
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(constants.CONNECTORS_DIR, 'jmx.properties'),
            self.cluster.master
        )

        cmd_output = installer.install()
        expected = ['Deploying rpm on master...',
                    'Deploying rpm on slave1...',
                    'Package deployed successfully on: slave1',
                    'Package installed successfully on: slave1',
                    'Package deployed successfully on: master',
                    'Package installed successfully on: master',
                    'Deploying configuration on: master',
                    'Deploying jmx.properties, tpch.properties '
                    'connector configurations on: master ',
                    'Deploying configuration on: slave1',
                    'Deploying jmx.properties, tpch.properties '
                    'connector configurations on: slave1 ']

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

        for container in [self.cluster.master,
                          self.cluster.slaves[0]]:
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)
            self.assert_has_jmx_connector(container)

    def test_install_when_topology_has_ips(self):
        installer = StandalonePrestoInstaller(self)
        ips = self.cluster.get_ip_address_dict()
        topology = {"coordinator": ips[self.cluster.master],
                    "workers": [ips[self.cluster.slaves[0]]]}
        self.upload_topology(topology)
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(constants.CONNECTORS_DIR, 'jmx.properties'),
            self.cluster.master
        )

        cmd_output = installer.install().splitlines()
        expected = [
            r'Deploying rpm on %s...' % ips[self.cluster.master],
            r'Deploying rpm on %s...' % ips[self.cluster.slaves[0]],
            r'Package deployed successfully on: ' + ips[
                self.cluster.master],
            r'Package installed successfully on: ' + ips[
                self.cluster.master],
            r'Package deployed successfully on: ' +
            ips[self.cluster.slaves[0]],
            r'Package installed successfully on: ' +
            ips[self.cluster.slaves[0]],
            r'Deploying configuration on: ' +
            ips[self.cluster.master],
            r'Deploying jmx.properties, tpch.properties '
            r'connector configurations on: ' +
            ips[self.cluster.master],
            r'Deploying configuration on: ' +
            ips[self.cluster.slaves[0]],
            r'Deploying jmx.properties, tpch.properties '
            r'connector configurations on: ' +
            ips[self.cluster.slaves[0]]]

        cmd_output.sort()
        expected.sort()
        self.assertRegexpMatchesLineByLine(expected, cmd_output)

        self.assert_installed_with_regex_configs(
            self.cluster.master,
            [self.cluster.slaves[0]])
        for container in [self.cluster.master,
                          self.cluster.slaves[0]]:
            self.assert_has_jmx_connector(container)

    def test_install_interactive_with_hostnames(self):
        installer = StandalonePrestoInstaller(self)
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(constants.CONNECTORS_DIR, 'jmx.properties'),
            self.cluster.master
        )
        rpm_name = installer.copy_presto_rpm_to_master()
        self.write_test_configs(self.cluster)

        cmd_output = self.run_script_from_prestoadmin_dir(
            'echo -e "root\n22\n%(master)s\n%(slave1)s\n" | '
            './presto-admin server install /mnt/presto-admin/%(rpm)s ',
            rpm=rpm_name)

        actual = cmd_output.splitlines()
        expected = [r'Enter user name for SSH connection to all nodes: '
                    r'\[root\] '
                    r'Enter port number for SSH connections to all nodes: '
                    r'\[22\] '
                    r'Enter host name or IP address for coordinator node. '
                    r'Enter an external host name or ip address if this is a '
                    r'multi-node cluster: \[localhost\] '
                    r'Enter host names or IP addresses for worker nodes '
                    r'separated by spaces: '
                    r'\[localhost\] Deploying rpm on .*\.\.\.',
                    r'Package deployed successfully on: ' +
                    self.cluster.internal_master,
                    r'Package installed successfully on: ' +
                    self.cluster.internal_master,
                    r'Package deployed successfully on: ' +
                    self.cluster.internal_slaves[0],
                    r'Package installed successfully on: ' +
                    self.cluster.internal_slaves[0],
                    r'Deploying configuration on: ' +
                    self.cluster.internal_master,
                    r'Deploying jmx.properties, tpch.properties connector '
                    r'configurations on: ' +
                    self.cluster.internal_master,
                    r'Deploying configuration on: ' +
                    self.cluster.internal_slaves[0],
                    r'Deploying jmx.properties, tpch.properties connector '
                    r'configurations on: ' +
                    self.cluster.internal_slaves[0],
                    r'Deploying rpm on .*\.\.\.']

        self.assertRegexpMatchesLineByLine(actual, expected)
        for container in [self.cluster.master,
                          self.cluster.slaves[0]]:
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)
            self.assert_has_jmx_connector(container)

    def test_install_interactive_with_ips(self):
        installer = StandalonePrestoInstaller(self)
        ips = self.cluster.get_ip_address_dict()
        rpm_name = installer.copy_presto_rpm_to_master()
        self.write_test_configs(self.cluster)

        additional_keywords = {
            'rpm': rpm_name,
            'master_ip': ips[self.cluster.master],
            'slave1_ip': ips[self.cluster.slaves[0]]
        }
        cmd_output = self.run_script_from_prestoadmin_dir(
            'echo -e "root\n22\n%(master_ip)s\n%(slave1_ip)s\n" | '
            './presto-admin server install /mnt/presto-admin/%(rpm)s ',
            **additional_keywords).splitlines()
        expected = [r'Enter user name for SSH connection to all nodes: '
                    r'\[root\] '
                    r'Enter port number for SSH connections to all nodes: '
                    r'\[22\] '
                    r'Enter host name or IP address for coordinator node. '
                    r'Enter an external host name or ip address if this is a '
                    r'multi-node cluster: \[localhost\] '
                    r'Enter host names or IP addresses for worker nodes '
                    r'separated by spaces: '
                    r'\[localhost\] Deploying rpm on .*\.\.\.',
                    r'Package deployed successfully on: ' +
                    ips[self.cluster.master],
                    r'Package installed successfully on: ' +
                    ips[self.cluster.master],
                    r'Package deployed successfully on: ' +
                    ips[self.cluster.slaves[0]],
                    r'Package installed successfully on: ' +
                    ips[self.cluster.slaves[0]],
                    r'Deploying configuration on: ' +
                    ips[self.cluster.master],
                    r'Deploying tpch.properties connector '
                    r'configurations on: ' +
                    ips[self.cluster.master],
                    r'Deploying configuration on: ' +
                    ips[self.cluster.slaves[0]],
                    r'Deploying tpch.properties connector '
                    r'configurations on: ' +
                    ips[self.cluster.slaves[0]],
                    r'Deploying rpm on .*\.\.\.']

        cmd_output.sort()
        expected.sort()
        for expected_regexp, actual_line in zip(expected, cmd_output):
            self.assertRegexpMatches(actual_line, expected_regexp)

        self.assert_installed_with_regex_configs(
            self.cluster.master,
            [self.cluster.slaves[0]])

    def test_install_with_wrong_topology(self):
        installer = StandalonePrestoInstaller(self)
        rpm_name = installer.copy_presto_rpm_to_master()
        topology = {'coordinator': 'dummy_master', 'workers': ['slave1']}
        self.upload_topology(topology)
        expected = 'u\'dummy_master\' is not a valid ip address or' \
                   ' host name.' \
                   '  More detailed information can be found in ' \
                   '/var/log/prestoadmin/presto-admin.log\n'
        self.assertRaisesRegexp(OSError,
                                expected,
                                self.run_prestoadmin,
                                'server install /mnt/presto-admin/%(rpm)s ',
                                rpm=rpm_name)

    def test_install_with_malformed_topology(self):
        installer = StandalonePrestoInstaller(self)
        rpm_name = installer.copy_presto_rpm_to_master()
        topology = {'coordinator': 'master',
                    'workers': 'slave1' 'slave2'}
        self.upload_topology(topology)
        expected = 'Workers must be of type list.  Found <type \'unicode\'>.' \
                   '  More detailed information can be found in ' \
                   '/var/log/prestoadmin/presto-admin.log'

        self.assertRaisesRegexp(OSError,
                                expected,
                                self.run_prestoadmin,
                                'server install /mnt/presto-admin/%(rpm)s ',
                                rpm=rpm_name)

    def test_install_with_malformed_connector(self):
        installer = StandalonePrestoInstaller(self)
        self.upload_topology()
        self.cluster.write_content_to_host(
            'connectr.typo:invalid',
            os.path.join(constants.CONNECTORS_DIR, 'jmx.properties'),
            self.cluster.master
        )
        actual_out = installer.install(pa_raise_error=False)
        expected = 'Underlying exception:\n    Catalog configuration ' \
                   'jmx.properties does not contain connector.name'
        self.assertRegexpMatches(actual_out, expected)

        for container in self.cluster.all_hosts():
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)

    def test_connection_to_coord_lost(self):
        installer = StandalonePrestoInstaller(self)
        down_node = self.cluster.internal_slaves[0]
        topology = {"coordinator": down_node,
                    "workers": [self.cluster.internal_master,
                                self.cluster.internal_slaves[1],
                                self.cluster.internal_slaves[2]]}
        self.upload_topology(topology=topology)
        self.cluster.stop_host(
            self.cluster.slaves[0])

        actual_out = installer.install(
            coordinator=down_node, pa_raise_error=False)

        self.assertRegexpMatches(
            actual_out,
            self.down_node_connection_error(down_node)
        )

        for container in [self.cluster.master,
                          self.cluster.slaves[1],
                          self.cluster.slaves[2]]:
            self.assert_common_configs(container)
            self.assert_file_content(
                container,
                '/etc/presto/config.properties',
                self.default_workers_config_with_slave1_.replace(
                    down_node, self.cluster.get_down_hostname(down_node)
                )
            )

    def test_install_twice(self):
        installer = StandalonePrestoInstaller(self)
        self.test_install(installer=installer)
        output = installer.install(pa_raise_error=False)

        self.default_keywords.update(installer.get_keywords())

        with open(os.path.join(LOCAL_RESOURCES_DIR, 'install_twice.txt'),
                  'r') as f:
            expected = f.read()
        expected = self.escape_for_regex(
            self.replace_keywords(expected))

        self.assertRegexpMatchesLineByLine(output.splitlines(),
                                           expected.splitlines())
        for container in self.cluster.all_hosts():
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)
