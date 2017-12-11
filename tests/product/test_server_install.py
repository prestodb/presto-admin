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

from tests.product import relocate_jdk_directory
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_PA_CLUSTER
from tests.product.config_dir_utils import get_catalog_directory
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
                                       'Deploying tpch.properties catalog '
                                       'configurations on: slave3 ',
                                       'Deploying configuration on: slave1',
                                       'Deploying tpch.properties catalog '
                                       'configurations on: slave1 ',
                                       'Deploying configuration on: slave2',
                                       'Deploying tpch.properties catalog '
                                       'configurations on: slave2 ',
                                       'Using rpm_specifier as a local path',
                                       'Fetching local presto rpm at path: .*',
                                       'Found existing rpm at: .*']

install_with_worker_pa_master_out = ['Deploying rpm on {master}...',
                                     'Deploying rpm on {slave1}...',
                                     'Deploying rpm on {slave2}...',
                                     'Deploying rpm on {slave3}...',
                                     'Package deployed successfully on: '
                                     '{slave3}',
                                     'Package installed successfully on: '
                                     '{slave3}',
                                     'Package deployed successfully on: '
                                     '{slave1}',
                                     'Package installed successfully on: '
                                     '{slave1}',
                                     'Package deployed successfully on: '
                                     '{master}',
                                     'Package installed successfully on: '
                                     '{master}',
                                     'Package deployed successfully on: '
                                     '{slave2}',
                                     'Package installed successfully on: '
                                     '{slave2}',
                                     'Deploying configuration on: {slave3}',
                                     'Deploying tpch.properties catalog '
                                     'configurations on: {slave3} ',
                                     'Deploying configuration on: {slave1}',
                                     'Deploying tpch.properties catalog '
                                     'configurations on: {slave1} ',
                                     'Deploying configuration on: {slave2}',
                                     'Deploying tpch.properties catalog '
                                     'configurations on: {slave2} ',
                                     'Deploying configuration on: {master}',
                                     'Deploying tpch.properties catalog '
                                     'configurations on: {master} ',
                                     'Using rpm_specifier as a local path',
                                     'Fetching local presto rpm at path: .*',
                                     'Found existing rpm at: .*']

installed_all_hosts_output = ['Deploying rpm on {master}...',
                              'Deploying rpm on {slave1}...',
                              'Deploying rpm on {slave2}...',
                              'Deploying rpm on {slave3}...',
                              'Package deployed successfully on: {slave3}',
                              'Package installed successfully on: {slave3}',
                              'Package deployed successfully on: {slave1}',
                              'Package installed successfully on: {slave1}',
                              'Package deployed successfully on: {master}',
                              'Package installed successfully on: {master}',
                              'Package deployed successfully on: {slave2}',
                              'Package installed successfully on: {slave2}',
                              'Deploying configuration on: {slave3}',
                              'Deploying tpch.properties catalog '
                              'configurations on: {slave3} ',
                              'Deploying configuration on: {slave1}',
                              'Deploying tpch.properties catalog '
                              'configurations on: {slave1} ',
                              'Deploying configuration on: {slave2}',
                              'Deploying tpch.properties catalog '
                              'configurations on: {slave2} ',
                              'Deploying configuration on: {master}',
                              'Deploying tpch.properties catalog '
                              'configurations on: {master} ',
                              'Using rpm_specifier as a local path',
                              'Fetching local presto rpm at path: .*',
                              'Found existing rpm at: .*']


class TestServerInstall(BaseProductTestCase):
    default_workers_config_with_slave1_ = """coordinator=false
discovery.uri=http://slave1:7070
http-server.http.port=7070
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    default_coord_config_with_slave1_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://slave1:7070
http-server.http.port=7070
node-scheduler.include-coordinator=false
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    default_workers_config_regex_ = """coordinator=false
discovery.uri=http:.*:7070
http-server.http.port=7070
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    default_coord_config_regex_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http:.*:7070
http-server.http.port=7070
node-scheduler.include-coordinator=false
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    def setUp(self):
        super(TestServerInstall, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)

    def assert_common_configs(self, host):
        installer = StandalonePrestoInstaller(self)
        installer.assert_installed(self, host)
        self.assert_file_content(host, '/etc/presto/jvm.config',
                                 self.default_jvm_config_)
        self.assert_node_config(host, self.default_node_properties_)
        self.assert_has_default_catalog(host)

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

    @attr('smoketest')
    def test_install_with_java8_home(self):
        installer = StandalonePrestoInstaller(self)

        with relocate_jdk_directory(self.cluster, '/usr') as new_java_home:
            topology = {"coordinator": "master",
                        "workers": ["slave1", "slave2", "slave3"],
                        "java8_home": new_java_home}
            self.upload_topology(topology)

            cmd_output = installer.install()
            expected = self.format_err_msgs_with_internal_hosts(installed_all_hosts_output)

            actual = cmd_output.splitlines()
            self.assertRegexpMatchesLineByLine(actual, expected)

            for host in self.cluster.all_hosts():
                installer.assert_installed(self, host)
                self.assert_has_default_config(host)
                self.assert_has_default_catalog(host)

    def test_install_ext_host_is_pa_master(self):
        installer = StandalonePrestoInstaller(self)
        topology = {"coordinator": "slave1",
                    "workers": ["slave2", "slave3"]}
        self.upload_topology(topology)

        cmd_output = installer.install(coordinator='slave1')
        expected = install_with_ext_host_pa_master_out

        actual = cmd_output.splitlines()
        self.assertRegexpMatchesLineByLine(actual, expected)

        self.assert_installed_with_configs(
            self.cluster.slaves[0],
            [self.cluster.slaves[1],
             self.cluster.slaves[2]])

    def test_install_when_catalog_json_exists(self):
        installer = StandalonePrestoInstaller(self)
        topology = {"coordinator": "master",
                    "workers": ["slave1"]}
        self.upload_topology(topology)
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(get_catalog_directory(), 'jmx.properties'),
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
                    'catalog configurations on: master ',
                    'Deploying configuration on: slave1',
                    'Deploying jmx.properties, tpch.properties '
                    'catalog configurations on: slave1 ',
                    'Using rpm_specifier as a local path',
                    'Fetching local presto rpm at path: .*',
                    'Found existing rpm at: .*']

        actual = cmd_output.splitlines()
        self.assertRegexpMatchesLineByLine(actual, expected)

        for container in [self.cluster.master,
                          self.cluster.slaves[0]]:
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_catalog(container)
            self.assert_has_jmx_catalog(container)

    def test_install_when_topology_has_ips(self):
        installer = StandalonePrestoInstaller(self)
        ips = self.cluster.get_ip_address_dict()
        topology = {"coordinator": ips[self.cluster.internal_master],
                    "workers": [ips[self.cluster.internal_slaves[0]]]}
        self.upload_topology(topology)
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(get_catalog_directory(), 'jmx.properties'),
            self.cluster.master
        )

        cmd_output = installer.install().splitlines()
        expected = [
            r'Deploying rpm on %s...' % ips[self.cluster.internal_master],
            r'Deploying rpm on %s...' % ips[self.cluster.internal_slaves[0]],
            r'Package deployed successfully on: ' +
            ips[self.cluster.internal_master],
            r'Package installed successfully on: ' +
            ips[self.cluster.internal_master],
            r'Package deployed successfully on: ' +
            ips[self.cluster.internal_slaves[0]],
            r'Package installed successfully on: ' +
            ips[self.cluster.internal_slaves[0]],
            r'Deploying configuration on: ' +
            ips[self.cluster.internal_master],
            r'Deploying jmx.properties, tpch.properties '
            r'catalog configurations on: ' +
            ips[self.cluster.internal_master] + r' ',
            r'Deploying configuration on: ' +
            ips[self.cluster.internal_slaves[0]],
            r'Deploying jmx.properties, tpch.properties '
            r'catalog configurations on: ' +
            ips[self.cluster.internal_slaves[0]] + r' ',
            r'Using rpm_specifier as a local path',
            r'Fetching local presto rpm at path: .*',
            r'Found existing rpm at: .*']

        cmd_output.sort()
        expected.sort()
        self.assertRegexpMatchesLineByLine(cmd_output, expected)

        self.assert_installed_with_regex_configs(
            self.cluster.master,
            [self.cluster.slaves[0]])
        for host in [self.cluster.master, self.cluster.slaves[0]]:
            self.assert_has_jmx_catalog(host)

    def test_install_interactive(self):
        installer = StandalonePrestoInstaller(self)
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(get_catalog_directory(), 'jmx.properties'),
            self.cluster.master
        )
        rpm_name = installer.copy_presto_rpm_to_master()
        self.write_test_configs(self.cluster)

        additional_keywords = {
            'user': self.cluster.user,
            'rpm_dir': self.cluster.rpm_cache_dir,
            'rpm': rpm_name
        }

        cmd_output = self.run_script_from_prestoadmin_dir(
            'echo -e "%(user)s\n22\n%(master)s\n%(slave1)s\n" | '
            './presto-admin server install %(rpm_dir)s/%(rpm)s ',
            **additional_keywords)

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
                    r'\[localhost\] Using rpm_specifier as a local path',
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
                    r'Deploying jmx.properties, tpch.properties catalog '
                    r'configurations on: ' +
                    self.cluster.internal_master,
                    r'Deploying configuration on: ' +
                    self.cluster.internal_slaves[0],
                    r'Deploying jmx.properties, tpch.properties catalog '
                    r'configurations on: ' +
                    self.cluster.internal_slaves[0],
                    r'Deploying rpm on .*\.\.\.',
                    r'Deploying rpm on .*\.\.\.',
                    r'Fetching local presto rpm at path: .*',
                    r'Found existing rpm at: .*'
                    ]

        self.assertRegexpMatchesLineByLine(actual, expected)
        for container in [self.cluster.master,
                          self.cluster.slaves[0]]:
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_catalog(container)
            self.assert_has_jmx_catalog(container)

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

        for host in [self.cluster.master,
                     self.cluster.slaves[1],
                     self.cluster.slaves[2]]:
            self.assert_common_configs(host)
            self.assert_file_content(
                host,
                '/etc/presto/config.properties',
                self.default_workers_config_with_slave1_
            )

    def test_install_twice(self):
        installer = StandalonePrestoInstaller(self)
        self.upload_topology()
        cmd_output = installer.install()
        expected = self.format_err_msgs_with_internal_hosts(installed_all_hosts_output)

        actual = cmd_output.splitlines()
        self.assertRegexpMatchesLineByLine(actual, expected)

        for container in self.cluster.all_hosts():
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_catalog(container)

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
            self.assert_has_default_catalog(container)

    def test_install_non_root_user(self):
        installer = StandalonePrestoInstaller(self)
        self.upload_topology(
            {"coordinator": "master",
             "workers": ["slave1", "slave2", "slave3"],
             "username": "app-admin"}
        )

        rpm_name = installer.copy_presto_rpm_to_master(cluster=self.cluster)
        self.write_test_configs(self.cluster)
        self.run_prestoadmin(
            'server install {rpm_dir}/{name} -p password'.format(
                rpm_dir=self.cluster.rpm_cache_dir, name=rpm_name)
        )

        for container in self.cluster.all_hosts():
            installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_catalog(container)

    def format_err_msgs_with_internal_hosts(self, msgs):
        formatted_msg = []
        for msg in msgs:
            formatted_msg.append(msg.format(master=self.cluster.internal_master,
                                            slave1=self.cluster.internal_slaves[0],
                                            slave2=self.cluster.internal_slaves[1],
                                            slave3=self.cluster.internal_slaves[2]))
        return formatted_msg
