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
product tests for presto-admin plugin commands
"""
import os

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_PA_CLUSTER
from tests.product.config_dir_utils import get_install_directory

TMP_JAR_PATH = os.path.join(get_install_directory(), 'pretend.jar')
STD_REMOTE_PATH = '/usr/lib/presto/lib/plugin/hive-cdh5/pretend.jar'


class TestPlugin(BaseProductTestCase):
    def setUp(self):
        super(TestPlugin, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)

    def deploy_jar_to_master(self):
        self.cluster.write_content_to_host('A PRETEND JAR', TMP_JAR_PATH,
                                           self.cluster.master)

    def test_basic_add_jars(self):
        self.upload_topology()
        self.deploy_jar_to_master()
        # no plugin dir argument
        output = self.run_prestoadmin(
            'plugin add_jar %s hive-cdh5' % TMP_JAR_PATH)
        self.assertEqualIgnoringOrder(output, '')
        for host in self.cluster.all_hosts():
            self.assert_path_exists(host, STD_REMOTE_PATH)
            self.cluster.exec_cmd_on_host(host, 'rm %s' % STD_REMOTE_PATH,
                                          raise_error=False)

        # supply plugin directory
        output = self.run_prestoadmin(
            'plugin add_jar %s hive-cdh5 /etc/presto/plugin' % TMP_JAR_PATH)
        self.assertEqual(output, '')
        for host in self.cluster.all_hosts():
            temp_jar_location = '/etc/presto/plugin/hive-cdh5/pretend.jar'
            self.assert_path_exists(host, temp_jar_location)
            self.cluster.exec_cmd_on_host(host, 'rm %s' % temp_jar_location, invoke_sudo=True)

    def test_lost_coordinator(self):
        internal_bad_host = self.cluster.internal_slaves[0]
        bad_host = self.cluster.slaves[0]
        good_hosts = [self.cluster.internal_master,
                      self.cluster.internal_slaves[1],
                      self.cluster.internal_slaves[2]]
        topology = {'coordinator': internal_bad_host,
                    'workers': good_hosts}
        self.upload_topology(topology)
        self.cluster.stop_host(bad_host)
        self.deploy_jar_to_master()
        output = self.run_prestoadmin(
            'plugin add_jar %s hive-cdh5' % TMP_JAR_PATH, raise_error=False)
        self.assertRegexpMatches(output, self.down_node_connection_error(
            internal_bad_host))
        self.assertEqual(len(output.splitlines()), self.len_down_node_error)
        for host in good_hosts:
            self.assert_path_exists(host, STD_REMOTE_PATH)
            self.cluster.exec_cmd_on_host(host, 'rm %s' % STD_REMOTE_PATH,
                                          raise_error=False)

    def test_lost_worker(self):
        internal_bad_host = self.cluster.internal_slaves[0]
        bad_host = self.cluster.slaves[0]
        good_hosts = [self.cluster.internal_master,
                      self.cluster.internal_slaves[1],
                      self.cluster.internal_slaves[2]]
        topology = {'coordinator': self.cluster.internal_master,
                    'workers': self.cluster.internal_slaves}
        self.upload_topology(topology)
        self.cluster.stop_host(bad_host)
        self.deploy_jar_to_master()
        output = self.run_prestoadmin(
            'plugin add_jar %s hive-cdh5' % TMP_JAR_PATH, raise_error=False)
        self.assertRegexpMatches(output, self.down_node_connection_error(
            internal_bad_host))
        self.assertEqual(len(output.splitlines()), self.len_down_node_error)
        for host in good_hosts:
            self.assert_path_exists(host, STD_REMOTE_PATH)
            self.cluster.exec_cmd_on_host(host, 'rm %s' % STD_REMOTE_PATH,
                                          raise_error=False)
