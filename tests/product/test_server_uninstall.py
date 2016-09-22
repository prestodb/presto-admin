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

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_PRESTO_CLUSTER
from tests.product.constants import LOCAL_RESOURCES_DIR
from tests.product.standalone.presto_installer import StandalonePrestoInstaller

uninstall_output = ['Package uninstalled successfully on: slave1',
                    'Package uninstalled successfully on: slave2',
                    'Package uninstalled successfully on: slave3',
                    'Package uninstalled successfully on: master']


class TestServerUninstall(BaseProductTestCase):
    def setUp(self):
        super(TestServerUninstall, self).setUp()
        self.installer = StandalonePrestoInstaller(self)

    @attr('smoketest')
    def test_uninstall(self):
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PRESTO_CLUSTER)
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())
        self.assert_started(process_per_host)

        cmd_output = self.run_prestoadmin(
            'server uninstall', raise_error=False).splitlines()
        self.assert_stopped(process_per_host)
        expected = uninstall_output + self.expected_stop()[:]
        self.assertRegexpMatchesLineByLine(cmd_output, expected)

        for container in self.cluster.all_hosts():
            self.assert_uninstalled_dirs_removed(container)

    def assert_uninstalled_dirs_removed(self, container):
        self.installer.assert_uninstalled(container)
        self.assert_path_removed(container, '/etc/presto')
        self.assert_path_removed(container, '/usr/lib/presto')
        self.assert_path_removed(container, '/var/lib/presto')
        self.assert_path_removed(container, '/usr/shared/doc/presto')
        self.assert_path_removed(container, '/etc/init.d/presto')

    def test_uninstall_twice(self):
        self.test_uninstall()

        output = self.run_prestoadmin('server uninstall', raise_error=False)
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'uninstall_twice.txt'),
                  'r') as f:
            expected = f.read()

        self.assertEqualIgnoringOrder(expected, output)

    def test_uninstall_lost_host(self):
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PRESTO_CLUSTER)

        self.cluster.stop_host(
            self.cluster.slaves[0])

        expected = self.down_node_connection_error(
            self.cluster.internal_slaves[0])
        cmd_output = self.run_prestoadmin('server uninstall',
                                          raise_error=False)
        self.assertRegexpMatches(cmd_output, expected)

        for container in [self.cluster.internal_master,
                          self.cluster.internal_slaves[1],
                          self.cluster.internal_slaves[2]]:
            self.assert_uninstalled_dirs_removed(container)
