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
from tests.product.base_product_case import BaseProductTestCase, docker_only
from tests.product.constants import LOCAL_RESOURCES_DIR
from tests.product.standalone.presto_installer import StandalonePrestoInstaller
from tests.product.prestoadmin_installer import PrestoadminInstaller


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
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
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

    def test_uninstall_when_server_down(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())

        self.run_prestoadmin('server stop -H %s' %
                             self.cluster.internal_slaves[0])
        cmd_output = self.run_prestoadmin('server uninstall').splitlines()
        self.assert_stopped(process_per_host)
        expected = uninstall_output + self.expected_stop(
            not_running=[self.cluster.internal_slaves[0]])[:]
        self.assertRegexpMatchesLineByLine(cmd_output, expected)

        for container in self.cluster.all_hosts():
            self.assert_uninstalled_dirs_removed(container)

    def test_uninstall_twice(self):
        self.test_uninstall()

        output = self.run_prestoadmin('server uninstall', raise_error=False)
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'uninstall_twice.txt'),
                  'r') as f:
            expected = f.read()

        self.assertEqualIgnoringOrder(expected, output)

    def test_uninstall_lost_host(self):
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)
        pa_installer = PrestoadminInstaller(self)
        pa_installer.install()
        topology = {"coordinator": self.cluster.internal_slaves[0],
                    "workers": [self.cluster.internal_master,
                                self.cluster.internal_slaves[1],
                                self.cluster.internal_slaves[2]]}
        self.upload_topology(topology)
        self.installer.install()
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())
        self.assert_started(process_per_host)
        down_node = self.cluster.internal_slaves[0]
        self.cluster.stop_host(
            self.cluster.slaves[0])

        expected = self.down_node_connection_error(
            self.cluster.internal_slaves[0])
        cmd_output = self.run_prestoadmin('server uninstall',
                                          raise_error=False)
        self.assertRegexpMatches(cmd_output, expected)
        process_per_active_host = []
        for host, pid in process_per_host:
            if host not in down_node:
                process_per_active_host.append((host, pid))
        self.assert_stopped(process_per_active_host)

        for container in [self.cluster.internal_master,
                          self.cluster.internal_slaves[1],
                          self.cluster.internal_slaves[2]]:
            self.assert_uninstalled_dirs_removed(container)

    def test_uninstall_with_dir_readonly(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())
        self.assert_started(process_per_host)

        self.run_script_from_prestoadmin_dir("chmod 500 -R /usr/lib/presto")
        self.run_prestoadmin('server uninstall', raise_error=False)

        # The master node was not able to be stopped or uninstalled because
        # the permissions of the directory were changed such that the
        # stop command can't run
        pid_to_remove = None
        for (host, pid) in process_per_host:
            if host == self.cluster.internal_master:
                pid_to_remove = pid
        process_per_host.remove((self.cluster.internal_master, pid_to_remove))
        self.assert_stopped(process_per_host)

        uninstalled_hosts = self.cluster.all_hosts()[:]
        uninstalled_hosts.remove(self.cluster.master)

        for container in uninstalled_hosts:
            self.assert_uninstalled_dirs_removed(container)

        self.installer.assert_installed(self, container=self.cluster.master)

    @docker_only
    def test_uninstall_as_non_sudo(self):
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)
        self.upload_topology()
        self.installer.install()

        script = './presto-admin server uninstall -u testuser -p testpass'
        output = self.run_script_from_prestoadmin_dir(script)
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'non_sudo_uninstall.txt'),
                  'r') as f:
            expected = f.read()

        self.assertEqualIgnoringOrder(expected, output)
