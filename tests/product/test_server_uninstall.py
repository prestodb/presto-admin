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
from tests.product.base_product_case import BaseProductTestCase, \
    LOCAL_RESOURCES_DIR

uninstall_output = ['Package uninstalled successfully on: slave1',
                    'Package uninstalled successfully on: slave2',
                    'Package uninstalled successfully on: slave3',
                    'Package uninstalled successfully on: master']


class TestServerUninstall(BaseProductTestCase):
    def test_uninstall(self):
        self.install_default_presto()
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())
        self.assert_started(process_per_host)

        cmd_output = self.run_prestoadmin('server uninstall').splitlines()
        self.assert_stopped(process_per_host)
        expected = uninstall_output + self.expected_stop()[:]
        self.assertRegexpMatchesLineByLine(cmd_output, expected)

        for container in self.all_hosts():
            self.assert_uninstalled_dirs_removed(container)

    def assert_uninstalled_dirs_removed(self, container):
        self.assert_uninstalled(container)
        self.assert_path_removed(container, '/etc/presto')
        self.assert_path_removed(container, '/usr/lib/presto')
        self.assert_path_removed(container, '/var/lib/presto')
        self.assert_path_removed(container, '/usr/shared/doc/presto')
        self.assert_path_removed(container, '/etc/rc.d/init.d/presto')

    def test_uninstall_when_server_down(self):
        self.install_default_presto()
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())

        self.run_prestoadmin('server stop -H %s' % self.slaves[0])
        cmd_output = self.run_prestoadmin('server uninstall').splitlines()
        self.assert_stopped(process_per_host)
        expected = uninstall_output + self.expected_stop(
            not_running=[self.slaves[0]])[:]
        self.assertRegexpMatchesLineByLine(cmd_output, expected)

        for container in self.all_hosts():
            self.assert_uninstalled_dirs_removed(container)

    def test_uninstall_twice(self):
        self.test_uninstall()

        output = self.run_prestoadmin('server uninstall')
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'uninstall_twice.txt'),
                  'r') as f:
            expected = f.read()

        self.assertEqualIgnoringOrder(expected, output)

    def test_uninstall_lost_host(self):
        self.install_presto_admin()
        topology = {"coordinator": "slave1",
                    "workers": ["master", "slave2", "slave3"]}
        self.upload_topology(topology)
        self.server_install()
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())
        self.assert_started(process_per_host)
        self.stop_and_wait(self.slaves[0])

        cmd_output = self.run_prestoadmin('server uninstall')
        expected = re.compile(r'Process slave1:.*?\nNetworkError: '
                              r'(Low level socket error connecting to'
                              r' host slave1 on port 22: No route to'
                              r' host \(tried 1 time\)|Timed out '
                              r'trying to connect to slave1 '
                              r'\(tried 1 time\))',
                              flags=re.DOTALL)
        self.assertRegexpMatches(cmd_output, expected)
        process_per_active_host = []
        for host, pid in process_per_host:
            if host not in self.slaves[0]:
                process_per_active_host.append((host, pid))
        self.assert_stopped(process_per_active_host)

        for container in [self.master, self.slaves[1], self.slaves[2]]:
            self.assert_uninstalled_dirs_removed(container)

    def test_uninstall_with_dir_readonly(self):
        self.install_default_presto()
        start_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(start_output.splitlines())
        self.assert_started(process_per_host)

        self.run_prestoadmin_script("chmod 500 -R /usr/lib/presto")
        cmd_output = self.run_prestoadmin('server uninstall').splitlines()
        self.assert_stopped(process_per_host)
        expected = uninstall_output + self.expected_stop()[:]
        self.assertRegexpMatchesLineByLine(cmd_output, expected)

        for container in self.all_hosts():
            self.assert_uninstalled_dirs_removed(container)

    def test_uninstall_as_non_sudo(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()

        script = './presto-admin server uninstall -u testuser -p testpass'
        output = self.run_prestoadmin_script(script)
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'non_sudo_uninstall.txt'),
                  'r') as f:
            expected = f.read()

        self.assertEqualIgnoringOrder(expected, output)
