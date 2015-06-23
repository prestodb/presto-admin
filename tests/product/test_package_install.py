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

from tests.product.base_product_case import BaseProductTestCase, PRESTO_RPM, \
    LOCAL_RESOURCES_DIR, PRESTO_RPM_BASENAME


class TestPackageInstall(BaseProductTestCase):
    def setUp(self):
        super(TestPackageInstall, self).setUp()
        self.setup_docker_cluster()
        self.install_presto_admin()
        self.upload_topology()

    @attr('smoketest')
    def test_package_install(self):
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s'
                             % PRESTO_RPM)
        for container in self.docker_cluster.all_hosts():
            self.assert_installed(container)

    def test_install_coord_using_dash_h(self):
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s -H master'
                             % PRESTO_RPM)
        self.assert_installed(self.docker_cluster.master)
        for slave in self.docker_cluster.slaves:
            self.assert_uninstalled(slave)

    def test_install_worker_using_dash_h(self):
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s '
                             '-H slave1' % PRESTO_RPM)

        self.assert_installed(self.docker_cluster.slaves[0])
        self.assert_uninstalled(self.docker_cluster.master)
        self.assert_uninstalled(self.docker_cluster.slaves[1])
        self.assert_uninstalled(self.docker_cluster.slaves[2])

    def test_install_workers_using_dash_h(self):
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s '
                             '-H slave1,slave2' % PRESTO_RPM)

        self.assert_installed(self.docker_cluster.slaves[0])
        self.assert_installed(self.docker_cluster.slaves[1])
        self.assert_uninstalled(self.docker_cluster.master)
        self.assert_uninstalled(self.docker_cluster.slaves[2])

    def test_install_exclude_coord(self):
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s -x master'
                             % PRESTO_RPM)

        self.assert_uninstalled(self.docker_cluster.master)
        for slave in self.docker_cluster.slaves:
            self.assert_installed(slave)

    def test_install_exclude_worker(self):
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s -x slave1'
                             % PRESTO_RPM)
        self.assert_uninstalled(self.docker_cluster.slaves[0])
        self.assert_installed(self.docker_cluster.slaves[1])
        self.assert_installed(self.docker_cluster.master)
        self.assert_installed(self.docker_cluster.slaves[2])

    def test_install_exclude_workers(self):
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s '
                             '-x slave1,slave2' % PRESTO_RPM)

        self.assert_uninstalled(self.docker_cluster.slaves[0])
        self.assert_uninstalled(self.docker_cluster.slaves[1])
        self.assert_installed(self.docker_cluster.master)
        self.assert_installed(self.docker_cluster.slaves[2])

    def test_install_invalid_path(self):
        self.copy_presto_rpm_to_master()
        self.assertRaisesRegexp(OSError,
                                'Fatal error: error: '
                                '/mnt/presto-admin/invalid-path/presto.rpm: '
                                'open failed: No such file or directory',
                                self.run_prestoadmin,
                                'package install '
                                '/mnt/presto-admin/invalid-path/presto.rpm')

    def test_install_no_path_arg(self):
        self.copy_presto_rpm_to_master()
        output = self.run_prestoadmin('package install', raise_error=False)
        self.assertEqual(output, 'Incorrect number of arguments to task.\n\n'
                                 'Displaying detailed information for task '
                                 '\'package install\':\n\n'
                                 '    Install the rpm package on the cluster\n'
                                 '    \n    Args:\n'
                                 '        local_path: Absolute path to the rpm'
                                 ' to be installed\n        '
                                 '--nodeps (optional): Flag to indicate if '
                                 'rpm install\n'
                                 '            should ignore c'
                                 'hecking package dependencies. Equivalent\n'
                                 '            to adding --nodeps flag to rpm '
                                 '-i.\n\n')

    def test_install_already_installed(self):
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master' % PRESTO_RPM)
        self.assert_installed(self.docker_cluster.master)
        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master' % PRESTO_RPM)
        expected = [r'Deploying rpm...',
                    r'Package deployed successfully on: master',
                    r'Warning: \[master\] sudo\(\) received nonzero return'
                    r' code 1 while executing \'rpm -i '
                    r'/opt/prestoadmin/packages/%s\'!' % PRESTO_RPM,
                    r'', r'', r'\[master\] out: ',
                    r'\[master\] out: \tpackage %s is '
                    r'already installed' % PRESTO_RPM_BASENAME]

        actual = cmd_output.splitlines()
        self.assertRegexpMatchesLineByLine(actual, expected)

    def test_install_not_an_rpm(self):
        self.assertRaisesRegexp(OSError,
                                'Fatal error: error: not an rpm package',
                                self.run_prestoadmin,
                                'package install '
                                '/etc/opt/prestoadmin/config.json')

    def test_install_rpm_with_missing_jdk(self):
        self.copy_presto_rpm_to_master()
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, 'rpm -e jdk1.8.0_40-1.8.0_40-fcs')
        self.assertRaisesRegexp(OSError,
                                'package jdk1.8.0_40-1.8.0_40-fcs is not '
                                'installed',
                                self.docker_cluster.exec_cmd_on_container,
                                self.docker_cluster.master,
                                'rpm -q jdk1.8.0_40-1.8.0_40-fcs')

        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master' % PRESTO_RPM)
        self.assertRegexpMatchesLineByLine(
            cmd_output.splitlines(),
            self.jdk_not_found_error_message()
        )

    def jdk_not_found_error_message(self):
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'jdk_not_found.txt')) as f:
            jdk_not_found_error = f.read()
        jdk_not_found_error = self.escape_for_regex(jdk_not_found_error)
        return jdk_not_found_error.splitlines()

    def test_install_rpm_missing_dependency(self):
        self.copy_presto_rpm_to_master()
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, 'rpm -e --nodeps python-2.6.6')
        self.assertRaisesRegexp(OSError,
                                'package python-2.6.6 is not installed',
                                self.docker_cluster.exec_cmd_on_container,
                                self.docker_cluster.master,
                                'rpm -q python-2.6.6')

        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master'
            % PRESTO_RPM)
        expected = [r'Deploying rpm...', '', '',
                    r'Warning: \[master\] sudo\(\) received nonzero return '
                    r'code 1 while executing \'rpm -i /opt/prestoadmin/'
                    r'packages/%s\'!' % PRESTO_RPM,
                    r'Package deployed successfully on: master',
                    r'\[master\] out: error: Failed dependencies:',
                    r'\[master\] out: 	python >= 2.4 is needed by %s'
                    % PRESTO_RPM_BASENAME, r'\[master\] out: ']
        self.assertRegexpMatchesLineByLine(
            [line.rstrip() for line in cmd_output.splitlines()],
            [line.rstrip() for line in expected])

    def test_install_rpm_with_nodeps(self):
        self.copy_presto_rpm_to_master()
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, 'rpm -e --nodeps python-2.6.6')
        self.assertRaisesRegexp(OSError,
                                'package python-2.6.6 is not installed',
                                self.docker_cluster.exec_cmd_on_container,
                                self.docker_cluster.master,
                                'rpm -q python-2.6.6')

        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master --nodeps'
            % PRESTO_RPM)
        expected = 'Deploying rpm...\nPackage deployed successfully on: master' \
                   '\nPackage installed successfully on: master'

        self.assertEqualIgnoringOrder(expected, cmd_output)
