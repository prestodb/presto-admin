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
    @attr('smoketest')
    def test_package_install(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s'
                             % PRESTO_RPM)
        for container in self.all_hosts():
            self.assert_installed(container)

    def assert_installed(self, container):
        check_rpm = self.exec_create_start(container, 'rpm -q presto')
        self.assertEqual(PRESTO_RPM[:-4] + '\n', check_rpm)

    def assert_uninstalled(self, container):
        self.assertRaisesRegexp(OSError, 'package presto is not installed',
                                self.exec_create_start,
                                container, 'rpm -q presto')

    def test_install_coord_using_h(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s -H master'
                             % PRESTO_RPM)
        self.assert_installed(self.master)
        for slave in self.slaves:
            self.assert_uninstalled(slave)

    def test_install_worker_using_h(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s '
                             '-H slave1' % PRESTO_RPM)

        self.assert_installed(self.slaves[0])
        self.assert_uninstalled(self.master)
        self.assert_uninstalled(self.slaves[1])
        self.assert_uninstalled(self.slaves[2])

    def test_install_workers_using_h(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s '
                             '-H slave1,slave2' % PRESTO_RPM)

        self.assert_installed(self.slaves[0])
        self.assert_installed(self.slaves[1])
        self.assert_uninstalled(self.master)
        self.assert_uninstalled(self.slaves[2])

    def test_install_exclude_coord(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s -x master'
                             % PRESTO_RPM)

        self.assert_uninstalled(self.master)
        for slave in self.slaves:
            self.assert_installed(slave)

    def test_install_exclude_worker(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s -x slave1'
                             % PRESTO_RPM)
        self.assert_uninstalled(self.slaves[0])
        self.assert_installed(self.slaves[1])
        self.assert_installed(self.master)
        self.assert_installed(self.slaves[2])

    def test_install_exclude_workers(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%s '
                             '-x slave1,slave2' % PRESTO_RPM)

        self.assert_uninstalled(self.slaves[0])
        self.assert_uninstalled(self.slaves[1])
        self.assert_installed(self.master)
        self.assert_installed(self.slaves[2])

    def test_install_invalid_path(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.assertRaisesRegexp(OSError,
                                'Fatal error: error: '
                                '/mnt/presto-admin/invalid-path/presto.rpm: '
                                'open failed: No such file or directory',
                                self.run_prestoadmin,
                                'package install '
                                '/mnt/presto-admin/invalid-path/presto.rpm')

    def test_install_no_path_arg(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        output = self.run_prestoadmin('package install', raise_error=False)
        self.assertEqual(output, "Incorrect number of arguments to task.\n\n"
                                 "Displaying detailed information for task "
                                 "'package install':\n\n"
                                 "    Install the rpm package on the cluster\n"
                                 "    \n    Args:\n"
                                 "        local_path: Absolute path to the rpm"
                                 " to be installed\n        "
                                 "--nodeps (optional): Flag to indicate if "
                                 "rpm install\n"
                                 "            should ignore c"
                                 "hecking package dependencies. Equivalent\n"
                                 "            to adding --nodeps flag to rpm "
                                 "-i.\n\n")

    def test_install_already_installed(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master' % PRESTO_RPM)
        self.assert_installed(self.master)
        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master' % PRESTO_RPM)
        expected = ['Deploying rpm...',
                    'Package deployed successfully on: master',
                    "Warning: [master] sudo() received nonzero return code 1 "
                    "while executing 'rpm -i "
                    "/opt/prestoadmin/packages/%s'!" % PRESTO_RPM,
                    '', '', '[master] out: ',
                    '[master] out: \tpackage %s is '
                    'already installed' % PRESTO_RPM_BASENAME]

        actual = cmd_output.splitlines()
        self.assertEqual(sorted(expected), sorted(actual))

    def test_install_not_an_rpm(self):
        self.install_presto_admin()
        self.upload_topology()
        self.assertRaisesRegexp(OSError,
                                'Fatal error: error: not an rpm package',
                                self.run_prestoadmin,
                                'package install '
                                '/etc/opt/prestoadmin/config.json')

    def test_install_rpm_with_missing_jdk(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.exec_create_start(self.master, 'rpm -e jdk1.8.0_40-1.8.0_40-fcs')
        self.assertRaisesRegexp(OSError,
                                'package jdk1.8.0_40-1.8.0_40-fcs is not '
                                'installed',
                                self.exec_create_start,
                                self.master, 'rpm -q jdk1.8.0_40-1.8.0_40-fcs')

        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master' % PRESTO_RPM)
        self.assertEqualIgnoringOrder(
            self.jdk_not_found_error_message(), cmd_output)

    def jdk_not_found_error_message(self):
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'jdk_not_found.txt')) as f:
            jdk_not_found_error = f.read()
        return jdk_not_found_error

    def test_install_rpm_missing_dependency(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.exec_create_start(self.master, 'rpm -e --nodeps python-2.6.6')
        self.assertRaisesRegexp(OSError,
                                'package python-2.6.6 is not installed',
                                self.exec_create_start,
                                self.master, 'rpm -q python-2.6.6')

        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master'
            % PRESTO_RPM)
        expected = 'Deploying rpm...\n\nWarning: [master] sudo() received ' \
                   'nonzero return code 1 while executing ' \
                   '\'rpm -i /opt/prestoadmin/packages/' \
                   '%s\'!\n\nPackage deployed ' \
                   'successfully on: master\n[master] out: error: ' \
                   'Failed dependencies:\n[master] out: 	python >= 2.6 is ' \
                   'needed by %s\n[master] out: 	' \
                   'python <= 2.7 is needed by %s\n' \
                   '[master] out: ' % (PRESTO_RPM, PRESTO_RPM_BASENAME,
                                       PRESTO_RPM_BASENAME)
        self.assertEqualIgnoringOrder(expected, cmd_output)

    def test_install_rpm_with_nodeps(self):
        self.install_presto_admin()
        self.upload_topology()
        self.copy_presto_rpm_to_master()
        self.exec_create_start(self.master, 'rpm -e --nodeps python-2.6.6')
        self.assertRaisesRegexp(OSError,
                                'package python-2.6.6 is not installed',
                                self.exec_create_start,
                                self.master, 'rpm -q python-2.6.6')

        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H master --nodeps'
            % PRESTO_RPM)
        expected = 'Deploying rpm...\nPackage deployed successfully on: master' \
                   '\nPackage installed successfully on: master'

        self.assertEqualIgnoringOrder(expected, cmd_output)
