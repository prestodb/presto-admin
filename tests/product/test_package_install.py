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
    LOCAL_RESOURCES_DIR


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
        cmd_output = self.run_prestoadmin('package install /mnt/presto-admin'
                                          '/invalid-path/presto.rpm')
        error = '\nFatal error: [%s] error: ' \
                '/mnt/presto-admin/invalid-path/presto.rpm: open failed: ' \
                'No such file or directory\n\nAborting.\n'
        expected = ''
        for host in self.docker_cluster.all_hosts():
            expected += error % host

        self.assertEqualIgnoringOrder(cmd_output, expected)

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
            'package install /mnt/presto-admin/%s -H %s' %
            (PRESTO_RPM, self.docker_cluster.master))
        self.assert_installed(self.docker_cluster.master)
        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%s -H %s' %
            (PRESTO_RPM, self.docker_cluster.master))
        expected = """
Fatal error: [%(host)s] sudo() received nonzero return code 1 while executing!

Requested: rpm -i /opt/prestoadmin/packages/{rpm}
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "rpm -i \
/opt/prestoadmin/packages/{rpm}"

Aborting.
Deploying rpm on %(host)s...
Package deployed successfully on: %(host)s
[%(host)s] out: 	package {rpm_basename} is already installed
[%(host)s] out: """ % {'host': self.docker_cluster.master}

        self.assertRegexpMatchesLineByLine(
            cmd_output.splitlines(),
            self.escape_for_regex(expected).splitlines())

    def test_install_not_an_rpm(self):
        cmd_output = self.run_prestoadmin('package install '
                                          '/etc/opt/prestoadmin/config.json')

        error = """
Fatal error: [%s] error: not an rpm package

Aborting.
"""
        expected = ''
        for host in self.docker_cluster.all_hosts():
            expected += error % host

        self.assertEqualIgnoringOrder(cmd_output, expected)

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
            self.jdk_not_found_error_message().splitlines()
        )

    def jdk_not_found_error_message(self):
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'jdk_not_found.txt')) as f:
            jdk_not_found_error = f.read()
        jdk_not_found_error = jdk_not_found_error % {
            'host': self.docker_cluster.master}
        return self.escape_for_regex(jdk_not_found_error)

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
        expected = """
Fatal error: [%(host)s] sudo() received nonzero return code 1 while executing!

Requested: rpm -i /opt/prestoadmin/packages/{rpm}
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "rpm -i \
/opt/prestoadmin/packages/{rpm}"

Aborting.
Deploying rpm on %(host)s...
Package deployed successfully on: %(host)s
[%(host)s] out: error: Failed dependencies:
[%(host)s] out: 	python >= 2.4 is needed by {rpm_basename}
[%(host)s] out: """ % {'host': self.docker_cluster.master}
        self.assertRegexpMatchesLineByLine(
            cmd_output.splitlines(),
            self.escape_for_regex(expected).splitlines())

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
        expected = 'Deploying rpm on %(host)s...\n' \
                   'Package deployed successfully on: %(host)s\n' \
                   'Package installed successfully on: %(host)s' \
                   % {'host': self.docker_cluster.master}

        self.assertEqualIgnoringOrder(expected, cmd_output)
