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

from nose.plugins.attrib import attr

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, \
    docker_only
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestPackageInstall(BaseProductTestCase):
    def setUp(self):
        super(TestPackageInstall, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)
        self.upload_topology()
        self.installer = StandalonePrestoInstaller(self)

    @attr('smoketest')
    def test_package_install(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        output = self.run_prestoadmin('package install '
                                      '/mnt/presto-admin/%(rpm)s',
                                      rpm=rpm_name)
        for container in self.cluster.all_hosts():
            self.installer.assert_installed(self, container,
                                            msg=output)

    def test_install_coord_using_dash_h(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%(rpm)s -H %(master)s',
            rpm=rpm_name)
        self.installer.assert_installed(self, self.cluster.master)
        for slave in self.cluster.slaves:
            self.installer.assert_uninstalled(slave, msg=output)

    def test_install_worker_using_dash_h(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%(rpm)s -H %(slave1)s',
            rpm=rpm_name)

        self.installer.assert_installed(self, self.cluster.slaves[0],
                                        msg=output)
        self.installer.assert_uninstalled(self.cluster.master, msg=output)
        self.installer.assert_uninstalled(self.cluster.slaves[1], msg=output)
        self.installer.assert_uninstalled(self.cluster.slaves[2], msg=output)

    def test_install_workers_using_dash_h(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        output = self.run_prestoadmin('package install /mnt/presto-admin/'
                                      '%(rpm)s -H %(slave1)s,%(slave2)s',
                                      rpm=rpm_name)

        self.installer.assert_installed(self, self.cluster.slaves[0],
                                        msg=output)
        self.installer.assert_installed(self, self.cluster.slaves[1],
                                        msg=output)
        self.installer.assert_uninstalled(self.cluster.master, msg=output)
        self.installer.assert_uninstalled(self.cluster.slaves[2], msg=output)

    def test_install_exclude_coord(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        output = self.run_prestoadmin('package install /mnt/presto-admin/'
                                      '%(rpm)s -x %(master)s', rpm=rpm_name)

        self.installer.assert_uninstalled(self.cluster.master, msg=output)
        for slave in self.cluster.slaves:
            self.installer.assert_installed(self, slave, msg=output)

    def test_install_exclude_worker(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        output = self.run_prestoadmin('package install /mnt/presto-admin/'
                                      '%(rpm)s -x %(slave1)s', rpm=rpm_name)
        self.installer.assert_uninstalled(self.cluster.slaves[0], msg=output)
        self.installer.assert_installed(self, self.cluster.slaves[1],
                                        msg=output)
        self.installer.assert_installed(self, self.cluster.master, msg=output)
        self.installer.assert_installed(self, self.cluster.slaves[2],
                                        msg=output)

    def test_install_exclude_workers(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        output = self.run_prestoadmin('package install /mnt/presto-admin/'
                                      '%(rpm)s -x %(slave1)s,%(slave2)s',
                                      rpm=rpm_name)

        self.installer.assert_uninstalled(self.cluster.slaves[0], msg=output)
        self.installer.assert_uninstalled(self.cluster.slaves[1], msg=output)
        self.installer.assert_installed(self, self.cluster.master, msg=output)
        self.installer.assert_installed(self, self.cluster.slaves[2],
                                        msg=output)

    def test_install_invalid_path(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        cmd_output = self.run_prestoadmin('package install /mnt/presto-admin'
                                          '/invalid-path/presto.rpm',
                                          rpm=rpm_name, raise_error=False)
        error = '\nFatal error: [%s] error: ' \
                '/mnt/presto-admin/invalid-path/presto.rpm: open failed: ' \
                'No such file or directory\n\nAborting.\n'
        expected = ''
        for host in self.cluster.all_internal_hosts():
            expected += error % host

        self.assertEqualIgnoringOrder(cmd_output, expected)

    def test_install_no_path_arg(self):
        self.installer.copy_presto_rpm_to_master()
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
        rpm_name = self.installer.copy_presto_rpm_to_master()
        self.run_prestoadmin('package install /mnt/presto-admin/%(rpm)s -H '
                             '%(master)s', rpm=rpm_name)
        self.installer.assert_installed(self, self.cluster.master)
        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%(rpm)s -H %(master)s',
            rpm=rpm_name, raise_error=False)
        expected = self.escape_for_regex(self.replace_keywords("""
Fatal error: [%(master)s] sudo() received nonzero return code 1 while \
executing!

Requested: rpm -i /opt/prestoadmin/packages/%(rpm)s
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "rpm -i \
/opt/prestoadmin/packages/%(rpm)s"

Aborting.
Deploying rpm on %(master)s...
Package deployed successfully on: %(master)s
[%(master)s] out: 	package %(rpm_basename)s is already installed
[%(master)s] out: """, **self.installer.get_keywords()))
        self.assertRegexpMatchesLineByLine(
            cmd_output.splitlines(),
            expected.splitlines()
        )

    def test_install_not_an_rpm(self):
        cmd_output = self.run_prestoadmin('package install '
                                          '/etc/opt/prestoadmin/config.json',
                                          raise_error=False)

        error = """
Fatal error: \[%s\] error: (/etc/opt/prestoadmin/config.json: )?not an rpm \
package

Aborting.
"""
        expected = ''
        for host in self.cluster.all_internal_hosts():
            expected += error % host

        self.assertRegexpMatchesLineByLine(cmd_output.splitlines(),
                                           expected.splitlines())

    @docker_only
    def test_install_rpm_missing_dependency(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        self.cluster.exec_cmd_on_host(
            self.cluster.master, 'rpm -e --nodeps python-2.6.6')
        self.assertRaisesRegexp(OSError,
                                'package python-2.6.6 is not installed',
                                self.cluster.exec_cmd_on_host,
                                self.cluster.master,
                                'rpm -q python-2.6.6')

        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%(rpm)s -H %(master)s',
            rpm=rpm_name, raise_error=False)
        expected = self.replace_keywords("""
Fatal error: [%(master)s] sudo() received nonzero return code 1 while \
executing!

Requested: rpm -i /opt/prestoadmin/packages/%(rpm)s
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "rpm -i \
/opt/prestoadmin/packages/%(rpm)s"

Aborting.
Deploying rpm on %(master)s...
Package deployed successfully on: %(master)s
[%(master)s] out: error: Failed dependencies:
[%(master)s] out: 	python >= 2.4 is needed by %(rpm_basename)s
[%(master)s] out: """, **self.installer.get_keywords())
        self.assertRegexpMatchesLineByLine(
            cmd_output.splitlines(),
            self.escape_for_regex(expected).splitlines()
        )

    @docker_only
    def test_install_rpm_with_nodeps(self):
        rpm_name = self.installer.copy_presto_rpm_to_master()
        self.cluster.exec_cmd_on_host(
            self.cluster.master, 'rpm -e --nodeps python-2.6.6')
        self.assertRaisesRegexp(OSError,
                                'package python-2.6.6 is not installed',
                                self.cluster.exec_cmd_on_host,
                                self.cluster.master,
                                'rpm -q python-2.6.6')

        cmd_output = self.run_prestoadmin(
            'package install /mnt/presto-admin/%(rpm)s -H %(master)s --nodeps',
            rpm=rpm_name
        )
        expected = 'Deploying rpm on %(host)s...\n' \
                   'Package deployed successfully on: %(host)s\n' \
                   'Package installed successfully on: %(host)s' \
                   % {'host': self.cluster.internal_master}

        self.assertEqualIgnoringOrder(expected, cmd_output)
