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
Product tests for generating an online and offline installer for presto-admin
"""

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, docker_only
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestRpm(BaseProductTestCase):
    def setUp(self):

        super(TestRpm, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)

    @docker_only
    def test_install_fails_java8_not_found(self):
        installer = StandalonePrestoInstaller(self)
        for container in self.cluster.all_hosts():
            self.cluster.exec_cmd_on_host(container,
                                          'mv /usr/java/jdk1.8.0_40 /usr/')
        self.upload_topology()
        cmd_output = installer.install(pa_raise_error=False)
        actual = cmd_output.splitlines()
        num_failures = 0
        for line in enumerate(actual):
            if str(line).find('Error: Required Java version'
                              ' could not be found') != -1:
                num_failures += 1

        self.assertEqual(4, num_failures)

        for container in self.cluster.all_hosts():
            installer.assert_uninstalled(container)

    @docker_only
    def test_server_starts_java8_in_bin_java(self):
        installer = StandalonePrestoInstaller(self)
        for container in self.cluster.all_hosts():
            self.cluster.exec_cmd_on_host(container,
                                          'mv /usr/java/jdk1.8.0_40 /usr')
            self.cluster.exec_cmd_on_host(container,
                                          'ln -s /usr/jdk1.8.0_40/bin/java '
                                          '/bin/java')
        self.upload_topology()

        installer.install()

        # starts successfully with java8_home set
        output = self.run_prestoadmin('server start')
        self.assertFalse(
            'Warning: No value found for JAVA8_HOME. Default Java will be '
            'used.' in output)

    @docker_only
    def test_server_starts_no_java8_variable(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_script_from_prestoadmin_dir('rm /etc/presto/env.sh')
        # tests that no error is encountered
        self.run_prestoadmin('server start')
