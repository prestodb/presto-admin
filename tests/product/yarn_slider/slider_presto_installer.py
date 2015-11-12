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
# limitations under the License
"""
Installer for presto using slider
"""
import fnmatch
import os

import prestoadmin
from prestoadmin.slider.config import APPNAME, SLIDER_USER, HOST, HADOOP_CONF
from prestoadmin.slider.server import get_slider_bin, run_slider

from tests.base_installer import BaseInstaller
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.product.yarn_slider.slider_installer import SliderInstaller
from tests.product.yarn_slider.test_slider_installation import \
    TestSliderInstallation

PRESTO_YARN_PACKAGE_GLOB = 'presto-yarn-package*.zip'


class SliderPrestoInstaller(BaseInstaller):
    def __init__(self, testcase):
        (self.package_dir, self.package_filename) = \
            self._detect_presto_package(testcase)
        self.testcase = testcase

    @staticmethod
    def get_dependencies():
        return [PrestoadminInstaller, SliderInstaller]

    @staticmethod
    def _get_slider_master(testcase):
        return testcase.default_keywords[HOST]

    @staticmethod
    def _get_slider_user(testcase):
        return testcase.default_keywords[SLIDER_USER]

    def install(self):
        cluster = self.testcase.cluster

        package_name = self._copy_package_to_pa_host()

        cmd_output = self.testcase.run_prestoadmin(
            ' slider install ' +
            os.path.join(cluster.mount_dir, package_name),
            cluster=cluster)

        return cmd_output

    def get_keywords(self, *args, **kwargs):
        return {}

    @staticmethod
    def _detect_presto_package(testcase):
        search_dir = prestoadmin.main_dir
        package_names = fnmatch.filter(os.listdir(search_dir),
                                       PRESTO_YARN_PACKAGE_GLOB)

        testcase.assertNotEqual(0, len(package_names),
                                'No presto-yarn package found in %s' %
                                (search_dir))
        testcase.assertEqual(1, len(package_names),
                             'Multiple presto-yarn packages found in %s:\n%s'
                             % (search_dir, '\n'.join(package_names)))
        return search_dir, package_names[0]

    @staticmethod
    def assert_installed(testcase, package_filename=None):
        # Verify that the package file exists
        if package_filename is None:
            _, package_filename = \
                SliderPrestoInstaller._detect_presto_package(testcase)

        si = SliderInstaller(testcase)
        keywords = si.get_keywords()
        hdfs_cmd = 'hdfs dfs -ls %s' % (
            os.path.join('/', 'user', keywords[SLIDER_USER], '.slider',
                         'package', keywords[APPNAME],
                         os.path.basename(package_filename))
        )
        testcase.cluster.exec_cmd_on_host(
            SliderPrestoInstaller._get_slider_master(testcase), hdfs_cmd,
            SliderPrestoInstaller._get_slider_user(testcase))

        # Verify that slider thinks the package is installed
        conf = TestSliderInstallation.get_config()
        slider_cmd = "bash -c 'export HADOOP_CONF_DIR=%s ; %s package --list'" % (
            conf[HADOOP_CONF], get_slider_bin(conf))

        output = testcase.cluster.exec_cmd_on_host(
            SliderPrestoInstaller._get_slider_master(testcase), slider_cmd,
            user=SliderPrestoInstaller._get_slider_user(testcase))
        testcase.assertRegexpMatches(output, r'\b%s\b' % (conf[APPNAME]))

    def _copy_package_to_pa_host(self):
        package_path = os.path.join(self.package_dir, self.package_filename)

        self.testcase.cluster.copy_to_host(
            package_path, self.testcase.cluster.master)

        return self.package_filename
