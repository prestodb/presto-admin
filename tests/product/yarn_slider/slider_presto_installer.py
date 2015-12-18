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
import urllib

import prestoadmin
from prestoadmin.yarn_slider.config import APPNAME, SLIDER_USER, HOST, \
    HADOOP_CONF
from prestoadmin.yarn_slider.server import get_slider_bin

from tests.base_installer import BaseInstaller
from tests.product.mode_installers import YarnSliderModeInstaller
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.product.yarn_slider.slider_installer import SliderInstaller
from tests.product.yarn_slider.pa_slider_config import get_config

# If you have the misfortune of having to fix the download link in the future,
# be aware that you're in for a treat.
#
# A couple of hard-learned lessons:
# repository.sonatype.org and oss.sonatype.org have different groups. To get a
# listing, go to https://oss.sonatype.org/content/groups/public/com/
# Make sure teradata is listed there; if it isn't, you're barking up the wrong
# tree
# The complete group is com.teradata.presto-yarn
# The complete package is presto-yarn-package
#
# You should be able to browse to
# https://oss.sonatype.org/content/groups/public/com/teradata/presto-yarn
# and follow links to verify that the presto-yarn-package exists and that there
# are builds.
#
# Other places to try to get a toehold:
# https://oss.sonatype.org will get you a web GUI, with which you can search
# for the presto-yarn-package
#
# Documentation for the REST API:
# https://oss.sonatype.org/nexus-restlet1x-plugin/default/docs/index.html
PRESTO_YARN_PACKAGE_GLOB = 'presto-yarn-package*.zip'
PRESTO_YARN_PACKAGE_URL = \
    'https://oss.sonatype.org/service/local/artifact/maven/redirect' \
    '?r=snapshots&g=com.teradata.presto-yarn' \
    '&a=presto-yarn-package&e=zip&v=LATEST'


class SliderPrestoInstaller(BaseInstaller):
    def __init__(self, testcase):
        (self.package_dir, self.package_filename) = \
            self._detect_presto_package(testcase)
        self.testcase = testcase

    @staticmethod
    def get_dependencies():
        return [PrestoadminInstaller, YarnSliderModeInstaller, SliderInstaller]

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
            ' server install ' +
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

        if len(package_names) == 0:
            print 'No presto-yarn package found in %s. Downloading' % \
                  (search_dir)
            SliderPrestoInstaller._download_package(testcase)
            package_names = fnmatch.filter(os.listdir(search_dir),
                                           PRESTO_YARN_PACKAGE_GLOB)

        testcase.assertEqual(
            1, len(package_names),
            'Found %s presto-yarn packages in %s. Expected exactly 1:\n%s'
            % (len(package_names), search_dir, '\n'.join(package_names)))

        return search_dir, package_names[0]

    @staticmethod
    def _download_package(testcase):
        package_filename = 'presto-yarn-package.zip'
        package_path = os.path.join(prestoadmin.main_dir, package_filename)

        urllib.urlretrieve(PRESTO_YARN_PACKAGE_URL, package_path)
        return package_filename

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
        conf = get_config()
        slider_cmd = \
            "bash -c 'export HADOOP_CONF_DIR=%s ; %s package --list'" % (
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
