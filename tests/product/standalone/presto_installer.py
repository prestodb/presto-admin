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
Module for installing presto on a cluster.
"""

import fnmatch
import os

import prestoadmin

from tests.base_installer import BaseInstaller
from tests.product.mode_installers import StandaloneModeInstaller
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.product.topology_installer import TopologyInstaller

RPM_BASENAME = r'presto.*'
PRESTO_RPM_GLOB = r'presto*.rpm'

PACKAGE_NAME = 'presto-server-rpm'


class StandalonePrestoInstaller(BaseInstaller):
    def __init__(self, testcase, rpm_location=None):
        if rpm_location:
            self.rpm_dir, self.rpm_name = rpm_location
        else:
            self.rpm_dir, self.rpm_name = self._detect_presto_rpm()

        self.testcase = testcase

    @staticmethod
    def get_dependencies():
        return [PrestoadminInstaller, StandaloneModeInstaller,
                TopologyInstaller]

    def install(self, extra_configs=None, coordinator=None,
                pa_raise_error=True):
        cluster = self.testcase.cluster
        rpm_name = self.copy_presto_rpm_to_master(cluster=cluster)

        self.testcase.write_test_configs(cluster, extra_configs, coordinator)
        cmd_output = self.testcase.run_prestoadmin(
            'server install ' + os.path.join(cluster.rpm_cache_dir, rpm_name),
            cluster=cluster, raise_error=pa_raise_error
        )

        return cmd_output

    def get_keywords(self):
        return {
            'rpm': self.rpm_name,
            'rpm_basename': RPM_BASENAME,
        }

    @staticmethod
    def assert_installed(testcase, container=None, msg=None, cluster=None):
        # cluster keyword arg supports configurable cluster, which needs to
        # assert that presto isn't installed before testcase.cluster is set.
        if not cluster:
            cluster = testcase.cluster

        # container keyword arg supports test_package_install and a few other
        # places where we need to check specific members of a cluster.
        if not container:
            container = cluster.master

        try:
            check_rpm = cluster.exec_cmd_on_host(
                container, 'rpm -q %s' % (PACKAGE_NAME,))
            testcase.assertRegexpMatches(
                check_rpm, RPM_BASENAME + '\n', msg=msg
            )
        except OSError as e:
            if msg:
                error_message = e.strerror + '\n' + msg
            else:
                error_message = e.strerror
            testcase.fail(msg=error_message)

    def copy_presto_rpm_to_master(self, cluster=None):
        if not cluster:
            cluster = self.testcase.cluster

        rpm_path = os.path.join(self.rpm_dir, self.rpm_name)
        if not self._check_rpm_already_uploaded(self.rpm_name, cluster):
            cluster.copy_to_host(rpm_path, cluster.master, dest_path=os.path.join(cluster.rpm_cache_dir,
                                                                                  self.rpm_name))
        self._check_if_corrupted_rpm(self.rpm_name, cluster)
        return self.rpm_name

    @staticmethod
    def _detect_presto_rpm():
        """
        Detects the Presto RPM in the main directory of presto-admin.
        Returns the name of the RPM, if it exists, else raises an OSError.
        """
        rpm_names = fnmatch.filter(os.listdir(prestoadmin.main_dir),
                                   PRESTO_RPM_GLOB)
        if rpm_names:
            # Choose the last RPM name if you sort the list, since if there
            # are multiple RPMs, the last one is probably the latest
            rpm_name = sorted(rpm_names)[-1]
        else:
            raise OSError(1, 'Presto RPM not detected.')

        return prestoadmin.main_dir, rpm_name

    @staticmethod
    def _check_if_corrupted_rpm(rpm_name, cluster):
        cluster.exec_cmd_on_host(
            cluster.master, 'rpm -K --nosignature ' +
                            os.path.join(cluster.rpm_cache_dir, rpm_name)
        )

    def assert_uninstalled(self, container, msg=None):
        failure_msg = 'package %s is not installed' % (PACKAGE_NAME,)
        rpm_cmd = 'rpm -q %s' % (PACKAGE_NAME,)

        self.testcase.assertRaisesRegexp(
            OSError,
            failure_msg,
            self.testcase.cluster.exec_cmd_on_host, container,
            rpm_cmd, msg=msg)

    @staticmethod
    def _check_rpm_already_uploaded(rpm_name, cluster):
        rpm_already_exists = True
        try:
            cluster.exec_cmd_on_host(
                cluster.master,
                'ls ' + os.path.join(cluster.rpm_cache_dir, rpm_name)
            )
        except OSError:
            rpm_already_exists = False
        return rpm_already_exists
