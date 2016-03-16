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
import urllib

import prestoadmin

from tests.base_installer import BaseInstaller
from tests.docker_cluster import DockerCluster
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
            'server install ' + os.path.join(cluster.mount_dir, rpm_name),
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
            container = cluster.get_master()

        try:
            check_rpm = cluster.exec_cmd_on_host(
                container, 'rpm -q %s' % (PACKAGE_NAME,))
            testcase.assertRegexpMatches(
                check_rpm, RPM_BASENAME + '\n', msg=msg
            )
        except OSError as e:
            if isinstance(testcase.cluster, DockerCluster):
                cluster.client.commit(cluster.master, 'db_error_master')
                cluster.client.commit(cluster.slaves[0], 'db_error_slave0')
                cluster.client.commit(cluster.slaves[1], 'db_error_slave1')
                cluster.client.commit(cluster.slaves[2], 'db_error_slave2')
            if msg:
                error_message = e.strerror + '\n' + msg
            else:
                error_message = e.strerror
            testcase.fail(msg=error_message)

    def copy_presto_rpm_to_master(self, cluster=None):
        if not cluster:
            cluster = self.testcase.cluster

        rpm_path = os.path.join(self.rpm_dir, self.rpm_name)
        try:
            cluster.copy_to_host(rpm_path, cluster.master)
            self._check_if_corrupted_rpm(self.rpm_name, cluster)
        except OSError:
            print 'Downloading RPM again'
            # try to download the RPM again if it's corrupt (but only once)
            StandalonePrestoInstaller._download_rpm()
            cluster.copy_to_host(rpm_path, cluster.master)
            self._check_if_corrupted_rpm(self.rpm_name, cluster)
        return self.rpm_name

    @staticmethod
    def _download_rpm():
        rpm_filename = 'presto-server-rpm.rpm'
        rpm_path = os.path.join(prestoadmin.main_dir,
                                rpm_filename)
        urllib.urlretrieve(
            'https://repository.sonatype.org/service/local/artifact/maven'
            '/content?r=central-proxy&g=com.facebook.presto'
            '&a=presto-server-rpm&e=rpm&v=RELEASE', rpm_path)
        return rpm_filename

    @staticmethod
    def _detect_presto_rpm():
        """
        Detects the Presto RPM in the main directory of presto-admin.
        Returns the name of the RPM, if it exists, else returns None.
        """
        rpm_names = fnmatch.filter(os.listdir(prestoadmin.main_dir),
                                   PRESTO_RPM_GLOB)
        if rpm_names:
            # Choose the last RPM name if you sort the list, since if there
            # are multiple RPMs, the last one is probably the latest
            rpm_name = sorted(rpm_names)[-1]
        else:
            try:
                rpm_name = StandalonePrestoInstaller._download_rpm()
            except:
                # retry once
                rpm_name = StandalonePrestoInstaller._download_rpm()

        return prestoadmin.main_dir, rpm_name

    @staticmethod
    def _check_if_corrupted_rpm(rpm_name, cluster):
        cluster.exec_cmd_on_host(
            cluster.master, 'rpm -K --nosignature ' +
                            os.path.join(cluster.mount_dir, rpm_name)
        )

    def assert_uninstalled(self, container, msg=None):
        failure_msg = 'package %s is not installed' % (PACKAGE_NAME,)
        rpm_cmd = 'rpm -q %s' % (PACKAGE_NAME,)

        self.testcase.assertRaisesRegexp(
            OSError,
            failure_msg,
            self.testcase.cluster.exec_cmd_on_host, container,
            rpm_cmd, msg=msg)
