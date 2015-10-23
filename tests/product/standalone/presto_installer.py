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
from tests.product.constants import LOCAL_RESOURCES_DIR

from tests.docker_cluster import DockerCluster
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.product.topology_installer import TopologyInstaller

RPM_BASENAME = r'presto.*'
PRESTO_RPM_GLOB = r'presto*.rpm'

DUMMY_RPM_NAME = 'dummy-rpm.rpm'


class StandalonePrestoInstaller(object):

    def __init__(self, testcase):
        self.presto_rpm_filename = self._detect_presto_rpm()
        self.testcase = testcase

    @staticmethod
    def get_dependencies():
        return [PrestoadminInstaller, TopologyInstaller]

    def install(self, dummy=False, extra_configs=None):
        cluster = self.testcase.cluster
        if dummy:
            rpm_dir = LOCAL_RESOURCES_DIR
            rpm_name = DUMMY_RPM_NAME
        else:
            rpm_dir = prestoadmin.main_dir
            rpm_name = self.presto_rpm_filename

        rpm_name = self.copy_presto_rpm_to_master(rpm_dir=rpm_dir,
                                                  rpm_name=rpm_name,
                                                  cluster=cluster)

        self.testcase.write_test_configs(cluster, extra_configs)
        cmd_output = self.testcase.run_prestoadmin(
            'server install ' +
            os.path.join(cluster.mount_dir, rpm_name),
            cluster=cluster
        )

        self.testcase.default_keywords.update(self.get_keywords(rpm_name))

        return cmd_output

    def get_keywords(self, rpm_name):
        return {
            'rpm': rpm_name,
            'rpm_basename': RPM_BASENAME,
            'rpm_basename_without_arch': self.presto_rpm_filename[:-11],
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
                container, 'rpm -q presto-server-rpm')
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

    def copy_presto_rpm_to_master(self, rpm_dir=LOCAL_RESOURCES_DIR,
                                  rpm_name=DUMMY_RPM_NAME, cluster=None):
        if not cluster:
            cluster = self.testcase.cluster

        rpm_path = os.path.join(rpm_dir, rpm_name)
        try:
            cluster.copy_to_host(rpm_path, cluster.master)
            self._check_if_corrupted_rpm(rpm_name, cluster)
        except OSError:
            #
            # Can't retry downloading the dummy rpm. It's a local resource.
            # If we've gotten here, we've corrupted it somehow.
            #
            self.testcase.assertNotEqual(rpm_name, DUMMY_RPM_NAME,
                                         "Bad dummy rpm!")

            print 'Downloading RPM again'
            # try to download the RPM again if it's corrupt (but only once)
            StandalonePrestoInstaller._download_rpm()
            cluster.copy_to_host(rpm_path, cluster.master)
            self._check_if_corrupted_rpm(rpm_name, cluster)
        return rpm_name

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
            return sorted(rpm_names)[-1]
        else:
            try:
                return StandalonePrestoInstaller._download_rpm()
            except:
                # retry once
                return StandalonePrestoInstaller._download_rpm()

    @staticmethod
    def _check_if_corrupted_rpm(rpm_name, cluster):
        cluster.exec_cmd_on_host(
            cluster.master, 'rpm -K --nosignature '
                            + os.path.join(cluster.mount_dir, rpm_name)
        )

    def assert_uninstalled(self, container, msg=None):
        self.testcase.assertRaisesRegexp(
            OSError,
            'package presto-server-rpm is not installed',
            self.testcase.cluster.exec_cmd_on_host, container,
            'rpm -q presto-server-rpm', msg=msg)
