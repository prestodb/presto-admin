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
import subprocess
import os
import fnmatch
import re
from nose.plugins.attrib import attr

from prestoadmin import main_dir
from tests.docker_cluster import DockerCluster, DEFAULT_LOCAL_MOUNT_POINT, \
    DEFAULT_DOCKER_MOUNT_POINT
from tests.product.base_product_case import BaseProductTestCase, docker_only
from tests.product.constants import LOCAL_RESOURCES_DIR
from tests.product.prestoadmin_installer import PrestoadminInstaller


class TestInstaller(BaseProductTestCase):

    def setUp(self):
        super(TestInstaller, self).setUp()
        self.centos_container = \
            self.__create_and_start_single_centos_container()
        self.pa_installer = PrestoadminInstaller(self)

    def tearDown(self):
        super(TestInstaller, self).tearDown()
        self.centos_container.tear_down()

    @attr('smoketest')
    @docker_only
    def test_online_installer(self):
        self.pa_installer._build_installer_in_docker(self.centos_container,
                                                     online_installer=True,
                                                     unique=True)
        self.__verify_third_party_dir(False)
        self.pa_installer.install(
            dist_dir=self.centos_container.get_dist_dir(unique=True))
        self.run_prestoadmin('--help', raise_error=True,
                             cluster=self.centos_container)

    @attr('smoketest')
    @docker_only
    def test_offline_installer(self):
        self.pa_installer._build_installer_in_docker(
            self.centos_container, online_installer=False, unique=True)
        self.__verify_third_party_dir(True)
        self.centos_container.exec_cmd_on_host(
            self.centos_container.master, 'ifdown eth0')
        self.pa_installer.install(
            dist_dir=self.centos_container.get_dist_dir(unique=True))
        self.run_prestoadmin('--help', raise_error=True,
                             cluster=self.centos_container)

    def __create_and_start_single_centos_container(self):
        centos_container = DockerCluster(
            'master', [], DEFAULT_LOCAL_MOUNT_POINT,
            DEFAULT_DOCKER_MOUNT_POINT)
        # we can't assume that another test has created the image
        centos_container.create_image(
            os.path.join(LOCAL_RESOURCES_DIR, 'centos6-ssh-test'),
            'teradatalabs/centos6-ssh-test',
            'jdeathe/centos-ssh'
        )
        centos_container.start_containers(
            'teradatalabs/centos6-ssh-test',
            cap_add=['NET_ADMIN']
        )
        return centos_container

    def __verify_third_party_dir(self, is_third_party_present):
        matches = fnmatch.filter(
            os.listdir(self.centos_container.get_dist_dir(unique=True)),
            'prestoadmin-*.tar.bz2')
        if len(matches) > 1:
            raise RuntimeError(
                'More than one archive found in the dist directory ' +
                ' '.join(matches)
            )
        cmd_to_run = ['tar', '-tf',
                      os.path.join(
                          self.centos_container.get_dist_dir(unique=True),
                          matches[0])
                      ]
        popen_obj = subprocess.Popen(cmd_to_run,
                                     cwd=main_dir, stdout=subprocess.PIPE)
        retcode = popen_obj.returncode
        if retcode:
            raise RuntimeError('Non zero return code when executing ' +
                               ' '.join(cmd_to_run))
        stdout = popen_obj.communicate()[0]
        match = re.search('/third-party/', stdout)
        if is_third_party_present and match is None:
            raise RuntimeError('Expected to have an offline installer with '
                               'a third-party directory. Found no '
                               'third-party directory in the installer '
                               'archive.')
        elif not is_third_party_present and match:
            raise RuntimeError('Expected to have an online installer with no '
                               'third-party directory. Found a third-party '
                               'directory in the installer archive.')
