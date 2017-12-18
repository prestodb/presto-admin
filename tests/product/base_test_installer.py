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
import fnmatch
import os
import re
import subprocess

from prestoadmin import main_dir
from tests.docker_cluster import DockerCluster
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_BARE_CLUSTER
from tests.product.prestoadmin_installer import PrestoadminInstaller


class BaseTestInstaller(BaseProductTestCase):
    def setUp(self, build_or_runtime):
        super(BaseTestInstaller, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(build_or_runtime), STANDALONE_BARE_CLUSTER)
        self.centos_container = \
            self.__create_and_start_single_centos_container(build_or_runtime)
        self.pa_installer = PrestoadminInstaller(self)

    def tearDown(self):
        super(BaseTestInstaller, self).tearDown()
        self.centos_container.tear_down()

    def __create_and_start_single_centos_container(self, build_or_runtime):
        cluster_type = 'installer_tester'
        bare_image_provider = NoHadoopBareImageProvider(build_or_runtime)
        centos_container, bare_cluster = DockerCluster.start_cluster(
            bare_image_provider, cluster_type, 'master', [],
            cap_add=['NET_ADMIN'])

        if bare_cluster:
            centos_container.commit_images(bare_image_provider, cluster_type)

        return centos_container

    def _verify_third_party_dir(self, is_third_party_present):
        matches = fnmatch.filter(
            os.listdir(self.centos_container.get_dist_dir(unique=True)),
            'prestoadmin-*.tar.gz')
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
