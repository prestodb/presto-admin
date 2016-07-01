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
Module for installing prestoadmin on a cluster.
"""

import fnmatch
import os

from tests.base_installer import BaseInstaller
from tests.product.constants import LOCAL_RESOURCES_DIR


class PrestoadminInstaller(BaseInstaller):
    def __init__(self, testcase):
        self.testcase = testcase

    @staticmethod
    def get_dependencies():
        return []

    def install(self, cluster=None, dist_dir=None):
        if not cluster:
            cluster = self.testcase.cluster

        if not dist_dir:
            dist_dir = cluster.get_dist_dir(unique=False)

        self.copy_dist_to_master(cluster, dist_dir)
        cluster.copy_to_host(
            LOCAL_RESOURCES_DIR + "/install-admin.sh", cluster.master)
        cluster.exec_cmd_on_host(
            cluster.master,
            'chmod +x ' + cluster.mount_dir + "/install-admin.sh"
        )
        cluster.exec_cmd_on_host(
            cluster.master, cluster.mount_dir + "/install-admin.sh")

    @staticmethod
    def assert_installed(testcase, msg=None):
        cluster = testcase.cluster
        cluster.exec_cmd_on_host(cluster.get_master(),
                                 'test -x /opt/prestoadmin/presto-admin')

    def get_keywords(self):
        return {}

    def copy_dist_to_master(self, cluster, dist_dir):
        if (not os.path.isdir(dist_dir) or
                len(fnmatch.filter(
                    os.listdir(dist_dir),
                    'prestoadmin-*.tar.bz2')) == 0):
            self.testcase.fail(
                'Unable to find presto-admin package. Have you run one of '
                '`make dist*` or `make docker-dist*`?')
        for dist_file in os.listdir(dist_dir):
            if fnmatch.fnmatch(dist_file, "prestoadmin-*.tar.bz2"):
                cluster.copy_to_host(
                    os.path.join(dist_dir, dist_file),
                    cluster.master)
