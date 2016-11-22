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
Module for setting the topology on the presto-admin host prior to installing
presto
"""

from tests.base_installer import BaseInstaller
from tests.product.config_dir_utils import get_config_file_path


class TopologyInstaller(BaseInstaller):
    def __init__(self, testcase):
        self.testcase = testcase

    @staticmethod
    def get_dependencies():
        return []

    def install(self):
        self.testcase.upload_topology(cluster=self.testcase.cluster)

    @staticmethod
    def assert_installed(testcase, msg=None):
        testcase.cluster.exec_cmd_on_host(
            testcase.cluster.master,
            'test -r %s' % get_config_file_path())

    def get_keywords(self):
        return {}
