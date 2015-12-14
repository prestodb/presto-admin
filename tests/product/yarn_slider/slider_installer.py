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
Installer for Apache Slider
"""

import os


from prestoadmin.yarn_slider.config import HOST, DIR

from tests.base_installer import BaseInstaller

from tests.product.constants import LOCAL_RESOURCES_DIR
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.product.yarn_slider.pa_slider_config import docker_config, \
    get_config, upload_config

SLIDER_DIST_FILENAME = 'slider-assembly-0.80.0-incubating-all.tar.gz'


class SliderInstaller(BaseInstaller):

    def __init__(self, testcase, override=None):
        self.testcase = testcase
        self.conf = get_config(override)

    @staticmethod
    def get_dependencies():
        return [PrestoadminInstaller]

    def install(self):
        upload_config(self.testcase.cluster, self.conf)
        slider_path = self.copy_slider_dist_to_cluster(self.testcase)
        self.install_slider_package(self.testcase, slider_path)

    def get_keywords(self):
        # The docker config has the external hostname for the slider master,
        # which is the one we need to run stuff on clusters.
        return docker_config(self.conf)

    @staticmethod
    def assert_installed(testcase, conf=get_config()):
        docker_conf = docker_config(conf)
        testcase.assert_path_exists(docker_conf[HOST],
                                os.path.join(docker_conf[DIR], 'LICENSE'))

    @staticmethod
    def copy_slider_dist_to_cluster(testcase):
        slider_filename = SLIDER_DIST_FILENAME
        slider_path = os.path.join(LOCAL_RESOURCES_DIR, slider_filename)
        testcase.cluster.copy_to_host(slider_path, testcase.cluster.master)
        return os.path.join(testcase.cluster.mount_dir, slider_filename)

    @staticmethod
    def install_slider_package(testcase, slider_path):
        testcase.run_prestoadmin(
            'slider slider_install %s' %
            (os.path.join(testcase.cluster.mount_dir, slider_path)))

