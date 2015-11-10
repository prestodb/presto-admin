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
Installer for slider
"""

from tests.base_installer import BaseInstaller
from tests.product.prestoadmin_installer import PrestoadminInstaller


class SliderInstaller(BaseInstaller):

    def __init__(self, testcase, override=None):
        from tests.product.yarn_slider.test_slider_installation import \
            TestSliderInstallation
        self.testcase = testcase
        self.conf = TestSliderInstallation.get_config(override)

    @staticmethod
    def get_dependencies():
        return [PrestoadminInstaller]

    def install(self):
        from tests.product.yarn_slider.test_slider_installation import \
            TestSliderInstallation
        tsi = TestSliderInstallation
        tsi.upload_config(self.testcase.cluster, self.conf)
        slider_path = tsi.copy_slider_dist_to_cluster(self.testcase)
        tsi.install_slider_package(self.testcase, slider_path)

    def get_keywords(self):
        from tests.product.yarn_slider.test_slider_installation import \
            TestSliderInstallation
        # The docker config has the external hostname,for the slider master,
        # which is the one we need to run stuff on clusters.
        return TestSliderInstallation.docker_config(self.conf)

    @staticmethod
    def assert_installed(testcase):
        from tests.product.yarn_slider.test_slider_installation import \
            TestSliderInstallation
        tsi = TestSliderInstallation
        conf = tsi.get_config()
        tsi.assert_slider_installed(testcase, conf)
