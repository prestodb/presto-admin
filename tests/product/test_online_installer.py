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

from nose.plugins.attrib import attr

from tests.product.base_test_installer import BaseTestInstaller


class TestOnlineInstaller(BaseTestInstaller):
    def setUp(self):
        # for online installer we need to install on "build" cluster
        # as essentially building presto is part of installation process
        super(TestOnlineInstaller, self).setUp("build")

    @attr('smoketest')
    def test_online_installer(self):
        self.pa_installer._build_installer_in_docker(self.centos_container,
                                                     online_installer=True,
                                                     unique=True)
        self._verify_third_party_dir(False)
        self.pa_installer.install(
            dist_dir=self.centos_container.get_dist_dir(unique=True))
        self.run_prestoadmin('--help', raise_error=True)
