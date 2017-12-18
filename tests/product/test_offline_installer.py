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

from tests.product.base_product_case import docker_only
from tests.product.base_test_installer import BaseTestInstaller


class TestOfflineInstaller(BaseTestInstaller):
    def setUp(self):
        super(TestOfflineInstaller, self).setUp("runtime")

    @attr('smoketest', 'offline_installer')
    @docker_only
    def test_offline_installer(self):
        self.pa_installer._build_installer_in_docker(
            self.centos_container, online_installer=False, unique=True)
        self._verify_third_party_dir(True)
        self.centos_container.exec_cmd_on_host(
            # IMPORTANT: ifdown eth0 fails silently without taking the
            # interface down if the NET_ADMIN capability isn't set for the
            # container. ifconfig eth0 down accomplishes the same thing, but
            # results in a failure if it fails.
            self.centos_container.master, 'ifconfig eth0 down')
        self.pa_installer.install(
            dist_dir=self.centos_container.get_dist_dir(unique=True))
        self.run_prestoadmin('--help', raise_error=True)
