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
# limitations under the License
"""
Tests for the server install sub command.
"""

from errno import ECONNREFUSED
from socket import socket
from StringIO import StringIO
import time

from prestoadmin.config import get_conf_from_json
from prestoadmin.yarn_slider.config import SLIDER_CONFIG_PATH, PRESTO_PACKAGE

from tests.product.base_product_case import BaseProductTestCase
from tests.hdp_bare_image_provider import HdpBareImageProvider
from tests.product.yarn_slider.slider_presto_installer import \
    SliderPrestoInstaller

HDFS_PORT = 8020


class TestServerInstall(BaseProductTestCase):
    def setUp(self):
        super(TestServerInstall, self).setUp()
        self.setup_cluster(HdpBareImageProvider(), self.PA_SLIDER_CLUSTER)
        self.installer = SliderPrestoInstaller(self)
        self.await_hdfs()

    def await_hdfs(self):
        start = time.clock()
        ip_addr = self.cluster.get_ip_address_dict()[self.cluster.master]
        while True:
            try:
                s = socket()
                s.connect((ip_addr, HDFS_PORT))
                break
            except Exception as e:
                s.close()
                if e.errno == ECONNREFUSED:
                    pass
                else:
                    raise
            finally:
                s.close()
        end = time.clock()
        duration = end - start
        print 'Waited %.3f seconds for hdfs' % (duration)

    def assert_uninstalled(self):
        self.assertRaisesRegexp(OSError, 'No such file or directory',
                                self.installer.assert_installed, self)

    def get_config(self):
        slider_master = self.installer._get_slider_master(self)
        conf = self.get_file_content(slider_master, SLIDER_CONFIG_PATH)
        conf_file = StringIO(conf)
        return get_conf_from_json(conf_file)

    def assert_config_has_package(self):
        conf_dict = self.get_config()
        self.assertIn(PRESTO_PACKAGE, conf_dict)

    def assert_config_no_package(self):
        conf_dict = self.get_config()
        self.assertNotIn(PRESTO_PACKAGE, conf_dict)

    def test_install(self):
        self.assert_uninstalled()
        self.installer.install()
        self.installer.assert_installed(self)
        self.assert_config_has_package()

    def test_install_twice(self):
        self.assert_uninstalled()
        self.installer.install()
        self.installer.assert_installed(self)
        self.assertRaisesRegexp(OSError, "Package exists",
                                self.installer.install)

    def test_uninstall(self):
        self.assert_uninstalled()
        self.installer.install()
        self.installer.assert_installed(self)
        self.assert_config_has_package()
        self.uninstall()
        self.assert_uninstalled()
        self.assert_config_no_package()

    def test_uninstall_not_installed(self):
        self.assert_uninstalled()
        self.assertRaisesRegexp(OSError, "Package does not exist",
                                self.uninstall)

    def uninstall(self):
        return self.run_prestoadmin(' yarn_slider uninstall')
