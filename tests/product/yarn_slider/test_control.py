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

import json
import os

from prestoadmin.yarn_slider.config import SLIDER_CONFIG_PATH, \
    SLIDER_CONFIG_DIR, DIR

from tests.hdp_bare_image_provider import HdpBareImageProvider

from tests.product.constants import LOCAL_RESOURCES_DIR
from tests.product.yarn_slider.yarn_slider_test_case import YarnSliderTestCase

SLIDER_CLIENT_XML = 'slider-client.xml'


class TestControl(YarnSliderTestCase):
    def setUp(self):
        super(TestControl, self).setUp()
        self.setup_cluster(HdpBareImageProvider(), self.PRESTO_YARN_CLUSTER)
        self.await_hdfs()

    def get_slider_config(self):
        config = self.cluster.exec_cmd_on_host(
            self.cluster.get_master(), 'cat %s' % (SLIDER_CONFIG_PATH,))
        return json.loads(config)

    def upload_slider_config(self):
        config = self.get_slider_config()
        slider_dir = config[DIR]

        self.cluster.copy_to_host(
            os.path.join(LOCAL_RESOURCES_DIR, SLIDER_CLIENT_XML),
            self.cluster.get_master(),
            os.path.join(slider_dir, 'conf', SLIDER_CLIENT_XML))

    def test_start_stop(self):
        self.upload_slider_config()
        self.cluster.copy_to_host(
            os.path.join(LOCAL_RESOURCES_DIR, 'appConfig.json'),
            self.cluster.get_master(),
            os.path.join(SLIDER_CONFIG_DIR, 'appConfig.json'))
        self.cluster.copy_to_host(
            os.path.join(LOCAL_RESOURCES_DIR, 'resources.json'),
            self.cluster.get_master(),
            os.path.join(SLIDER_CONFIG_DIR, 'resources.json'))
        self.run_prestoadmin(' slider start')
