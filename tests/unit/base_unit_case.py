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

from mock import patch

from prestoadmin.standalone.config import StandaloneConfig
from prestoadmin.util.presto_config import PrestoConfig

from tests.base_test_case import BaseTestCase

PRESTO_CONFIG = PrestoConfig({
    'http-server.http.enabled': 'true',
    'http-server.https.enabled': 'false',
    'http-server.http.port': '8080',
    'http-server.https.port': '7878',
    'http-server.https.keystore.path': '/UPDATE/THIS/PATH',
    'http-server.https.keystore.key': 'UPDATE PASSWORD'},
    "TEST_PATH",
    "TEST_HOST")


class BaseUnitCase(BaseTestCase):

    '''
    Tasks generally require that the configuration they need to run has been
    loaded. This takes care of loading the config without going to the
    filesystem. For cases where you want to test the configuration load process
    itself, you should pass load_config=False to setUp.
    '''
    def setUp(self, capture_output=False, load_config=True):
        super(BaseUnitCase, self).setUp(capture_output=capture_output)
        if load_config:
            @patch('tests.unit.base_unit_case.StandaloneConfig.'
                   '_get_conf_from_file')
            def loader(mock_get_conf):
                mock_get_conf.return_value = {'username': 'user',
                                              'port': 1234,
                                              'coordinator': 'master',
                                              'workers': ['slave1', 'slave2']}

                config = StandaloneConfig()
                config.get_config()
            loader()
