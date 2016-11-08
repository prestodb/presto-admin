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
import os

from mock import patch
from prestoadmin.util import local_config_util
from prestoadmin.util.constants import DEFAULT_LOCAL_CONF_DIR
from tests.base_test_case import BaseTestCase


class TestLocalConfigUtil(BaseTestCase):
    @patch('prestoadmin.util.local_config_util.os.environ.get')
    def test_get_default_config_dir(self, get_mock):
        get_mock.return_value = None
        self.assertEqual(local_config_util.get_config_directory(), DEFAULT_LOCAL_CONF_DIR)

    @patch('prestoadmin.util.local_config_util.os.environ.get')
    def test_get_configured_config_dir(self, get_mock):
        non_default_directory = '/not/the/default'
        get_mock.return_value = non_default_directory
        self.assertEqual(local_config_util.get_config_directory(), non_default_directory)

    @patch('prestoadmin.util.local_config_util.os.environ.get')
    def test_get_default_log_dir(self, get_mock):
        get_mock.return_value = None
        self.assertEqual(local_config_util.get_log_directory(), os.path.join(DEFAULT_LOCAL_CONF_DIR, 'log'))

    @patch('prestoadmin.util.local_config_util.os.environ.get')
    def test_get_configured_log_dir(self, get_mock):
        non_default_directory = '/not/the/default'
        get_mock.return_value = non_default_directory
        self.assertEqual(local_config_util.get_log_directory(), non_default_directory)
