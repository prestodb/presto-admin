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

'''
Tests for the base_config module.
'''

from prestoadmin.yarn_slider.config import SliderConfig
from prestoadmin.yarn_slider.slider_application_configs import AppConfigJson, \
    ResourcesJson
from prestoadmin.standalone.config import StandaloneConfig
from prestoadmin.util.base_config import requires_config
from prestoadmin.util.exception import ConfigFileNotFoundError, \
    ConfigurationError

from mock import patch

from tests.base_test_case import BaseTestCase


class TestBaseConfig(BaseTestCase):
    @patch('tests.unit.util.test_base_config.SliderConfig.'
           'get_conf_interactive')
    @patch('tests.unit.util.test_base_config.SliderConfig.read_conf')
    @patch('tests.unit.util.test_base_config.SliderConfig.set_env_from_conf')
    def test_get_config_already_loaded(
            self, set_env_mock, file_conf_mock, interactive_conf_mock):
        config = SliderConfig()
        config.set_config_loaded()
        config.get_config()
        self.assertFalse(file_conf_mock.called)
        self.assertFalse(interactive_conf_mock.called)
        self.assertFalse(set_env_mock.called)

    @patch('tests.unit.util.test_base_config.StandaloneConfig.'
           'get_conf_interactive')
    @patch('tests.unit.util.test_base_config.StandaloneConfig.read_conf')
    @patch('tests.unit.util.test_base_config.StandaloneConfig.'
           'set_env_from_conf')
    def test_get_config_load_file(
            self, set_env_mock, file_conf_mock, interactive_conf_mock):
        config = StandaloneConfig()
        config.get_config()
        self.assertTrue(file_conf_mock.called)
        self.assertFalse(interactive_conf_mock.called)
        self.assertTrue(set_env_mock.called)
        self.assertTrue(config.is_config_loaded())

    @patch('tests.unit.util.test_base_config.StandaloneConfig.'
           'get_conf_interactive')
    @patch('tests.unit.util.test_base_config.StandaloneConfig.read_conf')
    @patch('tests.unit.util.test_base_config.StandaloneConfig.write_conf')
    @patch('tests.unit.util.test_base_config.StandaloneConfig.'
           'set_env_from_conf')
    def test_get_config_load_interactive(
            self, set_env_mock, store_conf_mock, file_conf_mock,
            interactive_conf_mock):
        file_conf_mock.side_effect = ConfigFileNotFoundError(
            message='oops', config_path='/asdf')
        config = StandaloneConfig()
        config.get_config()
        self.assertTrue(file_conf_mock.called)
        self.assertTrue(interactive_conf_mock.called)
        self.assertTrue(set_env_mock.called)
        self.assertTrue(store_conf_mock.called)
        self.assertTrue(config.is_config_loaded())

    @patch('tests.unit.util.test_base_config.SliderConfig.is_config_loaded')
    def test_decorator_has_config(self, mock_is_config_loaded):
        mock_is_config_loaded.return_value = True

        @requires_config(SliderConfig)
        def func():
            return 'runs'

        self.assertEquals(func(), 'runs')
        self.assertTrue(mock_is_config_loaded.called)

    @patch('tests.unit.util.test_base_config.StandaloneConfig.'
           'is_config_loaded')
    def test_decorator_no_config(self, mock_is_config_loaded):
        mock_is_config_loaded.return_value = False

        @requires_config(StandaloneConfig)
        def func():
            return 'runs'

        self.assertRaises(ConfigurationError, func)

    @patch('tests.unit.util.test_base_config.SliderConfig.is_config_loaded')
    @patch('tests.unit.util.test_base_config.AppConfigJson.is_config_loaded')
    @patch('tests.unit.util.test_base_config.ResourcesJson.is_config_loaded')
    def test_decorator_multi_has_config(
            self, mock_resources_icl, mock_app_config_icl,
            mock_slider_icl):
        mock_slider_icl.return_value = True
        mock_app_config_icl.return_value = True
        mock_resources_icl.return_value = True

        @requires_config(SliderConfig, AppConfigJson, ResourcesJson)
        def func():
            return 'runs'

        self.assertEquals(func(), 'runs')
        self.assertTrue(mock_app_config_icl.called)
        self.assertTrue(mock_resources_icl.called)
        self.assertTrue(mock_slider_icl.called)

    @patch('tests.unit.util.test_base_config.SliderConfig.is_config_loaded')
    @patch('tests.unit.util.test_base_config.AppConfigJson.is_config_loaded')
    @patch('tests.unit.util.test_base_config.ResourcesJson.is_config_loaded')
    def test_decorator_multi_missing_config(
            self, mock_resources_icl, mock_app_config_icl,
            mock_slider_icl):
        mock_slider_icl.return_value = True
        mock_app_config_icl.return_value = False
        mock_resources_icl.return_value = True

        @requires_config(SliderConfig, AppConfigJson, ResourcesJson)
        def func():
            return 'runs'

        self.assertRaises(ConfigurationError, func)
        # Don't care who was or wasn't called. That's an implementation detail.

    @patch('tests.unit.util.test_base_config.SliderConfig.read_conf')
    @patch('tests.unit.util.test_base_config.StandaloneConfig.'
           '_get_conf_from_file')
    def test_multi_set_hosts(self, mock_standalone_conf, mock_slider_conf):
        mock_standalone_conf.return_value = \
            {'username': 'user',
             'port': 1234,
             'coordinator': 'master',
             'workers': ['slave1', 'slave2']}

        mock_slider_conf.return_value = \
            {'slider_directory': '/opt/slider',
             'admin': 'root',
             'HADOOP_CONF': '/etc/hadoop/conf',
             'ssh_port': 22,
             'slider_user': 'yarn',
             'slider_master': 'localhost',
             'JAVA_HOME': '/usr/lib/jvm/java'}

        @requires_config(StandaloneConfig, SliderConfig)
        def func():
            return 'runs'

        callbacks = func.pa_config_callbacks

        self.assertEqual(len(callbacks), 2)
        callbacks[0]()
        self.assertRaises(ConfigurationError, callbacks[1])
