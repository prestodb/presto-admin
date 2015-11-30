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

from prestoadmin import config
from prestoadmin.util.exception import ConfigurationError, \
    ConfigFileNotFoundError
from tests.base_test_case import BaseTestCase


DIR = os.path.abspath(os.path.dirname(__file__))


class TestConfiguration(BaseTestCase):
    def test_file_does_not_exist_json(self):
        self.assertRaisesRegexp(ConfigFileNotFoundError,
                                'Missing configuration file ',
                                config.get_conf_from_json_file,
                                'does/not/exist/conf.json')

    def test_file_is_empty_json(self):
        emptyconf = {}
        conf = config.get_conf_from_json_file(DIR + '/resources/empty.txt')
        self.assertEqual(conf, emptyconf)

    def test_file_is_empty_properties(self):
        emptyconf = {}
        conf = config.get_conf_from_properties_file(
            DIR + '/resources/empty.txt')
        self.assertEqual(conf, emptyconf)

    def test_file_is_empty_config(self):
        emptyconf = []
        conf = config.get_conf_from_config_file(DIR + '/resources/empty.txt')
        self.assertEqual(conf, emptyconf)

    def test_invalid_json(self):
        self.assertRaisesRegexp(ConfigurationError,
                                'Expecting , delimiter: line 3 column 3 '
                                '\(char 19\)',
                                config.get_conf_from_json_file,
                                DIR + '/resources/invalid_json_conf.json')

    def test_get_config(self):
        config_file = os.path.join(DIR, 'resources', 'valid.config')
        conf = config.get_conf_from_config_file(config_file)
        self.assertEqual(conf, ['prop1', 'prop2', 'prop3'])

    def test_get_properties(self):
        config_file = os.path.join(DIR, 'resources', 'valid.properties')
        conf = config.get_conf_from_properties_file(config_file)
        self.assertEqual(conf, {'a': '1', 'b': '2', 'c': '3',
                                'd\\=': '4', 'e\\:': '5', 'f': '==6',
                                'g': '= 7', 'h': ':8', 'i': '9'})

    @patch('__builtin__.open')
    def test_get_properties_ignores_whitespace(self, open_mock):
        file_manager = open_mock.return_value.__enter__.return_value
        file_manager.read.return_value = ' key1 =value1 \n   \n key2= value2'
        conf = config.get_conf_from_properties_file('/dummy/path')
        self.assertEqual(conf, {'key1': 'value1', 'key2': 'value2'})

    def test_get_properties_invalid(self):
        config_file = os.path.join(DIR, 'resources', 'invalid.properties')
        self.assertRaisesRegexp(ConfigurationError,
                                'abcd is not in the expected format: '
                                '<property>=<value>, <property>:<value> or '
                                '<property> <value>',
                                config.get_conf_from_properties_file,
                                config_file)

    def test_fill_defaults_no_missing(self):
        orig = {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'}
        defaults = {'key1': 'default1', 'key2': 'default2'}
        filled = orig.copy()
        config.fill_defaults(filled, defaults)
        self.assertEqual(filled, orig)

    def test_fill_defaults(self):
        orig = {'key1': 'val1', 'key3': 'val3'}
        defaults = {'key1': 'default1', 'key2': 'default2'}
        filled = orig.copy()
        config.fill_defaults(filled, defaults)
        self.assertEqual(filled,
                         {'key1': 'val1', 'key2': 'default2', 'key3': 'val3'})
