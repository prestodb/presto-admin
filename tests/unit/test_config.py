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
import re

from mock import patch

from prestoadmin import config
from prestoadmin.util.exception import ConfigurationError,\
    ConfigFileNotFoundError
from tests import utils


DIR = os.path.abspath(os.path.dirname(__file__))


class TestConfiguration(utils.BaseTestCase):

    def test_file_does_not_exist_json(self):
        self.assertRaisesRegexp(ConfigFileNotFoundError,
                                'Missing configuration file at',
                                config.get_conf_from_json_file,
                                'does/not/exist/conf.json')

    def test_file_is_empty_json(self):
        emptyconf = {}
        conf = config.get_conf_from_json_file(DIR + '/files/empty.txt')
        self.assertEqual(conf, emptyconf)

    def test_file_is_empty_properties(self):
        emptyconf = {}
        conf = config.get_conf_from_properties_file(DIR + '/files/empty.txt')
        self.assertEqual(conf, emptyconf)

    def test_file_is_empty_config(self):
        emptyconf = []
        conf = config.get_conf_from_config_file(DIR + '/files/empty.txt')
        self.assertEqual(conf, emptyconf)

    def test_invalid_json(self):
        self.assertRaisesRegexp(ConfigurationError,
                                'Expecting , delimiter: line 3 column 3 '
                                '\(char 19\)',
                                config.get_conf_from_json_file,
                                DIR + '/files/invalid_json_conf.json')

    def test_get_config(self):
        config_file = os.path.join(DIR, 'files', 'valid.config')
        conf = config.get_conf_from_config_file(config_file)
        self.assertEqual(conf, ['prop1', 'prop2', 'prop3'])

    def test_get_properties(self):
        config_file = os.path.join(DIR, 'files', 'valid.properties')
        conf = config.get_conf_from_properties_file(config_file)
        self.assertEqual(conf, {'a': '1', 'b': '2', 'c': '3'})

    @patch('__builtin__.open')
    def test_get_properties_ignores_whitespace(self, open_mock):
        file_manager = open_mock.return_value.__enter__.return_value
        file_manager.read.return_value = 'key1=value1 \n   \n key2=value2'
        conf = config.get_conf_from_properties_file('/dummy/path')
        self.assertEqual(conf, {'key1': 'value1', 'key2': 'value2'})

    def test_get_properties_invalid(self):
        config_file = os.path.join(DIR, 'files', 'invalid.properties')
        self.assertRaisesRegexp(ConfigurationError,
                                'abcd is not in the expected format: '
                                '<property>=<value>',
                                config.get_conf_from_properties_file,
                                config_file)

    @patch('prestoadmin.config.os.path.isdir')
    @patch('prestoadmin.config.os.listdir')
    @patch('prestoadmin.config.get_conf_from_properties_file')
    @patch('prestoadmin.config.get_conf_from_config_file')
    def test_get_presto_conf(self, config_mock, props_mock, listdir_mock,
                             isdir_mock):
        isdir_mock.return_value = True
        listdir_mock.return_value = ['log.properties', 'jvm.config', ]
        config_mock.return_value = ['prop1', 'prop2']
        props_mock.return_value = {'a': '1', 'b': '2'}
        conf = config.get_presto_conf('dummy/dir')
        config_mock.assert_called_with('dummy/dir/jvm.config')
        props_mock.assert_called_with('dummy/dir/log.properties')
        self.assertEqual(conf, {'log.properties': {'a': '1', 'b': '2'},
                                'jvm.config': ['prop1', 'prop2']})

    @patch('prestoadmin.config.os.listdir')
    @patch('prestoadmin.config.os.path.isdir')
    @patch('prestoadmin.config.get_conf_from_properties_file')
    def test_get_non_presto_file(self, get_mock, isdir_mock, listdir_mock):
        isdir_mock.return_value = True
        listdir_mock.return_value = ['test.properties']
        self.assertFalse(config.get_conf_from_properties_file.called)

    def test_conf_not_exists_is_empty(self):
        self.assertEqual(config.get_presto_conf('/does/not/exist'), {})

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

    def test_valid_conf(self):
        conf = {'node.properties': {}, 'jvm.config': [],
                'config.properties': {}}
        self.assertEqual(config.validate_presto_conf(conf), conf)

    def test_invalid_conf(self):
        conf = {'jvm.config': [],
                'config.properties': {}}
        self.assertRaisesRegexp(ConfigurationError,
                                'Missing configuration for required file:',
                                config.validate_presto_conf,
                                conf)

    def test_invalid_node_type(self):
        conf = {'node.properties': '', 'jvm.config': [],
                'config.properties': {}}
        self.assertRaisesRegexp(ConfigurationError,
                                'node.properties must be an object with key-'
                                'value property pairs',
                                config.validate_presto_conf,
                                conf)

    def test_invalid_jvm_type(self):
        conf = {'node.properties': {}, 'jvm.config': {},
                'config.properties': {}}
        self.assertRaisesRegexp(ConfigurationError,
                                re.escape('jvm.config must contain a json '
                                          'array of jvm arguments ([arg1, '
                                          'arg2, arg3])'),
                                config.validate_presto_conf,
                                conf)

    def test_invalid_config_type(self):
        conf = {'node.properties': {}, 'jvm.config': [],
                'config.properties': []}
        self.assertRaisesRegexp(ConfigurationError,
                                'config.properties must be an object with key-'
                                'value property pairs',
                                config.validate_presto_conf,
                                conf)
