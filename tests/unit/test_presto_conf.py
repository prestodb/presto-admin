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
Test the presto_conf module
"""

import re
from mock import patch
from prestoadmin.presto_conf import get_presto_conf, validate_presto_conf
from prestoadmin.util.exception import ConfigurationError
from tests.base_test_case import BaseTestCase


class TestPrestoConf(BaseTestCase):

    @patch('prestoadmin.presto_conf.os.path.isdir')
    @patch('prestoadmin.presto_conf.os.listdir')
    @patch('prestoadmin.presto_conf.get_conf_from_properties_file')
    @patch('prestoadmin.presto_conf.get_conf_from_config_file')
    def test_get_presto_conf(self, config_mock, props_mock, listdir_mock,
                             isdir_mock):
        isdir_mock.return_value = True
        listdir_mock.return_value = ['log.properties', 'jvm.config', ]
        config_mock.return_value = ['prop1', 'prop2']
        props_mock.return_value = {'a': '1', 'b': '2'}
        conf = get_presto_conf('dummy/dir')
        config_mock.assert_called_with('dummy/dir/jvm.config')
        props_mock.assert_called_with('dummy/dir/log.properties')
        self.assertEqual(conf, {'log.properties': {'a': '1', 'b': '2'},
                                'jvm.config': ['prop1', 'prop2']})

    @patch('prestoadmin.presto_conf.os.listdir')
    @patch('prestoadmin.presto_conf.os.path.isdir')
    @patch('prestoadmin.presto_conf.get_conf_from_properties_file')
    def test_get_non_presto_file(self, get_mock, isdir_mock, listdir_mock):
        isdir_mock.return_value = True
        listdir_mock.return_value = ['test.properties']
        self.assertFalse(get_mock.called)

    def test_conf_not_exists_is_empty(self):
        self.assertEqual(get_presto_conf('/does/not/exist'), {})

    def test_valid_conf(self):
        conf = {'node.properties': {}, 'jvm.config': [],
                'config.properties': {'discovery.uri': 'http://uri'}}
        self.assertEqual(validate_presto_conf(conf), conf)

    def test_invalid_conf(self):
        conf = {'jvm.config': [],
                'config.properties': {}}
        self.assertRaisesRegexp(ConfigurationError,
                                'Missing configuration for required file:',
                                validate_presto_conf,
                                conf)

    def test_invalid_node_type(self):
        conf = {'node.properties': '', 'jvm.config': [],
                'config.properties': {}}
        self.assertRaisesRegexp(ConfigurationError,
                                'node.properties must be an object with key-'
                                'value property pairs',
                                validate_presto_conf,
                                conf)

    def test_invalid_jvm_type(self):
        conf = {'node.properties': {}, 'jvm.config': {},
                'config.properties': {}}
        self.assertRaisesRegexp(ConfigurationError,
                                re.escape('jvm.config must contain a json '
                                          'array of jvm arguments ([arg1, '
                                          'arg2, arg3])'),
                                validate_presto_conf,
                                conf)

    def test_invalid_config_type(self):
        conf = {'node.properties': {}, 'jvm.config': [],
                'config.properties': []}
        self.assertRaisesRegexp(ConfigurationError,
                                'config.properties must be an object with key-'
                                'value property pairs',
                                validate_presto_conf,
                                conf)
