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
from fabric.operations import _AttributeString
from mock import patch
from prestoadmin.util.exception import ConfigurationError
from prestoadmin.util.remote_config_util import lookup_port,\
    lookup_string_config, NODE_CONFIG_FILE
from tests.base_test_case import BaseTestCase


class TestRemoteConfigUtil(BaseTestCase):
    @patch('prestoadmin.util.remote_config_util.sudo')
    def test_lookup_port_failure(self, sudo_mock):
        sudo_mock.return_value = Exception('File not found')

        self.assertRaisesRegexp(
            ConfigurationError,
            'Could not access config file /etc/presto/config.properties on host any_host',
            lookup_port, 'any_host'
        )

    @patch('prestoadmin.util.remote_config_util.sudo')
    def test_lookup_port_not_integer_failure(self, sudo_mock):
        sudo_mock.return_value = _AttributeString(
            'http-server.http.port=hello')
        sudo_mock.return_value.failed = False
        sudo_mock.return_value.return_code = 0

        self.assertRaisesRegexp(
            ConfigurationError,
            'Invalid port number hello: port must be a number between 1 and'
            ' 65535 for property http-server.http.port on host any_host.',
            lookup_port, 'any_host'
        )

    @patch('prestoadmin.util.remote_config_util.sudo')
    def test_lookup_port_not_in_file(self, sudo_mock):
        sudo_mock.return_value = _AttributeString('')
        sudo_mock.return_value.failed = False
        sudo_mock.return_value.return_code = 1
        port = lookup_port('any_host')
        self.assertEqual(port, 8080)

    @patch('prestoadmin.util.remote_config_util.sudo')
    def test_lookup_port_out_of_range(self, sudo_mock):
        sudo_mock.return_value = _AttributeString(
            'http-server.http.port=99999')
        sudo_mock.return_value.failed = False
        sudo_mock.return_value.return_code = 0
        self.assertRaisesRegexp(
            ConfigurationError,
            'Invalid port number 99999: port must be a number between 1 and '
            '65535 for property http-server.http.port on host any_host.',
            lookup_port, 'any_host'
        )

    @patch('prestoadmin.util.remote_config_util.sudo')
    def test_lookup_string_config(self, sudo_mock):
        sudo_mock.return_value = _AttributeString(
            'config.to.lookup=/path/hello')
        sudo_mock.return_value.failed = False
        sudo_mock.return_value.return_code = 0
        config_value = lookup_string_config('config.to.lookup',
                                            NODE_CONFIG_FILE, 'any_host')
        self.assertEqual(config_value, '/path/hello')

    @patch('prestoadmin.util.remote_config_util.sudo')
    def test_lookup_string_config_not_in_file(self, sudo_mock):
        sudo_mock.return_value = _AttributeString('')
        sudo_mock.return_value.failed = False
        sudo_mock.return_value.return_code = 1
        config_value = lookup_string_config('config.to.lookup',
                                            NODE_CONFIG_FILE, 'any_host')
        self.assertEqual(config_value, '')

    @patch('prestoadmin.util.remote_config_util.sudo')
    def test_lookup_string_config_file_not_found(self, sudo_mock):
        sudo_mock.return_value = _AttributeString(
            'grep: /etc/presto/node.properties does not exist')
        sudo_mock.return_value.return_code = 2

        self.assertRaisesRegexp(
            ConfigurationError,
            'Could not access config file /etc/presto/node.properties on host any_host',
            lookup_string_config, 'config.to.lookup', NODE_CONFIG_FILE,
            'any_host'
        )
