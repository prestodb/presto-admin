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
from prestoadmin.util.service_util import lookup_port
from tests.base_test_case import BaseTestCase


class TestServiceUtil(BaseTestCase):
    @patch('prestoadmin.util.service_util.run')
    def test_lookup_port_failure(self, run_mock):
        run_mock.return_value = Exception('File not found')

        self.assertRaisesRegexp(
            ConfigurationError,
            'Configuration file /etc/presto/config.properties does not exist '
            'on host any_host',
            lookup_port, 'any_host'
        )

    @patch('prestoadmin.util.service_util.run')
    def test_lookup_port_not_integer_failure(self, run_mock):
        run_mock.return_value = _AttributeString('http-server.http.port=hello')
        run_mock.return_value.failed = False
        self.assertRaisesRegexp(
            ConfigurationError,
            'Unable to coerce http-server.http.port \'hello\' to an int. '
            'Failed to connect to any_host.',
            lookup_port, 'any_host'
        )

    @patch('prestoadmin.util.service_util.run')
    def test_lookup_port_not_in_file(self, run_mock):
        run_mock.return_value = _AttributeString('')
        run_mock.return_value.failed = False
        port = lookup_port('any_host')
        self.assertEqual(port, 8080)

    @patch('prestoadmin.util.service_util.run')
    def test_lookup_port_out_of_range(self, run_mock):
        run_mock.return_value = _AttributeString('http-server.http.port=99999')
        run_mock.return_value.failed = False
        self.assertRaisesRegexp(
            ConfigurationError,
            'Invalid port number 99999: port must be between 1 and 65535 '
            'for property http-server.http.port on host any_host.',
            lookup_port, 'any_host'
        )
