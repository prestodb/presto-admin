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
Test the various validators
"""
from prestoadmin.util import validators
from prestoadmin.util.exception import ConfigurationError
from tests.base_test_case import BaseTestCase


class TestValidators(BaseTestCase):
    def test_valid_ipv4(self):
        ipv4 = "10.14.1.10"
        self.assertEqual(validators.validate_host(ipv4), ipv4)

    def test_valid_full_ipv6(self):
        ipv6 = "FE80:0000:0000:0000:0202:B3FF:FE1E:8329"
        self.assertEqual(validators.validate_host(ipv6), ipv6)

    def test_valid_collapsed_ipv6(self):
        ipv6 = "FE80::0202:B3FF:FE1E:8329"
        self.assertEqual(validators.validate_host(ipv6), ipv6)

    def test_valid_hostname(self):
        host = "master"
        self.assertEqual(validators.validate_host(host), host)

    def test_invalid_host(self):
        self.assertRaisesRegexp(ConfigurationError,
                                "'.1234' is not a valid ip address "
                                "or host name",
                                validators.validate_host,
                                (".1234"))

    def test_invalid_host_type(self):
        self.assertRaisesRegexp(ConfigurationError,
                                "Host must be of type string.  "
                                "Found <type 'list'>",
                                validators.validate_host,
                                (["my", "list"]))

    def test_valid_port(self):
        port = 1234
        self.assertEqual(validators.validate_port(port), port)

    def test_invalid_port(self):
        self.assertRaisesRegexp(ConfigurationError,
                                "Invalid port number 99999999: port must be "
                                "a number between 1 and 65535",
                                validators.validate_port,
                                ("99999999"))

    def test_invalid_port_type(self):
        self.assertRaises(ConfigurationError,
                          validators.validate_port, (["123"]))
