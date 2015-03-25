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
Tests the presto topology config
"""

import os
from prestoadmin import topology
from prestoadmin import configuration as config
import utils
import unittest


class TestTopologyConfig(utils.BaseTestCase):

    def test_fill_conf(self):
        topology._get_conf_from_file = lambda: {"username": "john",
                                                "port": "100"}
        conf = topology.get_conf()
        self.assertEqual(conf, {"username": "john", "port": "100",
                                "coordinator": "localhost",
                                "workers": ["localhost"]})

    def test_invalid_property(self):
        conf = config.get_conf_from_file(os.path.dirname(__file__) +
                                         "/files/invalid_conf.json")
        self.assertRaisesRegexp(topology.ConfigurationError,
                                "Invalid property: invalid property",
                                topology.validate, conf)

    def test_valid_conf(self):
        conf = config.get_conf_from_file(os.path.dirname(__file__) +
                                         "/files/valid_conf.json")
        topology.validate(conf)

    def test_valid_ipv4(self):
        topology.validate_host("10.14.1.10")

    def test_valid_full_ipv6(self):
        topology.validate_host("FE80:0000:0000:0000:0202:B3FF:FE1E:8329")

    def test_valid_collapsed_ipv6(self):
        topology.validate_host("FE80::0202:B3FF:FE1E:8329")

    def test_empty_host(self):
        self.assertRaisesRegexp(topology.ConfigurationError,
                                "'' is not a valid ip address or host name",
                                topology.validate_coordinator, (""))

    def test_valid_hostname(self):
        topology.validate_host("master")

    def test_invalid_host(self):
        self.assertRaisesRegexp(topology.ConfigurationError,
                                "'.1234' is not a valid ip address "
                                "or host name",
                                topology.validate_host, (".1234"))

    def test_invalid_host_type(self):
        self.assertRaisesRegexp(topology.ConfigurationError,
                                "Host must be of type string.  "
                                "Found <type 'list'>",
                                topology.validate_host, (["my", "list"]))

    def test_valid_port(self):
        topology.validate_port("1234")

    def test_invalid_port(self):
        self.assertRaisesRegexp(topology.ConfigurationError,
                                "Invalid port number 99999999: port must be "
                                "between 1 and 65535",
                                topology.validate_port, ("99999999"))

    def test_invalid_port_type(self):
        self.assertRaises(topology.ConfigurationError,
                          topology.validate_port, (["123"]))

    def test_valid_workers(self):
        topology.validate_workers(["172.16.1.10", "myslave",
                                   "FE80::0202:B3FF:FE1E:8329"])

    def test_no_workers(self):
        self.assertRaisesRegexp(topology.ConfigurationError,
                                "Must specify at least one worker",
                                topology.validate_workers, ([]))

    def test_invalid_workers_type(self):
        self.assertRaisesRegexp(topology.ConfigurationError,
                                "Workers must be of type list.  "
                                "Found <type 'str'>",
                                topology.validate_workers, ("not a list"))

    def test_invalid_coordinator_type(self):
        self.assertRaisesRegexp(topology.ConfigurationError,
                                "Host must be of type string.  "
                                "Found <type 'list'>",
                                topology.validate_coordinator,
                                (["my", "list"]))

    def test_valid_read_username(self):
        self.assertEqual(topology.read_in_username(lambda: "user"), "user")

    def test_default_read_username(self):
        self.assertEqual(topology.read_in_username(lambda: ""), "root")

    def test_valid_read_port(self):
        self.assertEqual(topology.read_in_port(lambda: "123"), "123")

    def test_default_read_port(self):
        self.assertEqual(topology.read_in_port(lambda: ""), "22")

    def test_valid_read_coordinator(self):
        self.assertEqual(topology.read_in_coordinator(lambda: "master"),
                         "master")

    def test_default_read_coordinator(self):
        self.assertEqual(topology.read_in_coordinator(lambda: ""),
                         "localhost")

    def test_valid_read_workers(self):
        self.assertEqual(topology.read_in_workers(lambda: ["slave1",
                                                           "slave2"]),
                         ["slave1", "slave2"])

    def test_default_read_workers(self):
        self.assertEqual(topology.read_in_workers(lambda: ""),
                         ["localhost"])

if __name__ == "__main__":
    unittest.main()
