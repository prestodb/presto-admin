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
        self.assertRaisesRegexp(config.ConfigurationError,
                                "Invalid property: invalid property",
                                topology.validate, conf)

    def test_valid_conf(self):
        conf = config.get_conf_from_file(os.path.dirname(__file__) +
                                         "/files/valid_conf.json")
        self.assertEqual(topology.validate(conf), conf)

    def test_valid_ipv4(self):
        ipv4 = "10.14.1.10"
        self.assertEqual(topology.validate_host(ipv4), ipv4)

    def test_valid_full_ipv6(self):
        ipv6 = "FE80:0000:0000:0000:0202:B3FF:FE1E:8329"
        self.assertEqual(topology.validate_host(ipv6), ipv6)

    def test_valid_collapsed_ipv6(self):
        ipv6 = "FE80::0202:B3FF:FE1E:8329"
        self.assertEqual(topology.validate_host(ipv6), ipv6)

    def test_empty_host(self):
        self.assertRaisesRegexp(config.ConfigurationError,
                                "'' is not a valid ip address or host name",
                                topology.validate_coordinator, (""))

    def test_valid_hostname(self):
        host = "master"
        self.assertEqual(topology.validate_host(host), host)

    def test_invalid_host(self):
        self.assertRaisesRegexp(config.ConfigurationError,
                                "'.1234' is not a valid ip address "
                                "or host name",
                                topology.validate_host, (".1234"))

    def test_invalid_host_type(self):
        self.assertRaisesRegexp(config.ConfigurationError,
                                "Host must be of type string.  "
                                "Found <type 'list'>",
                                topology.validate_host, (["my", "list"]))

    def test_valid_port(self):
        port = "1234"
        self.assertEqual(topology.validate_port(port), port)

    def test_invalid_port(self):
        self.assertRaisesRegexp(config.ConfigurationError,
                                "Invalid port number 99999999: port must be "
                                "between 1 and 65535",
                                topology.validate_port, ("99999999"))

    def test_invalid_port_type(self):
        self.assertRaises(config.ConfigurationError,
                          topology.validate_port, (["123"]))

    def test_valid_workers(self):
        workers = ["172.16.1.10", "myslave", "FE80::0202:B3FF:FE1E:8329"]
        self.assertEqual(topology.validate_workers(workers), workers)

    def test_no_workers(self):
        self.assertRaisesRegexp(config.ConfigurationError,
                                "Must specify at least one worker",
                                topology.validate_workers, ([]))

    def test_invalid_workers_type(self):
        self.assertRaisesRegexp(config.ConfigurationError,
                                "Workers must be of type list.  "
                                "Found <type 'str'>",
                                topology.validate_workers, ("not a list"))

    def test_invalid_coordinator_type(self):
        self.assertRaisesRegexp(config.ConfigurationError,
                                "Host must be of type string.  "
                                "Found <type 'list'>",
                                topology.validate_coordinator,
                                (["my", "list"]))

    def test_validate_workers_for_prompt(self):
        workers_input = "172.16.1.10 myslave FE80::0202:B3FF:FE1E:8329"
        workers_list = ["172.16.1.10", "myslave", "FE80::0202:B3FF:FE1E:8329"]
        self.assertEqual(topology.validate_workers_for_prompt(workers_input),
                         workers_list)

if __name__ == "__main__":
    unittest.main()
