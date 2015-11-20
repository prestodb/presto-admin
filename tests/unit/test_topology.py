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
import unittest

from mock import patch
from fabric.state import env

from prestoadmin import topology
from prestoadmin.util.exception import ConfigurationError,\
    ConfigFileNotFoundError
from tests.base_test_case import BaseTestCase


class TestTopologyConfig(BaseTestCase):
    def setUp(self):
        super(TestTopologyConfig, self).setUp(capture_output=True)

    @patch('prestoadmin.topology._get_conf_from_file')
    def test_fill_conf(self, get_conf_from_file_mock):
        get_conf_from_file_mock.return_value = \
            ({"username": "john", "port": "100"}, 'config_path')
        conf, unused = topology.get_conf()
        self.assertEqual(conf, {"username": "john", "port": 100,
                                "coordinator": "localhost",
                                "workers": ["localhost"]})

    def test_invalid_property(self):
        conf = {"username": "me",
                "port": "1234",
                "coordinator": "coordinator",
                "workers": ["node1", "node2"],
                "invalid property": "fake"}
        self.assertRaisesRegexp(ConfigurationError,
                                "Invalid property: invalid property",
                                topology.validate, conf)

    def test_basic_valid_conf(self):
        conf = {"username": "user",
                "port": 1234,
                "coordinator": "my.coordinator",
                "workers": ["my.worker1", "my.worker2", "my.worker3"]}
        self.assertEqual(topology.validate(conf.copy()), conf)

    def test_valid_string_port_to_int(self):
        conf = {'username': 'john',
                'port': '123',
                'coordinator': 'master',
                'workers': ['worker1', 'worker2']}
        validated_conf = topology.validate(conf.copy())
        self.assertEqual(validated_conf['port'], 123)

    def test_empty_host(self):
        self.assertRaisesRegexp(ConfigurationError,
                                "'' is not a valid ip address or host name",
                                topology.validate_coordinator, (""))

    def test_valid_workers(self):
        workers = ["172.16.1.10", "myslave", "FE80::0202:B3FF:FE1E:8329"]
        self.assertEqual(topology.validate_workers(workers), workers)

    def test_no_workers(self):
        self.assertRaisesRegexp(ConfigurationError,
                                "Must specify at least one worker",
                                topology.validate_workers, ([]))

    def test_invalid_workers_type(self):
        self.assertRaisesRegexp(ConfigurationError,
                                "Workers must be of type list.  "
                                "Found <type 'str'>",
                                topology.validate_workers, ("not a list"))

    def test_invalid_coordinator_type(self):
        self.assertRaisesRegexp(ConfigurationError,
                                "Host must be of type string.  "
                                "Found <type 'list'>",
                                topology.validate_coordinator,
                                (["my", "list"]))

    def test_validate_workers_for_prompt(self):
        workers_input = "172.16.1.10 myslave FE80::0202:B3FF:FE1E:8329"
        workers_list = ["172.16.1.10", "myslave", "FE80::0202:B3FF:FE1E:8329"]
        self.assertEqual(topology.validate_workers_for_prompt(workers_input),
                         workers_list)

    def test_show(self):
        env.roledefs = {'coordinator': ['hello'], 'worker': ['a', 'b'],
                        'all': ['a', 'b', 'hello']}
        env.user = 'user'
        env.port = '22'

        self.remove_runs_once_flag(topology.show)
        topology.show()
        self.assertEqual("", self.test_stderr.getvalue())
        self.assertEqual("{'coordinator': 'hello',\n 'port': '22',\n "
                         "'username': 'user',\n 'workers': ['a',\n"
                         "             'b']}\n",
                         self.test_stdout.getvalue())

    @patch('prestoadmin.main.topology.get_conf')
    def test_hosts_set(self, conf_mock):
        conf_mock.return_value = ({"username": "root", "port": "22",
                                   "coordinator": "hello",
                                   "workers": ["a", "b"]}, 'config_path')
        topology.set_env_from_conf()
        self.assertEqual(topology.env.hosts, ['hello', 'a', 'b'])

    def test_decorator_no_topology(self):
        env.topology_config_not_found = ConfigFileNotFoundError(
            message='I got 99 problems but type safety ain\'t one',
            config_path='config_path')

        @topology.requires_topology
        def func():
            pass
        self.assertRaises(ConfigFileNotFoundError, func)

    def test_decorator_has_topology(self):
        env.topology_config_not_found = False

        @topology.requires_topology
        def func():
            return "runs"
        self.assertEqual(func(), "runs")

    @patch('prestoadmin.topology.set_conf_interactive')
    @patch('prestoadmin.topology.set_env_from_conf')
    def test_set_has_topology(self, set_conf_mock, set_env_mock):
        env.topology_config_not_found = False
        topology.set_topology_if_missing()
        self.assertFalse(set_conf_mock.called)
        self.assertFalse(set_env_mock.called)

    @patch('prestoadmin.topology.set_conf_interactive')
    @patch('prestoadmin.topology.set_env_from_conf')
    def test_set_missing_no_topology(self, set_conf_mock, set_env_mock):
        env.topology_config_not_found = True

        topology.set_topology_if_missing()
        self.assertTrue(set_conf_mock.called)
        self.assertTrue(set_env_mock.called)
        self.assertFalse(env.topology_config_not_found)

    @patch('prestoadmin.topology.set_conf_interactive')
    @patch('prestoadmin.topology.get_conf')
    def test_interactive_install(self,  get_conf_mock,
                                 mock_set_interactive):
        env.topology_config_not_found = ConfigurationError()
        get_conf_mock.return_value = ({'username': 'bob', 'port': '225',
                                       'coordinator': 'master',
                                       'workers': ['slave1', 'slave2']},
                                      'config_path')
        topology.set_topology_if_missing()

        self.assertEqual(env.user, 'bob'),
        self.assertEqual(env.port, '225')
        self.assertEqual(env.hosts, ['master', 'slave1', 'slave2'])
        self.assertEqual(env.roledefs['all'],
                         ['master', 'slave1', 'slave2'])
        self.assertEqual(env.roledefs['coordinator'], ['master'])
        self.assertEqual(env.roledefs['worker'], ['slave1', 'slave2'])
        self.assertFalse(env.topology_config_not_found)

if __name__ == "__main__":
    unittest.main()
