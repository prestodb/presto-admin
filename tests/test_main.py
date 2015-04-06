#!/usr/bin/env python
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
#


"""
test_prestoadmin
----------------------------------

Tests for `prestoadmin` module.
"""

import os
import prestoadmin
import unittest
import utils

from prestoadmin import main
from prestoadmin.configuration import ConfigFileNotFoundError
from prestoadmin.configuration import ConfigurationError
from mock import patch


class TestMain(utils.BaseTestCase):

    def run_command_compare_to_file(self, command, exit_status, filename):
        """
            Compares stdout from the CLI to the given file
        """
        current_dir = os.path.abspath(os.path.dirname(__file__))
        input_file = open(current_dir + filename, 'r')
        text = "".join(input_file.readlines())
        input_file.close()
        self.run_command_compare_to_string(command, exit_status, text)

    def run_command_compare_to_string(self, command, exit_status, text):
        """
            Compares stdout from the CLI to the given string
        """
        try:
            main.parse_and_validate_commands(command)
        except SystemExit as e:
            self.assertEqual(e.code, exit_status)

        self.assertEqual(text, self.test_stdout.getvalue())

    def test_help_text_short(self):
        # See if the help text matches what we expect it to be (in
        # tests/help.txt)
        self.run_command_compare_to_file(["-h"], 0, "/files/help.txt")

    def test_help_text_long(self):
        self.run_command_compare_to_file(["--help"], 0, "/files/help.txt")

    def test_help_displayed_with_no_args(self):
        self.run_command_compare_to_file([], 0, "/files/help.txt")

    def test_list_commands(self):
        # Note: this will have to be updated whenever we add a new command
        self.run_command_compare_to_file(["-l"], 0, "/files/list.txt")

    def test_version(self):
        # Note: this will have to be updated whenever we have a new version.
        self.run_command_compare_to_string(["--version"], 0,
                                           "presto-admin %s\n" %
                                           prestoadmin.__version__)

    def test_argument_parsing_with_invalid_command(self):
        try:
            main.parse_and_validate_commands(["hello", "world"])
        except SystemExit as e:
            self.assertEqual(e.code, 2)

        self.assertEqual(self.test_stderr.getvalue(), "\nWarning: Command not "
                         "found:\n    hello world\n\n")
        self.assertTrue("Available commands:" in self.test_stdout.getvalue())

    def test_argument_parsing_with_short_command(self):
        try:
            main.parse_and_validate_commands(["topology"])
        except SystemExit as e:
            self.assertEqual(e.code, 2)

        self.assertEqual(self.test_stderr.getvalue(), "\nWarning: Command not "
                         "found:\n    topology\n\n")
        self.assertTrue("Available commands:" in self.test_stdout.getvalue())

    def test_argument_parsing_with_valid_command(self):
        commands = main.parse_and_validate_commands(["topology", "show"])
        self.assertEqual(commands[0][0], "topology.show")

    def test_argument_parsing_with_arguments(self):
        commands = main.parse_and_validate_commands(["topology", "show", "f"])
        self.assertEqual(commands[0][0], "topology.show")
        self.assertEqual(commands[0][1], ["f"])

    def test_arbitrary_remote_shell_disabled(self):
        try:
            main.parse_and_validate_commands(["--", "echo", "hello"])
        except SystemExit as e:
            self.assertEqual(e.code, 2)

        self.assertEqual(self.test_stderr.getvalue(), "\nWarning: Arbitrary "
                         "remote shell commands not supported.\n\n")
        self.assertTrue("Available commands:" in self.test_stdout.getvalue())

    @patch('prestoadmin.main.topology')
    def test_load_topology(self, topology_mock):
        topology_mock.get_coordinator.return_value = 'hello'
        topology_mock.get_workers.return_value = ['a', 'b']
        topology_mock.get_port.return_value = '1234'
        topology_mock.get_username.return_value = 'user'
        main.load_topology()
        self.assertEqual(main.state.env.roledefs,
                         {'coordinator': ['hello'], 'worker': ['a', 'b'],
                          'all': ['a', 'b', 'hello']})
        self.assertEqual(main.state.env.port, '1234')
        self.assertEqual(main.state.env.user, 'user')
        self.assertEqual(main.state.env.hosts, ['a', 'b', 'hello'])

    @patch('prestoadmin.main.topology')
    def test_load_topology_not_exists(self, topology_mock):
        e = ConfigFileNotFoundError()

        def func():
            raise e
        topology_mock.get_coordinator = func
        main.load_topology()
        self.assertEqual(main.state.env.roledefs,
                         {'coordinator': [], 'worker': [], 'all': []})
        self.assertEqual(main.state.env.port, '22')
        self.assertNotEqual(main.state.env.user, 'user')
        self.assertEqual(main.state.env.topology_config_not_found, e)

    @patch('prestoadmin.main.topology')
    def test_load_topology_failure(self, topology_mock):
        e = ConfigurationError()

        def func():
            raise e
        topology_mock.get_coordinator = func
        self.assertRaises(ConfigurationError, main.load_topology)

    @patch('prestoadmin.main.topology')
    def test_hosts_on_cli_overrides_topology(self, topology_mock):
        topology_mock.get_coordinator.return_value = 'hello'
        topology_mock.get_workers.return_value = ['a', 'b']
        try:
            main.main(['--hosts', 'hello,a', 'topology', 'show'])
        except SystemExit as e:
            self.assertEqual(e.code, 0)

        self.assertEqual(main.state.env.roledefs,
                         {'coordinator': ['hello'], 'worker': ['a', 'b'],
                          'all': ['a', 'b', 'hello']})
        self.assertEqual(main.state.env.hosts, ['hello', 'a'])
        self.assertEqual(main.api.env.hosts, ['hello', 'a'])

    def test_describe(self):
        self.run_command_compare_to_string(
            ['-d', 'topology', 'show'],
            0,
            "Displaying detailed information for task 'topology show':\n\n   "
            " Shows the current topology configuration for the cluster "
            "(including the\n    coordinators, workers, SSH port, and SSH "
            "username)\n    \n    Arguments: None\n\n"
        )

    def test_describe_with_args(self):
        self.run_command_compare_to_string(
            ['-d', 'topology', 'show', 'arg'],
            0,
            "Displaying detailed information for task 'topology show':\n\n   "
            " Shows the current topology configuration for the cluster "
            "(including the\n    coordinators, workers, SSH port, and SSH "
            "username)\n    \n    Arguments: None\n\n"
        )

    def test_shortlist(self):
        self.run_command_compare_to_file(["--shortlist"], 0,
                                         "/files/shortlist.txt")

    @patch('prestoadmin.main.getpass.getpass')
    def test_initial_password(self, pass_mock):
        try:
            main.parse_and_validate_commands(['-I', 'topology', 'show'])
        except SystemExit as e:
            self.assertEqual(0, e.code)
        pass_mock.assert_called_once_with('Initial value for env.password: ')

    @patch('prestoadmin.main.topology')
    def test_env_vars_persisted(self, topology_mock):
        topology_mock.get_coordinator.return_value = 'hello'
        topology_mock.get_workers.return_value = ['a', 'b']
        topology_mock.get_port.return_value = '1234'
        topology_mock.get_username.return_value = 'user'
        try:
            main.main(['topology', 'show'])
        except SystemExit as e:
            self.assertEqual(e.code, 0)
        self.assertEqual(['a', 'b', 'hello'], main.state.env.hosts)

    @patch('prestoadmin.topology._get_conf_from_file')
    def test_topology_defaults_override_fabric_defaults(self, get_conf_mock):
        get_conf_mock.return_value = {}
        try:
            main.main(['topology', 'show'])
        except SystemExit as e:
            self.assertEqual(e.code, 0)
        self.assertEqual(['localhost'], main.state.env.hosts)
        self.assertEqual({'coordinator': ['localhost'],
                          'worker': ['localhost'], 'all': ['localhost']},
                         main.state.env.roledefs)
        self.assertEqual('22', main.state.env.port)
        self.assertEqual('root', main.state.env.user)


if __name__ == '__main__':
    unittest.main()
