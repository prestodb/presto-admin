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
from optparse import Values

import os
from fabric.state import env
import prestoadmin
import unittest
import utils

from prestoadmin import main
from prestoadmin.config import ConfigFileNotFoundError
from prestoadmin.config import ConfigurationError
from mock import patch


class TestMain(utils.BaseTestCase):

    def _run_command_compare_to_file(self, command, exit_status, filename):
        """
            Compares stdout from the CLI to the given file
        """
        current_dir = os.path.abspath(os.path.dirname(__file__))
        input_file = open(current_dir + filename, 'r')
        text = "".join(input_file.readlines())
        input_file.close()
        self._run_command_compare_to_string(command, exit_status,
                                            stdout_text=text)

    def _run_command_compare_to_string(self, command, exit_status,
                                       stdout_text=None, stderr_text=None):
        """
            Compares stdout from the CLI to the given string
        """
        try:
            main.parse_and_validate_commands(command)
        except SystemExit as e:
            self.assertEqual(e.code, exit_status)

        if stdout_text is not None:
            self.assertEqual(stdout_text, self.test_stdout.getvalue())

        if stderr_text is not None:
            self.assertEqual(stderr_text, self.test_stderr.getvalue())

    def test_help_text_short(self):
        # See if the help text matches what we expect it to be (in
        # tests/help.txt)
        self._run_command_compare_to_file(["-h"], 0, "/files/help.txt")

    def test_help_text_long(self):
        self._run_command_compare_to_file(["--help"], 0, "/files/help.txt")

    def test_help_displayed_with_no_args(self):
        self._run_command_compare_to_file([], 0, "/files/help.txt")

    def test_list_commands(self):
        # Note: this will have to be updated whenever we add a new command
        self._run_command_compare_to_file(["-l"], 0, "/files/list.txt")

    def test_version(self):
        # Note: this will have to be updated whenever we have a new version.
        self._run_command_compare_to_string(["--version"], 0,
                                            stdout_text="presto-admin %s\n" %
                                            prestoadmin.__version__)

    @patch('prestoadmin.main._LOGGER')
    def test_argument_parsing_with_invalid_command(self, logger_mock):
        self._run_command_compare_to_string(
            ["hello", "world"],
            2,
            stderr_text="\nWarning: Command not found:\n    hello world\n\n"
        )
        self.assertTrue("Available commands:" in self.test_stdout.getvalue())

    @patch('prestoadmin.main._LOGGER')
    def test_argument_parsing_with_short_command(self, logger_mock):
        self._run_command_compare_to_string(
            ["topology"],
            2,
            stderr_text="\nWarning: Command not found:\n    topology\n\n"
        )
        self.assertTrue("Available commands:" in self.test_stdout.getvalue())

    def test_argument_parsing_with_valid_command(self):
        commands = main.parse_and_validate_commands(["topology", "show"])
        self.assertEqual(commands[0][0], "topology.show")

    def test_argument_parsing_with_arguments(self):
        commands = main.parse_and_validate_commands(["topology", "show", "f"])
        self.assertEqual(commands[0][0], "topology.show")
        self.assertEqual(commands[0][1], ["f"])

    def test_arbitrary_remote_shell_disabled(self):
        self._run_command_compare_to_string(
            ["--", "echo", "hello"],
            2,
            stderr_text="\nWarning: Arbitrary remote shell commands not "
                        "supported.\n\n"
        )
        self.assertTrue("Available commands:" in self.test_stdout.getvalue())

    @patch('prestoadmin.main.topology.get_conf')
    def test_load_topology(self, get_conf_mock):
        get_conf_mock.return_value = {'username': 'user',
                                      'port': '1234',
                                      'coordinator': 'hello',
                                      'workers': ['a', 'b']}
        main.load_topology()
        self.assertEqual(main.state.env.roledefs,
                         {'coordinator': ['hello'], 'worker': ['a', 'b'],
                          'all': ['hello', 'a', 'b']})
        self.assertEqual(main.state.env.port, '1234')
        self.assertEqual(main.state.env.user, 'user')
        self.assertEqual(main.state.env.hosts, ['hello', 'a', 'b'])

    @patch('prestoadmin.main.topology.get_conf')
    def test_load_topology_not_exists(self, get_conf_mock):
        e = ConfigFileNotFoundError()

        get_conf_mock.side_effect = e
        main.load_topology()
        self.assertEqual(main.state.env.roledefs,
                         {'coordinator': [], 'worker': [], 'all': []})
        self.assertEqual(main.state.env.port, '22')
        self.assertNotEqual(main.state.env.user, 'user')
        self.assertEqual(main.state.env.topology_config_not_found, e)

    @patch('prestoadmin.topology.get_conf')
    def test_load_topology_failure(self, get_conf_mock):
        e = ConfigurationError()

        get_conf_mock.side_effect = e
        self.assertRaises(ConfigurationError, main.load_topology)

    @patch('prestoadmin.main.topology.get_conf')
    def test_hosts_on_cli_overrides_topology(self, get_conf_mock):
        get_conf_mock.return_value = {'username': 'root', 'port': '22',
                                      'coordinator': 'hello',
                                      'workers': ['a', 'b']}
        try:
            main.main(['--hosts', 'hello,a', 'topology', 'show'])
        except SystemExit as e:
            self.assertEqual(e.code, 0)

        self.assertEqual(main.state.env.roledefs,
                         {'coordinator': ['hello'], 'worker': ['a', 'b'],
                          'all': ['hello', 'a', 'b']})
        self.assertEqual(main.state.env.hosts, ['hello', 'a'])
        self.assertEqual(main.api.env.hosts, ['hello', 'a'])

    def test_describe(self):
        self._run_command_compare_to_string(
            ['-d', 'topology', 'show'],
            0,
            "Displaying detailed information for task 'topology show':\n\n   "
            " Shows the current topology configuration for the cluster "
            "(including the\n    coordinators, workers, SSH port, and SSH "
            "username)\n\n"
        )

    def test_describe_with_args(self):
        self._run_command_compare_to_string(
            ['-d', 'topology', 'show', 'arg'],
            0,
            "Displaying detailed information for task 'topology show':\n\n   "
            " Shows the current topology configuration for the cluster "
            "(including the\n    coordinators, workers, SSH port, and SSH "
            "username)\n\n"
        )

    def test_shortlist(self):
        self._run_command_compare_to_file(["--shortlist"], 0,
                                          "/files/shortlist.txt")

    @patch('prestoadmin.main.getpass.getpass')
    def test_initial_password(self, pass_mock):
        try:
            main.parse_and_validate_commands(['-I', 'topology', 'show'])
        except SystemExit as e:
            self.assertEqual(0, e.code)
        pass_mock.assert_called_once_with('Initial value for env.password: ')

    @patch('prestoadmin.main.topology.get_conf')
    def test_env_vars_persisted(self, get_conf_mock):
        get_conf_mock.return_value = {'username': 'user', 'port': '1234',
                                      'coordinator': 'hello',
                                      'workers': ['a', 'b']}
        try:
            main.main(['topology', 'show'])
        except SystemExit as e:
            self.assertEqual(e.code, 0)
        self.assertEqual(['hello', 'a', 'b'], main.state.env.hosts)

    @patch('prestoadmin.topology._get_conf_from_file')
    def test_topology_defaults_override_fabric_defaults(self, get_conf_mock):
        self.remove_runs_once_flag(main.topology.show)
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

    def test_fabfile_option_not_present(self):
        self._run_command_compare_to_string(["--fabfile"], 2)
        self.assertTrue("no such option: --fabfile" in
                        self.test_stderr.getvalue())

    def test_rcfile_option_not_present(self):
        self._run_command_compare_to_string(["--config"], 2)
        self.assertTrue("no such option: --config" in
                        self.test_stderr.getvalue())

    def test_extended_help(self):
        self._run_command_compare_to_file(['--extended-help'], 0,
                                          "/files/extended-help.txt")

    @patch('prestoadmin.main.load_topology')
    def test_wrong_arguments_expecting_none(self, load_topology_mock):
        self.remove_runs_once_flag(main.topology.show)
        try:
            main.main(['topology', 'show', "extra_arg"])
        except SystemExit as e:
            self.assertEqual(e.code, 2)
        self.assertTrue('Invalid argument(s) to task.\n\nDisplaying '
                        'detailed information for task \'topology show\''
                        in self.test_stdout.getvalue())

    def test_wrong_arguments_expecting_fewer(self):
        self.remove_runs_once_flag(prestoadmin.server.install)
        try:
            main.main(['server', 'install', "local_path", "extra_arg"])
        except SystemExit as e:
            self.assertEqual(e.code, 2)
        self.assertTrue('Invalid argument(s) to task.\n\nDisplaying '
                        'detailed information for task \'server install\''
                        in self.test_stdout.getvalue())

    @patch('prestoadmin.main.topology.get_conf')
    def test_hosts_no_topology_raises_error(self, conf_mock):
        conf_mock.side_effect = ConfigFileNotFoundError
        self.assertRaisesRegexp(ConfigurationError,
                                "Hosts defined in --hosts/-H must be in the "
                                "topology file",
                                main._update_env,
                                Values(), Values({'hosts': 'master'}))

    @patch('prestoadmin.main.topology.get_conf')
    def test_hosts_not_in_topology_raises_error(self, conf_mock):
        conf_mock.return_value = {'username': 'root',
                                  'port': '22',
                                  'coordinator': 'master',
                                  'workers': ['slave']}
        self.assertRaisesRegexp(ConfigurationError,
                                "Hosts defined in --hosts/-H must be in the "
                                "topology file",
                                main._update_env,
                                Values(), Values({'hosts': 'bob'}))

    def test_env_parallel(self):
        main.parse_and_validate_commands(['server', 'install',
                                          "local_path", "--serial"])
        self.assertEqual(env.parallel, False)

        main.parse_and_validate_commands(['server', 'install',
                                          "local_path"])
        self.assertEqual(env.parallel, True)

    def test_set_vars(self):
        main.parse_and_validate_commands(
            ['--set', 'skip_bad_hosts,shell=,hosts=m\,slave1\,slave2,'
                      'skip_unknown_tasks=True,use_shell=False',
             'server', 'install', "local_path"])
        self.assertEqual(env.skip_bad_hosts, True)
        self.assertEqual(env.shell, '')
        self.assertEqual(env.hosts, ['m', 'slave1', 'slave2'])
        self.assertEqual(env.use_shell, False)
        self.assertEqual(env.skip_unknown_tasks, True)

    def test_env_abort_on_error(self):
        main.parse_and_validate_commands(['server', 'install',
                                          "local_path", "--abort-on-error"])
        self.assertEqual(env.warn_only, False)

        main.parse_and_validate_commands(['server', 'install',
                                          "local_path"])
        self.assertEqual(env.warn_only, True)

if __name__ == '__main__':
    unittest.main()
