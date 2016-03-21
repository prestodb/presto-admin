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


"""
test_prestoadmin
----------------------------------

Tests for `prestoadmin` module.
"""
from optparse import Values
import os
import unittest
from fabric import state

import fabric
from fabric.state import env
from mock import patch

import prestoadmin
from prestoadmin import main
from prestoadmin import topology

# LINTED: the @patch decorators in mock_load_topology and mock_empty_topology
# require that this import be here in order to work properly.
from prestoadmin.standalone.config import StandaloneConfig  # noqa
from prestoadmin.util.exception import ConfigurationError
from tests.unit.base_unit_case import BaseUnitCase


#
# There is a certain amount of magic happening here.
#
# Most of the tests in test_main require that there's configuration information
# loaded in order to validate the argument parsing logic. In order to avoid
# every test in here having to know about the internals of how that
# configuration gets loaded, main.py provides load_config as a patch point.
#
# The tests that need config loaded can patch that with one of the following
# functions as a side-effect. Instead of main.load_config being called, the
# function returned by e.g. mock_load_topology gets called, and it patches
# the config load implementation to achieve the desired result.
#
# The downside of this approach is that any tests function that uses this ends
# up getting an unused mock as a parameter. The upside is that when config load
# inevitably changes, there will be 3 places to change instead of every test.
#
def mock_load_topology():
    @patch('tests.unit.test_main.StandaloneConfig._get_conf_from_file')
    def loader(load_config_callback, get_conf_mock):
        get_conf_mock.return_value = {'username': 'user',
                                      'port': 1234,
                                      'coordinator': 'master',
                                      'workers': ['slave1', 'slave2']}
        return load_config_callback()
    return loader


def mock_empty_topology():
    @patch('tests.unit.test_main.StandaloneConfig._get_conf_from_file')
    def loader(load_config_callback, get_conf_mock):
        get_conf_mock.return_value = {}
        return load_config_callback()
    return loader


def mock_error_topology():
    @patch('tests.unit.test_main.StandaloneConfig._get_conf_from_file')
    @patch('prestoadmin.standalone.config.validate',
           side_effect=ConfigurationError())
    def loader(load_config_callback, validate_mock, get_conf_mock):
        return load_config_callback()
    return loader


class BaseMainCase(BaseUnitCase):
    def setUp(self):
        super(BaseMainCase, self).setUp(capture_output=True, load_config=False)
        # Empty out commands from previous tests.
        fabric.state.commands = {}

    def _run_command_compare_to_file(self, command, exit_status, filename):
        """
            Compares stdout from the CLI to the given file
        """
        current_dir = os.path.abspath(os.path.dirname(__file__))
        expected_path = os.path.join(current_dir, filename)
        input_file = open(expected_path, 'r')
        text = "".join(input_file.readlines())
        input_file.close()
        self._run_command_compare_to_string(command, exit_status,
                                            stdout_text=text)

    def _format_expected_actual(self, expected, actual):
        return '\t\t======== vv EXPECTED vv ========\n%s\n' \
               '\t\t========       !=       ========\n%s\n' \
               '\t\t======== ^^  ACTUAL  ^^ ========\n' % (expected, actual)

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
            actual = self.test_stdout.getvalue()
            self.assertEqual(stdout_text, actual,
                             self._format_expected_actual(stdout_text, actual))

        if stderr_text is not None:
            actual = self.test_stderr.getvalue()
            self.assertEqual(stderr_text, self.test_stderr.getvalue(),
                             self._format_expected_actual(stderr_text, actual))


class TestMain(BaseMainCase):

    # Everything in here needs some kind of mode set. Since they were all
    # written against standalone originally, standalone it is.
    @patch('prestoadmin.mode.get_mode', return_value='standalone')
    def setUp(self, mode_mock):
        super(TestMain, self).setUp()
        reload(prestoadmin)

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
        self.assertTrue("Commands:" in self.test_stdout.getvalue())

    @patch('prestoadmin.main._LOGGER')
    def test_argument_parsing_with_short_command(self, logger_mock):
        self._run_command_compare_to_string(
            ["topology"],
            2,
            stderr_text="\nWarning: Command not found:\n    topology\n\n"
        )
        self.assertTrue("Commands:" in self.test_stdout.getvalue())

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_argument_parsing_with_valid_command(self, unused_load_mock):
        commands = main.parse_and_validate_commands(["topology", "show"])
        self.assertEqual(commands[0][0], "topology.show")

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_argument_parsing_with_arguments(self, unused_load_mock):
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
        self.assertTrue("Commands:" in self.test_stdout.getvalue())

    def assertDefaultRoledefs(self):
        self.assertEqual(main.state.env.roledefs,
                         {'coordinator': ['master'],
                          'worker': ['slave1', 'slave2'],
                          'all': ['master', 'slave1', 'slave2']})

    def assertDefaultHosts(self):
        self.assertEqual(main.state.env.hosts, ['master', 'slave1', 'slave2'])

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_hosts_on_cli_overrides_topology(self, unused_mock_load):
        try:
            main.main(['--hosts', 'master,slave1', 'topology', 'show'])
        except SystemExit as e:
            self.assertEqual(e.code, 0)

        self.assertDefaultRoledefs()
        self.assertEqual(main.state.env.hosts, ['master', 'slave1'])
        self.assertEqual(main.api.env.hosts, ['master', 'slave1'])

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

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    @patch('prestoadmin.main.getpass.getpass')
    def test_initial_password(self, pass_mock, unused_mock_load):
        try:
            main.parse_and_validate_commands(['-I', 'topology', 'show'])
        except SystemExit as e:
            self.assertEqual(0, e.code)
        pass_mock.assert_called_once_with('Initial value for env.password: ')

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_env_vars_persisted(self, unused_mock_load):
        try:
            main.main(['topology', 'show'])
        except SystemExit as e:
            self.assertEqual(e.code, 0)
        self.assertDefaultHosts()

    @patch('prestoadmin.main.load_config', side_effect=mock_empty_topology())
    def test_topology_defaults_override_fabric_defaults(
            self, unused_mock_load):
        self.remove_runs_once_flag(topology.show)
        try:
            main.main(['topology', 'show'])
        except SystemExit as e:
            self.assertEqual(e.code, 0)
        self.assertEqual(['localhost'], main.state.env.hosts)
        self.assertEqual({'coordinator': ['localhost'],
                          'worker': ['localhost'], 'all': ['localhost']},
                         main.state.env.roledefs)
        self.assertEqual(22, main.state.env.port)
        self.assertEqual('root', main.state.env.user)

    def test_fabfile_option_not_present(self):
        self._run_command_compare_to_string(["--fabfile"], 2)
        self.assertTrue("no such option: --fabfile" in
                        self.test_stderr.getvalue())

    def test_rcfile_option_not_present(self):
        self._run_command_compare_to_string(["--config"], 2)
        self.assertTrue("no such option: --config" in
                        self.test_stderr.getvalue())

    @patch('prestoadmin.main.crawl')
    @patch('prestoadmin.fabric_patches.crawl')
    def test_has_args_expecting_none(self, crawl_mock, crawl_mock_main):
        def task():
            """This is my task"""
            pass

        crawl_mock.return_value = task
        crawl_mock_main.return_value = task
        state.env.nodeps = False
        try:
            main.run_tasks([('my task', ['arg1'], {}, [], [], [])])
        except SystemExit as e:
            self.assertEqual(e.code, 2)
        self.assertEqual('Incorrect number of arguments to task.\n\n'
                         'Displaying detailed information for task '
                         '\'my task\':\n\n    This is my task\n\n',
                         self.test_stdout.getvalue())

    @patch('prestoadmin.main.crawl')
    @patch('prestoadmin.fabric_patches.crawl')
    def test_too_few_args(self, crawl_mock, crawl_mock_main):
        def task(arg1):
            """This is my task"""
            pass

        crawl_mock.return_value = task
        crawl_mock_main.return_value = task
        state.env.nodeps = False
        try:
            main.run_tasks([('my task', [], {}, [], [], [])])
        except SystemExit as e:
            self.assertEqual(e.code, 2)
        self.assertEqual('Incorrect number of arguments to task.\n\n'
                         'Displaying detailed information for task '
                         '\'my task\':\n\n    This is my task\n\n',
                         self.test_stdout.getvalue())

    @patch('prestoadmin.main.crawl')
    @patch('prestoadmin.fabric_patches.crawl')
    def test_too_many_args(self, crawl_mock, crawl_mock_main):
        def task(arg1):
            """This is my task"""
            pass

        crawl_mock.return_value = task
        crawl_mock_main.return_value = task
        state.env.nodeps = False
        try:
            main.run_tasks([('my task', ['arg1', 'arg2'], {}, [], [], [])])
        except SystemExit as e:
            self.assertEqual(e.code, 2)
        self.assertEqual('Incorrect number of arguments to task.\n\n'
                         'Displaying detailed information for task '
                         '\'my task\':\n\n    This is my task\n\n',
                         self.test_stdout.getvalue())

    @patch('prestoadmin.main.crawl')
    @patch('prestoadmin.fabric_patches.crawl')
    def test_too_many_args_has_optionals(self, crawl_mock, crawl_mock_main):
        def task(optional=None):
            """This is my task"""
            pass

        crawl_mock.return_value = task
        crawl_mock_main.return_value = task
        state.env.nodeps = False
        try:
            main.run_tasks([('my task', ['arg1', 'arg2'], {}, [], [], [])])
        except SystemExit as e:
            self.assertEqual(e.code, 2)
        self.assertEqual('Incorrect number of arguments to task.\n\n'
                         'Displaying detailed information for task '
                         '\'my task\':\n\n    This is my task\n\n',
                         self.test_stdout.getvalue())

    @patch('prestoadmin.main.crawl')
    @patch('prestoadmin.fabric_patches.crawl')
    def test_too_few_args_has_optionals(self, crawl_mock, crawl_mock_main):
        def task(arg1, optional=None):
            """This is my task"""
            pass

        crawl_mock.return_value = task
        crawl_mock_main.return_value = task
        state.env.nodeps = False
        try:
            main.run_tasks([('my task', [], {}, [], [], [])])
        except SystemExit as e:
            self.assertEqual(e.code, 2)
        self.assertEqual('Incorrect number of arguments to task.\n\n'
                         'Displaying detailed information for task '
                         '\'my task\':\n\n    This is my task\n\n',
                         self.test_stdout.getvalue())

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_env_parallel(self, unused_mock_load):
        main.parse_and_validate_commands(['server', 'install',
                                          "local_path", "--serial"])
        self.assertEqual(env.parallel, False)

        main.parse_and_validate_commands(['server', 'install',
                                          "local_path"])
        self.assertEqual(env.parallel, True)

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_set_vars(self, unused_mock_load_topology):
        main.parse_and_validate_commands(
            ['--set', 'skip_bad_hosts,shell=,hosts=master\,slave1\,slave2,'
                      'skip_unknown_tasks=True,use_shell=False',
             'server', 'install', "local_path"])
        self.assertEqual(env.skip_bad_hosts, True)
        self.assertEqual(env.shell, '')
        self.assertEqual(env.hosts, ['master', 'slave1', 'slave2'])
        self.assertEqual(env.use_shell, False)
        self.assertEqual(env.skip_unknown_tasks, True)

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_nodeps_check(self, unused_mock_load):
        env.nodeps = True
        try:
            main.main(['topology', 'show', '--nodeps'])
        except SystemExit as e:
            self.assertEqual(e.code, 2)
        self.assertTrue('Invalid argument --nodeps to task: topology.show\n'
                        in self.test_stderr.getvalue())
        self.assertTrue('Displaying detailed information for task '
                        '\'topology show\':\n\n    Shows the current topology '
                        'configuration for the cluster (including the\n    '
                        'coordinators, workers, SSH port, and SSH username)'
                        '\n\n' in self.test_stdout.getvalue())

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_skip_bad_hosts(self, unused_mock_load):
        main.parse_and_validate_commands(['server', 'install',
                                          "local_path"])
        self.assertEqual(env.skip_bad_hosts, True)

    def test_get_default_options(self):
        options = Values({'k1': 'dv1', 'k2': 'dv2'})
        non_default_options = Values({'k2': 'V2', 'k3': 'V3'})
        default_options = main.get_default_options(options,
                                                   non_default_options)
        self.assertEqual(default_options, Values({'k1': 'dv1'}))

    #
    # The env.port situation is currently a special kind of hell. There are a
    # bunch of different ways for port to get set:
    # 1) Topology exists, port in it: port is an int.
    # 2) Topology exists, port is NOT in it: port is an int.
    # 3) --port CLI option: port is a string
    # 4) Interactive config: port is an int.
    #
    # What should it be? Probably an int being as it's a port *number* and all.
    # What should we likely settle on? Probably string, because that's what
    #   fabric sets the default to in env.
    # Is this a terrible situation? Yes; we need to clean it up.
    #
    # Note that interactive config isn't tested here. because getting input fed
    # into main.main()'s stdin seems problematic with all the magic the tests
    # are already doing.
    #

    # PORT CASE 1
    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_unchanged_hosts(self, unused_mock_load):
        """
        Possible alternate name for the test: test_does_my_magic_work
        """
        main.parse_and_validate_commands(
            args=['server', 'uninstall'])
        self.assertDefaultHosts()
        self.assertDefaultRoledefs()
        self.assertEqual(env.port, 1234)
        self.assertEqual(env.user, 'user')
        self.assertNotIn('conf_hosts', env)

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_specific_hosts_long_option(self, unused_mock_load):
        main.parse_and_validate_commands(
            args=['--hosts', 'master', 'server', 'uninstall'])
        self.assertEqual(env.hosts, ['master'])
        self.assertNotIn('cli_hosts', env)

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_specific_hosts_short_option(self, unused_mock_load):
        main.parse_and_validate_commands(
            args=['-H', 'master,slave2', 'server', 'uninstall'])
        self.assertEqual(env.hosts, ['master', 'slave2'])

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_generic_set_hosts(self, unused_mock_load):
        main.parse_and_validate_commands(
            args=['--set', 'hosts=master\,slave2', 'server', 'uninstall'])
        self.assertEqual(env.hosts, ['master', 'slave2'])
        self.assertNotIn('env_settings', env)

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_generic_invalid_host(self, unused_mock_load):
        self.assertRaises(
            ConfigurationError, main.parse_and_validate_commands,
            args=['--set', 'hosts=bogushost\,slave2', 'server', 'uninstall'])

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_specific_overrides_generic(self, unused_mock_load):
        main.parse_and_validate_commands(
            args=['-H', 'master,slave1', '--set', 'hosts=master\,slave2',
                  'server', 'uninstall'])
        self.assertEqual(env.hosts, ['master', 'slave1'])

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_host_not_in_conf(self, unused_mock_load):
        self.assertRaises(
            ConfigurationError, main.parse_and_validate_commands,
            args=['--hosts', 'non_conf_host', 'server', 'uninstall'])

    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_host_not_in_conf_short_option(self, unused_mock_load):
        self.assertRaises(
            ConfigurationError, main.parse_and_validate_commands,
            args=['-H', 'non_conf_host', 'server', 'uninstall'])

    # PORT CASE 3
    @patch('prestoadmin.main.load_config', side_effect=mock_load_topology())
    def test_cli_overrides_config(self, unused_mock_load):
        main.parse_and_validate_commands(
            args=['-H', 'master,slave1', '-u', 'other_user', '--port', '2179',
                  'server', 'uninstall'])
        self.assertEqual(env.hosts, ['master', 'slave1'])
        self.assertEqual(env.user, 'other_user')
        self.assertEqual(env.port, '2179')

    # PORT CASE 2
    @patch('prestoadmin.main.load_config', side_effect=mock_empty_topology())
    def test_default_topology(self, unused_mock_load):
        main.parse_and_validate_commands(args=['server', 'uninstall'])
        self.assertEqual(env.port, 22)
        self.assertEqual(env.user, 'root')
        self.assertEqual(env.hosts, ['localhost'])

    @patch('prestoadmin.main.load_config', side_effect=mock_error_topology())
    def test_error_topology(self, unused_mock_load):
        self.assertRaises(ConfigurationError, main.parse_and_validate_commands,
                          args=['server', 'uninstall'])

if __name__ == '__main__':
    unittest.main()
