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

import unittest
import StringIO
import sys

import prestoadmin
from prestoadmin import main


class TestMain(unittest.TestCase):
    stdout = None
    old_stdout = sys.__stdout__

    def setUp(self):
        # We redirect the stdout for all of the tests, because optparse
        # sometimes just prints to stdout and exits.
        self.stdout = StringIO.StringIO()
        sys.stdout = self.stdout
        pass

    def run_command_compare_to_file(self, command, exit_status, filename):
        """
            Compares stdout from the CLI to the given file
        """
        input_file = open(filename, 'r')
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

        self.assertEqual(self.stdout.getvalue(), text)

    def test_help_text_short(self):
        # See if the help text matches what we expect it to be (in
        # tests/help.txt)
        self.run_command_compare_to_file(["-h"], 0, "tests/files/help.txt")

    def test_help_text_long(self):
        self.run_command_compare_to_file(["--help"], 0, "tests/files/help.txt")

    def test_help_displayed_with_no_args(self):
        self.run_command_compare_to_file([], 0, "tests/files/help.txt")

    def test_list_commands(self):
        # Note: this will have to be updated whenever we add a new command
        self.run_command_compare_to_file(["-l"], 0, "tests/files/list.txt")

    def test_version(self):
        # Note: this will have to be updated whenever we have a new version.
        self.run_command_compare_to_string(["--version"], 0,
                                           "presto-admin %s\n" %
                                           prestoadmin.___version___)

    def test_argument_parsing_with_invalid_command(self):
        old_stderr = sys.stderr
        sys.stderr = test_stderr = StringIO.StringIO()
        try:
            main.parse_and_validate_commands(["hello", "world"])
        except SystemExit as e:
            self.assertEqual(e.code, 2)

        self.assertEqual(test_stderr.getvalue(), "\nWarning: Command not " +
                         "found:\n    hello world\n\n")
        self.assertTrue("Available commands:" in self.stdout.getvalue())
        sys.stderr = old_stderr

    def test_argument_parsing_with_short_command(self):
        old_stderr = sys.stderr
        sys.stderr = test_stderr = StringIO.StringIO()
        try:
            main.parse_and_validate_commands(["topology"])
        except SystemExit as e:
            self.assertEqual(e.code, 2)

        self.assertEqual(test_stderr.getvalue(), "\nWarning: Command not " +
                         "found:\n    topology\n\n")
        self.assertTrue("Available commands:" in self.stdout.getvalue())
        sys.stderr = old_stderr

    def test_argument_parsing_with_valid_command(self):
        commands = main.parse_and_validate_commands(["topology", "show"])
        self.assertEqual(commands[0][0], "topology.show")

    def test_argument_parsing_with_arguments(self):
        commands = main.parse_and_validate_commands(["topology", "show", "f"])
        self.assertEqual(commands[0][0], "topology.show")
        self.assertEqual(commands[0][1], ["f"])

    def test_arbitrary_remote_shell_disabled(self):
        old_stderr = sys.stderr
        sys.stderr = test_stderr = StringIO.StringIO()
        try:
            main.parse_and_validate_commands(["--", "echo", "hello"])
        except SystemExit as e:
            self.assertEqual(e.code, 2)

        self.assertEqual(test_stderr.getvalue(), "\nWarning: Arbitrary "
                         "remote shell commands not supported.\n\n")
        self.assertTrue("Available commands:" in self.stdout.getvalue())
        sys.stderr = old_stderr

    # Test with too many arguments/make that error look much prettier

    def tearDown(self):
        self.stdout.close()
        sys.stdout = self.old_stdout

if __name__ == '__main__':
    unittest.main()
