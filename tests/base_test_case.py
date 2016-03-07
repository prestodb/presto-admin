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
General utilities for running tests.  To be able to use the methods in
BaseTestCase, your test cases should extend BaseTestCase rather than
unittest.TestCase
"""

import copy
import logging
import os
import re
import StringIO
import sys
import tempfile
import unittest

from fabric.state import env

from prestoadmin.util import constants


class BaseTestCase(unittest.TestCase):
    test_stdout = None
    test_stderr = None
    old_stdout = sys.__stdout__
    old_stderr = sys.__stderr__
    env_vars = None

    def setUp(self, capture_output=False):
        if capture_output:
            self.capture_stdout_stderr()
        self.env_vars = copy.deepcopy(env)
        logging.disable(logging.CRITICAL)
        self.redirect_log_to_tmp()

    def capture_stdout_stderr(self):
        sys.stdout = self.test_stdout = StringIO.StringIO()
        sys.stderr = self.test_stderr = StringIO.StringIO()

    def redirect_log_to_tmp(self):
        # monkey patch the log directory constant so that
        # we force log files to a temporary dir
        self.__old_prestoadmin_log = constants.PRESTOADMIN_LOG_DIR
        self.__temporary_dir_path = tempfile.mkdtemp(
            prefix='app-int-test-'
        )
        constants.PRESTOADMIN_LOG_DIR = self.__temporary_dir_path

    def restore_log_and_delete_temp_dir(self):
        # restore the log constant
        constants.PRESTOADMIN_LOG_DIR = self.__old_prestoadmin_log

        # clean up the temporary directory
        os.system('rm -rf ' + self.__temporary_dir_path)

    def restore_stdout_stderr(self):
        if self.test_stdout:
            self.test_stdout.close()
        sys.stdout = self.old_stdout

        if self.test_stderr:
            self.test_stderr.close()
        sys.stderr = self.old_stderr

    def restore_stdout_stderr_keep_open(self):
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

    # This method is equivalent to Python 2.7's unittest.assertIn()
    def assertIsNone(self, foo, msg=None):
        self.assertTrue(foo is None, msg=msg)

    # This method is equivalent to Python 2.7's unittest.assertIn()
    def assertIn(self, member, container, msg=None):
        self.assertTrue(member in container, msg=msg)

    # This method is equivalent to Python 2.7's unittest.assertNotIn()
    def assertNotIn(self, member, container, msg=None):
        self.assertTrue(member not in container, msg=msg)

    # This method is equivalent to Python 2.7's unittest.assertRaisesRegexp()
    def assertRaisesRegexp(self, expected_exception, expected_regexp,
                           callable_object, *args, **kwargs):
        # Copy kwargs so we remove msg from the copy before passing it into
        # callable_object. This lets us use this assertion with callables that
        # don't expect to get an msg parameter.
        callable_kwargs = kwargs.copy()
        msg = ''

        if 'msg' in kwargs:
            del callable_kwargs['msg']
            if kwargs['msg']:
                msg = '\n' + kwargs['msg']

        try:
            callable_object(*args, **callable_kwargs)
        except expected_exception as e:
            self.assertTrue(re.search(expected_regexp, str(e)),
                            repr(expected_regexp) + " not found in " +
                            repr(str(e)) + msg)
        else:
            self.fail("Expected exception " + str(expected_exception) +
                      " not raised" + msg)

    def assertRaisesMessageIgnoringOrder(self, expected_exception,
                                         expected_msg, callable_object,
                                         *args, **kwargs):
        try:
            callable_object(*args, **kwargs)
        except expected_exception as e:
            self.assertEqualIgnoringOrder(expected_msg, str(e))
        else:
            self.fail("Expected exception " + str(expected_exception) +
                      " not raised")

    def assertLazyMessage(self, msg_func, assert_function, *args, **kwargs):
        try:
            assert_function(*args, **kwargs)
        except AssertionError:
            self.fail(msg=msg_func())

    # equivalent to python 2.7's unittest.assertRegexpMatches()
    def assertRegexpMatches(
            self, text, expected_regexp, msg="Regexp didn't match"):
        self.assertTrue(re.search(expected_regexp, text), msg)

    def assertRegexpMatchesLineByLine(self, actual_lines,
                                      expected_regexp_lines, msg=None):
        for expected_regexp, actual_line in zip(sorted(expected_regexp_lines),
                                                sorted(actual_lines)):
            try:
                self.assertRegexpMatches(actual_line, expected_regexp, msg=msg)
            except AssertionError:
                self.assertEqualIgnoringOrder('\n'.join(actual_lines),
                                              '\n'.join(expected_regexp_lines))

    def remove_runs_once_flag(self, callable_obj):
        # since we annotated show with @runs_once, we need to delete the
        # attribute the Fabric decorator gives it to indicate that it has
        # already run once in this session
        if hasattr(callable_obj, 'return_value'):
            delattr(callable_obj.wrapped, 'return_value')

    def assertEqualIgnoringOrder(self, one, two):
        self.assertEqual([line.rstrip() for line in sorted(one.splitlines())],
                         [line.rstrip() for line in sorted(two.splitlines())])

    def tearDown(self):
        self.restore_stdout_stderr()
        env.clear()
        env.update(self.env_vars)
        logging.disable(logging.NOTSET)
        self.restore_log_and_delete_temp_dir()
