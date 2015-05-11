"""
General utilities for running tests.  To be able to use the methods in
BaseTestCase, your test cases should extend BaseTestCase rather than
unittest.TestCase
"""

import copy
import logging
import re
import StringIO
import sys
import unittest
from fabric.state import env


class BaseTestCase(unittest.TestCase):
    test_stdout = None
    test_stderr = None
    old_stdout = sys.__stdout__
    old_stderr = sys.__stderr__
    env_vars = None

    def setUp(self):
        self.capture_stdout_stderr()
        self.env_vars = copy.deepcopy(env)
        logging.disable(logging.CRITICAL)

    def capture_stdout_stderr(self):
        sys.stdout = self.test_stdout = StringIO.StringIO()
        sys.stderr = self.test_stderr = StringIO.StringIO()

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

    # This method is equivalent to Python 2.7's unittest.assertRaisesRegexp()
    def assertRaisesRegexp(self, expected_exception, expected_regexp,
                           callable_object, *args):
        try:
            callable_object(*args)
        except expected_exception as e:
            self.assertTrue(re.search(expected_regexp, str(e)),
                            repr(expected_regexp) + " not found in "
                            + repr(str(e)))
        else:
            self.fail("Expected exception " + str(expected_exception) +
                      " not raised")

    # equivalent to python 2.7's unittest.assertRegexpMatches()
    def assertRegexpMatches(self, text, expected_regexp, msg=None):
        self.assertTrue(re.search(expected_regexp, text), msg)

    def remove_runs_once_flag(self, callable_obj):
        # since we annotated show with @runs_once, we need to delete the
        # attribute the Fabric decorator gives it to indicate that it has
        # already run once in this session
        if hasattr(callable_obj, 'return_value'):
            delattr(callable_obj.wrapped, 'return_value')

    def tearDown(self):
        self.restore_stdout_stderr()
        env.clear()
        env.update(self.env_vars)
        logging.disable(logging.NOTSET)
