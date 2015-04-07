"""
General utilities for running tests.  To be able to use the methods in
BaseTestCase, your test cases should extend BaseTestCase rather than
unittest.TestCase
"""

import copy
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

    def tearDown(self):
        self.restore_stdout_stderr()
        env.clear()
        env.update(self.env_vars)
