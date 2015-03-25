"""
General utilities for running tests.  To be able to use the methods in
BaseTestCase, your test cases should extend BaseTestCase rather than
unittest.TestCase
"""

import re
import unittest


class BaseTestCase(unittest.TestCase):

    # This method is equivalent to Python 2.7's unittest.assertRaisesRegexp()
    def assertRaisesRegexp(self, expected_exception, expected_regexp,
                           callable_object, *args):
        try:
            callable_object(*args)
        except expected_exception as e:
            self.assertTrue(re.search(expected_regexp, str(e)),
                            repr(expected_regexp) + " not found in "
                            + repr(str(e)))
