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
Tests the LoggingOptionParser
"""
from StringIO import StringIO

from prestoadmin.util.parser import LoggingOptionParser
from prestoadmin.util.hiddenoptgroup import HiddenOptionGroup
from tests.base_test_case import BaseTestCase


class TestParser(BaseTestCase):
    def test_print_extended_help(self):
        parser = LoggingOptionParser(usage="Hello World")
        parser.add_option_group("a")
        hidden_group = HiddenOptionGroup(parser, "b", suppress_help=True)
        non_hidden_group = HiddenOptionGroup(parser, "c", suppress_help=False)
        parser.add_option_group(hidden_group)
        parser.add_option_group(non_hidden_group)

        help_out = StringIO()
        parser.print_help(help_out)
        self.assertEqual(help_out.getvalue(),
                         "Usage: Hello World\n\nOptions:\n  -h, --help  show "
                         "this help message and exit\n\n  a:\n\n\n  c:\n")

        extended_help_out = StringIO()
        parser.print_extended_help(extended_help_out)
        self.assertEqual(extended_help_out.getvalue(),
                         "Usage: Hello World\n\nOptions:\n  -h, --help  show "
                         "this help message and exit\n\n  a:\n\n  b:\n\n  "
                         "c:\n")
