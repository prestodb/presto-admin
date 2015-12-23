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

from mock import patch
import os

import prestoadmin
from prestoadmin import main

from tests.unit.test_main import BaseMainCase


#
# Getting this and TestStandaloneHelp running, and subsequently running
# successfully in the same run was a treat and a joy not to be missed.
#
#                                      A
# Because the import runs way up there |, and __init__.py runs get_mode, it's
# basically impossible to patch get_mode using the usual mechanisms; the mode
# has long been set by the time we get to setUp or any of the tests. Instead,
# we patch it down here, and then reload the prestoadmin module to re-execute
# the code that calls get_mode and sets up the imports and __all__.
#
# The other thing to keep in mind is that the help tests end up (many levels
# in) updating fabric.state.commands, and you need to clear it out in order for
# the second test case to run correctly. BaseMainCase.setUp does this because
# TestMain also ends up updating fabric.state.commands, and therefore ought to
# clear it too.
#
# There's a lot of duplication between this and TestStandaloneHelp. Here are a
# few things that don't work to remove it:
#
# Have a common abstract base class. Nosetests tries to instantiate it.
# Mark the base class @nottest. Nosetests doesn't find the tests in the
#     concrete classes.
# Common non-abstract base class with additional constructor args. Nosetest
#     will probably try to instantiate that too.
# Multiple inheritance. Now you have two problems ;-)
#
class TestSliderHelp(BaseMainCase):
    @patch('prestoadmin.mode.get_mode', return_value='yarn_slider')
    def setUp(self, mode_mock):
        super(TestSliderHelp, self).setUp()
        reload(prestoadmin)
        reload(main)

    def get_short_help_path(self):
        return os.path.join('resources', 'slider-help.txt')

    def get_extended_help_path(self):
        return os.path.join('resources', 'slider-extended-help.txt')

    def test_standalone_help_text_short(self):
        self._run_command_compare_to_file(
            ["-h"], 0, self.get_short_help_path())

    def test_standalone_help_text_long(self):
        self._run_command_compare_to_file(
            ["--help"], 0, self.get_short_help_path())

    def test_standalone_help_displayed_with_no_args(self):
        self._run_command_compare_to_file(
            [], 0, self.get_short_help_path())

    def test_standalone_extended_help(self):
        self._run_command_compare_to_file(
            ['--extended-help'], 0, self.get_extended_help_path())
