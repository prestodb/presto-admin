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
# In principle, this and TestStandaloneHelp could inherit the test method from
# a common abstract base class, which would mean the test methods wouldn't have
# to be duplicated. In practice, nosetests doesn't understand abstract base
# classes, and tries to instantiate it, hence the duplication. In principle, we
# could probably also have a non-abstract base class that takes a couple
# additional constructor parameters for the mode_mock's return value and for
# the expected help file paths, but I'm guessing we'd run into much the same
# problem (i.e. nosetests tries to instantiate a class that it doesn't know how
# to supply the appropriate constructor args to, and that doesn't really have
# sensible defaults). There's probably a solution that involves not duplicating
# all of the tests, but finding it will need to wait until somebody has time to
# dig into it.
#
# The other thing to keep in mind is that the help tests end up (many levels
# in) updating fabric.state.commands, and you need to clear it out in order for
# the second test case to run correctly. BaseMainCase.setUp does this because
# TestMain also ends up updating fabric.state.commands, and therefore ought to
# clear it too.
#
class TestSliderHelp(BaseMainCase):
    @patch('prestoadmin.mode.get_mode', return_value='yarn_slider')
    def setUp(self, mode_mock):
        super(TestSliderHelp, self).setUp()
        reload(prestoadmin)

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
