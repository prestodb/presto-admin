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


# Consult the comment on yarn_slider.test_help.TestSliderHelp for more info.
class TestStandaloneHelp(BaseMainCase):
    @patch('prestoadmin.mode.get_mode', return_value='standalone')
    def setUp(self, mode_mock):
        super(TestStandaloneHelp, self).setUp()
        reload(prestoadmin)
        reload(main)

    def get_short_help_path(self):
        return os.path.join('resources', 'standalone-help.txt')

    def get_extended_help_path(self):
        return os.path.join('resources', 'standalone-extended-help.txt')

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
