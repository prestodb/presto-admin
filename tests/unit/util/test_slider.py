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
Tests for utility functions for dealing with slider
"""

import os

from prestoadmin.util.slider import degarbage_json

from tests.unit.base_unit_case import BaseUnitCase


class TestSlider(BaseUnitCase):
    def inhale_file(self, relpath):
        current_dir = os.path.abspath(os.path.dirname(__file__))
        fullpath = os.path.join(current_dir, relpath)
        with open(fullpath, 'r') as input_file:
            return "".join(input_file.readlines())

    def test_degarbage_real_output(self):
        garbage_in = self.inhale_file(
            os.path.join('resources', 'slider-status.txt'))
        garbage_out = self.inhale_file(
            os.path.join('resources', 'slider-status-clean.txt')).strip()

        actual = degarbage_json(garbage_in)
        self.assertEqual(garbage_out, actual,
                         self.format_expected_actual(garbage_out, actual))

    def test_simple(self):
        garbage_in = "Again { the }morning's come"
        garbage_out = '{ the }'
        self.assertEqual(garbage_out, degarbage_json(garbage_in))

    def test_no_braces(self):
        garbage_in = "Again he's on the run"
        self.assertIsNone(degarbage_json(garbage_in))

    def test_more_left(self):
        garbage_in = 'Sunbeams shining { { through } his hair'
        self.assertIsNone(degarbage_json(garbage_in))

    def test_more_right(self):
        garbage_in = 'Appearing { not { to } have } a } care'
        garbage_out = '{ not { to } have }'
        self.assertEqual(garbage_out, degarbage_json(garbage_in))

    def test_second_json(self):
        garbage_in = '{ Well pick up your gear}{ and, Gypsy, roll on'
        garbage_out = '{ Well pick up your gear}'
        self.assertEqual(garbage_out, degarbage_json(garbage_in))

    def test_no_left(self):
        garbage_in = 'Roll on...}}'
        self.assertIsNone(degarbage_json(garbage_in))
