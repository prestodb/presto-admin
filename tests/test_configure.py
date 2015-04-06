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
Tests setting the presto configure
"""

from prestoadmin import configure
import utils


class TestConfigure(utils.BaseTestCase):
    def test_output_format_dict(self):
        conf = {'a': 'b', 'c': 'd'}
        self.assertEqual(configure.output_format(conf),
                         "a=b\nc=d")

    def test_output_format_list(self):
        self.assertEqual(configure.output_format(['a', 'b']),
                         'a\nb')

    def test_output_format_string(self):
        conf = "A string"
        self.assertEqual(configure.output_format(conf), conf)

    def test_output_format_int(self):
        conf = 1
        self.assertEqual(configure.output_format(conf), str(conf))
