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

from prestoadmin.standalone.config import _expand_host
from tests.unit.base_unit_case import BaseUnitCase


class TestExpandHost(BaseUnitCase):

    def test_basic_expand_host_01(self):
        input_host = "worker0[1-2].example.com"
        expected = ["worker01.example.com", "worker02.example.com"]
        self.assertEqual(expected, _expand_host(input_host))

    def test_basic_expand_host_02(self):
        input_host = "worker[01-02].example.com"
        expected = ["worker01.example.com", "worker02.example.com"]
        self.assertEqual(expected, _expand_host(input_host))

    def test_expand_host_include_hyphen(self):
        input_host = "cdh5-[1-2].example.com"
        expected = ["cdh5-1.example.com", "cdh5-2.example.com"]
        self.assertEqual(expected, _expand_host(input_host))

    def test_not_expand_host(self):
        input_host = "worker1.example.com"
        expected = ["worker1.example.com"]
        self.assertEqual(expected, _expand_host(input_host))

    def test_except_expand_host(self):
        input_host = "worker0[3-2].example.com"
        self.assertRaises(ValueError, _expand_host, input_host)
