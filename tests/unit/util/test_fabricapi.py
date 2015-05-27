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
Tests the utility
"""
from fabric.api import env
from prestoadmin.util import fabricapi

import tests.utils as utils


class TestFabricapi(utils.BaseTestCase):
    def test_get_host_with_exclude(self):
        env.hosts = ['a', 'b', 'bad']
        env.exclude_hosts = ['bad']
        self.assertEqual(fabricapi.get_host_list(), ['a', 'b'])
