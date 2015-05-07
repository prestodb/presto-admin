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
from mock import patch
from prestoadmin.util import fabricapi

from prestoadmin.util.fabricapi import execute_fail_on_error
import tests.utils as utils


class TestFabricapi(utils.BaseTestCase):
    @patch('prestoadmin.util.fabricapi.execute')
    def test_execute_fail(self, mock_execute):
        def dummyfunc():
            pass

        result = {}
        result['some_host'] = Exception()
        mock_execute.return_value = result

        self.assertRaisesRegexp(Exception,
                                "command failed for some nodes; "
                                "result={'some_host': Exception\(\)}",
                                execute_fail_on_error, dummyfunc)

    def test_get_host_with_exclude(self):
        env.hosts = ['a', 'b', 'bad']
        env.exclude_hosts = ['bad']
        self.assertEqual(fabricapi.get_host_list(), ['a', 'b'])
