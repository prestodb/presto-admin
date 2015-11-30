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
unit tests for plugin module
"""
from mock import patch
from prestoadmin import plugin
from tests.unit.base_unit_case import BaseUnitCase


class TestPlugin(BaseUnitCase):
    @patch('prestoadmin.plugin.write')
    def test_add_jar(self, write_mock):
        plugin.add_jar('/my/local/path.jar', 'hive-hadoop2')
        write_mock.assert_called_with(
            '/my/local/path.jar', '/usr/lib/presto/lib/plugin/hive-hadoop2')

    @patch('prestoadmin.plugin.write')
    def test_add_jar_provide_dir(self, write_mock):
        plugin.add_jar('/my/local/path.jar', 'hive-hadoop2',
                       '/etc/presto/plugin')
        write_mock.assert_called_with('/my/local/path.jar',
                                      '/etc/presto/plugin/hive-hadoop2')
