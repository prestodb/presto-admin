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
Tests the script module
"""

from mock import patch, call
from prestoadmin import file

from tests.unit.base_unit_case import BaseUnitCase


class TestFile(BaseUnitCase):

    @patch('prestoadmin.file.sudo')
    @patch('prestoadmin.file.put')
    def test_script_basic(self, put_mock, sudo_mock):
        file.run('/my/local/path/script.sh')
        put_mock.assert_called_with('/my/local/path/script.sh',
                                    '/tmp/script.sh')
        sudo_mock.assert_has_calls(
            [call('chmod u+x /tmp/script.sh'), call('/tmp/script.sh'),
             call('rm /tmp/script.sh')], any_order=False)

    @patch('prestoadmin.file.sudo')
    @patch('prestoadmin.file.put')
    def test_script_specify_dir(self, put_mock, sudo_mock):
        file.run('/my/local/path/script.sh', '/my/remote/path')
        put_mock.assert_called_with('/my/local/path/script.sh',
                                    '/my/remote/path/script.sh')
        sudo_mock.assert_has_calls(
            [call('chmod u+x /my/remote/path/script.sh'),
             call('/my/remote/path/script.sh'),
             call('rm /my/remote/path/script.sh')], any_order=False)
