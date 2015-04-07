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
Tests the presto server service control
"""
from mock import patch

from prestoadmin import service
from prestoadmin.service import INIT_SCRIPTS
import utils


class TestServer(utils.BaseTestCase):
    @patch('prestoadmin.service.sudo')
    def test_control_command_is_called(self, mock_sudo):
        service.start()
        mock_sudo.assert_called_with(INIT_SCRIPTS + ' start', pty=False)
