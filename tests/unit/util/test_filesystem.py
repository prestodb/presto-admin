# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import errno
from mock import patch
from prestoadmin.util import filesystem
from tests.base_test_case import BaseTestCase


class TestFilesystem(BaseTestCase):
    @patch('prestoadmin.util.filesystem.os.fdopen')
    @patch('prestoadmin.util.filesystem.os.open')
    @patch('prestoadmin.util.filesystem.os.makedirs')
    def test_write_file_exits(self, makedirs_mock, open_mock, fdopen_mock):
        makedirs_mock.side_effect = OSError(errno.EEXIST, 'message')
        open_mock.side_effect = OSError(errno.EEXIST, 'message')
        filesystem.write_to_file_if_not_exists('content', 'path/to/anyfile')
        self.assertFalse(fdopen_mock.called)

    @patch('prestoadmin.util.filesystem.os.makedirs')
    def test_write_file_error_in_dirs(self, makedirs_mock):
        makedirs_mock.side_effect = OSError(errno.EACCES, 'message')
        self.assertRaisesRegexp(OSError, 'message',
                                filesystem.write_to_file_if_not_exists,
                                'content', 'path/to/anyfile')

    @patch('prestoadmin.util.filesystem.os.makedirs')
    @patch('prestoadmin.util.filesystem.os.open')
    def test_write_file_error_in_files(self, open_mock, makedirs_mock):
        open_mock.side_effect = OSError(errno.EACCES, 'message')
        self.assertRaisesRegexp(OSError, 'message',
                                filesystem.write_to_file_if_not_exists,
                                'content', 'path/to/anyfile')
