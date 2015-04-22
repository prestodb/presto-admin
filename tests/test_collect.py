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
Tests the presto diagnostic information using presto-admin collect
"""

from os import path

from mock import patch

import prestoadmin.collect as collect
import utils


class TestCollect(utils.BaseTestCase):
    @patch("prestoadmin.collect.execute")
    @patch("prestoadmin.collect.tarfile.open")
    @patch("prestoadmin.collect.shutil.copy")
    @patch("prestoadmin.collect.os.mkdir")
    def test_collect_logs(self, mkdirs_mock, copy_mock,
                          tarfile_open_mock, mock_execute):
        collect.logs()
        mkdirs_mock.assert_called_with(collect.TMP_PRESTO_DEBUG)
        copy_mock.assert_called_with(
            path.join(collect.PRESTOADMIN_LOG_DIR, collect.
                      PRESTOADMIN_LOG_NAME), collect.TMP_PRESTO_DEBUG)

        mock_execute.assert_called_with(collect.file_get,
                                        collect.REMOTE_PRESTO_LOG_DIR,
                                        collect.TMP_PRESTO_DEBUG,
                                        roles=[])
        tarfile_open_mock.assert_called_with(collect.OUTPUT_FILENAME, 'w:bz2')
        tar = tarfile_open_mock.return_value
        tar.add.assert_called_with(collect.TMP_PRESTO_DEBUG,
                                   arcname=path.basename(
                                       collect.TMP_PRESTO_DEBUG))
