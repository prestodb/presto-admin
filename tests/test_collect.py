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
from fabric.api import env
import requests

from prestoadmin import collect
from prestoadmin.collect import TMP_PRESTO_DEBUG, \
    PRESTOADMIN_LOG_NAME, PRESTOADMIN_LOG_DIR, \
    REMOTE_PRESTO_LOG_DIR, OUTPUT_FILENAME
import prestoadmin
import utils


class TestCollect(utils.BaseTestCase):
    @patch("prestoadmin.collect.execute")
    @patch("prestoadmin.collect.tarfile.open")
    @patch("prestoadmin.collect.shutil.copy")
    @patch("prestoadmin.collect.os.mkdir")
    @patch("prestoadmin.collect.os.path.exists")
    def test_collect_logs(self, path_exists_mock, mkdirs_mock, copy_mock,
                          tarfile_open_mock, mock_execute):
        path_exists_mock.return_value = False

        collect.logs()

        mkdirs_mock.assert_called_with(TMP_PRESTO_DEBUG)
        copy_mock.assert_called_with(path.join(PRESTOADMIN_LOG_DIR,
                                               PRESTOADMIN_LOG_NAME),
                                     TMP_PRESTO_DEBUG)

        mock_execute.assert_called_with(collect.file_get,
                                        REMOTE_PRESTO_LOG_DIR,
                                        TMP_PRESTO_DEBUG,
                                        roles=[])
        tarfile_open_mock.assert_called_with(OUTPUT_FILENAME, 'w:bz2')
        tar = tarfile_open_mock.return_value
        tar.add.assert_called_with(TMP_PRESTO_DEBUG,
                                   arcname=path.basename(TMP_PRESTO_DEBUG))

    @patch("prestoadmin.collect.get")
    @patch("prestoadmin.collect.exists")
    def test_file_get(self, exists_mock, get_mock):
        remote_path = "/a/b"
        local_path = "/c/d"
        exists_mock.return_value = True

        collect.file_get(remote_path, local_path)

        exists_mock.assert_called_with(remote_path, True)
        get_mock.assert_called_with(remote_path, local_path + '%(host)s', True)

    @patch("prestoadmin.collect.warn")
    @patch("prestoadmin.collect.exists")
    def test_file_get_warning(self, exists_mock, warn_mock):
        remote_path = "/a/b"
        local_path = "/c/d"
        env.host = "remote_host"
        exists_mock.return_value = False

        collect.file_get(remote_path, local_path)

        exists_mock.assert_called_with(remote_path, True)
        warn_mock.assert_called_with("remote path " + remote_path +
                                     " not found on " + env.host)

    def test_query_info_fail_no_id(self):
        env.host = "myhost"
        env.roledefs["coordinator"] = ["myhost"]
        query_id = None
        self.assertRaisesRegexp(SystemExit, "Missing argument query_id",
                                collect.query_info, query_id)

    @patch("prestoadmin.collect.requests.get")
    def test_query_info_not_run_on_workers(self, req_get_mock):
        env.host = ["worker1"]
        env.roledefs["worker"] = ["worker1"]
        collect.query_info("any_query_id")
        assert not req_get_mock.called

    @patch("prestoadmin.collect.requests.get")
    def test_query_info_fail_invalid_id(self, req_get_mock):
        env.host = "myhost"
        env.roledefs["coordinator"] = ["myhost"]
        query_id = "invalid_id"
        req_get_mock.return_value.status_code = requests.codes.ok + 10
        self.assertRaisesRegexp(SystemExit, "Unable to retrieve information. "
                                            "Please check that the query_id "
                                            "is correct, or check that server "
                                            "is up with command server status",
                                collect.query_info, query_id)

    @patch("prestoadmin.collect.json.dumps")
    @patch("prestoadmin.collect.requests.models.json")
    @patch("__builtin__.open")
    @patch("prestoadmin.collect.os.mkdir")
    @patch("prestoadmin.collect.os.path.exists")
    @patch("prestoadmin.collect.requests.get")
    def test_collect_query_info(self, requests_get_mock,
                                path_exist_mock, mkdir_mock, open_mock,
                                req_json_mock, json_dumps_mock):
        query_id = "1234_abcd"
        query_info_file_name = path.join(TMP_PRESTO_DEBUG,
                                         "query_info_" + query_id + ".json")
        path_exist_mock.return_value = False
        file_obj = open_mock.return_value.__enter__.return_value
        requests_get_mock.return_value.json.return_value = req_json_mock
        requests_get_mock.return_value.status_code = requests.codes.ok
        env.host = "myhost"
        env.roledefs["coordinator"] = ["myhost"]

        collect.query_info(query_id)

        requests_get_mock.assert_called_with(collect.QUERY_REQUEST_URL
                                             + query_id)
        mkdir_mock.assert_called_with(TMP_PRESTO_DEBUG)

        open_mock.assert_called_with(query_info_file_name, "w")

        json_dumps_mock.assert_called_with(req_json_mock, indent=4)

        file_obj.write.assert_called_with(json_dumps_mock.return_value)

    @patch("prestoadmin.collect.requests.get")
    def test_system_info_not_run_on_workers(self, req_get_mock):
        env.host = ["worker1"]
        env.roledefs["worker"] = ["worker1"]
        collect.system_info()
        assert not req_get_mock.called

    @patch('prestoadmin.collect.run')
    @patch('prestoadmin.server.run')
    @patch("prestoadmin.collect.platform.platform")
    @patch("prestoadmin.collect.json.dumps")
    @patch("prestoadmin.collect.requests.models.json")
    @patch("__builtin__.open")
    @patch("prestoadmin.collect.os.mkdir")
    @patch("prestoadmin.collect.os.path.exists")
    @patch("prestoadmin.collect.requests.get")
    def test_collect_system_info(self, requests_get_mock,
                                 path_exist_mock, mkdir_mock, open_mock,
                                 req_json_mock, json_dumps_mock,
                                 platform_mock, run_server_mock,
                                 run_collect_mock):
        node_info_file_name = path.join(TMP_PRESTO_DEBUG, "node_info.json")
        version_info_file_name = path.join(TMP_PRESTO_DEBUG,
                                           "version_info.txt")
        path_exist_mock.return_value = False
        file_obj = open_mock.return_value.__enter__.return_value
        requests_get_mock.return_value.json.return_value = req_json_mock
        requests_get_mock.return_value.status_code = requests.codes.ok

        platform_value = platform_mock.return_value
        server_version = "dummy_verion"
        run_server_mock.return_value = server_version
        java_version = "java dummy version"
        run_collect_mock.return_value = java_version

        env.host = "myhost"
        env.roledefs["coordinator"] = ["myhost"]

        collect.system_info()

        requests_get_mock.assert_called_with(collect.NODES_REQUEST_URL)
        mkdir_mock.assert_called_with(TMP_PRESTO_DEBUG)

        open_mock.assert_any_call(node_info_file_name, "w")

        json_dumps_mock.assert_called_with(req_json_mock, indent=4)

        file_obj.write.assert_any_call(json_dumps_mock.return_value)

        open_mock.assert_called_with(version_info_file_name, "w")

        file_obj.write.assert_any_call("platform information : "
                                       + platform_value + "\n")
        file_obj.write.assert_any_call("Java version : "
                                       + java_version + "\n")
        file_obj.write.assert_any_call("presto admin version : "
                                       + prestoadmin.__version__ + "\n")
        file_obj.write.assert_any_call("presto server version : "
                                       + server_version + "\n")
