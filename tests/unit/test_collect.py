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
    OUTPUT_FILENAME_FOR_LOGS, OUTPUT_FILENAME_FOR_SYS_INFO
import prestoadmin
from tests.unit.base_unit_case import BaseUnitCase


class TestCollect(BaseUnitCase):

    @patch('prestoadmin.collect.lookup_launcher_log_file')
    @patch('prestoadmin.collect.lookup_server_log_file')
    @patch('prestoadmin.collect.file_get')
    @patch("prestoadmin.collect.tarfile.open")
    @patch("prestoadmin.collect.shutil.copy")
    @patch("prestoadmin.collect.ensure_directory_exists")
    @patch("prestoadmin.collect.os.path.exists")
    def test_collect_logs(self, path_exists_mock, mkdirs_mock, copy_mock,
                          tarfile_open_mock, file_get_mock, server_log_mock,
                          launcher_log_mock):
        downloaded_logs_loc = path.join(TMP_PRESTO_DEBUG, "logs")
        path_exists_mock.return_value = False

        collect.logs()

        mkdirs_mock.assert_called_with(downloaded_logs_loc)
        copy_mock.assert_called_with(path.join(PRESTOADMIN_LOG_DIR,
                                               PRESTOADMIN_LOG_NAME),
                                     downloaded_logs_loc)

        tarfile_open_mock.assert_called_with(OUTPUT_FILENAME_FOR_LOGS, 'w:bz2')
        tar = tarfile_open_mock.return_value
        tar.add.assert_called_with(downloaded_logs_loc,
                                   arcname=path.basename(downloaded_logs_loc))

    @patch("prestoadmin.collect.os.path.exists")
    @patch("prestoadmin.collect.get")
    @patch("prestoadmin.collect.exists")
    def test_file_get(self, exists_mock, get_mock, path_exists_mock):
        remote_path = "/a/b"
        local_path = "/c/d"
        env.host = "myhost"
        path_with_host_name = path.join(local_path, env.host)
        exists_mock.return_value = True
        path_exists_mock.return_value = True

        collect.file_get(remote_path, local_path)

        exists_mock.assert_called_with(remote_path, True)
        get_mock.assert_called_with(remote_path, path_with_host_name, True)

    @patch("prestoadmin.collect.os.path.exists")
    @patch("prestoadmin.collect.warn")
    @patch("prestoadmin.collect.exists")
    def test_file_get_warning(self, exists_mock, warn_mock, path_exists_mock):
        remote_path = "/a/b"
        local_path = "/c/d"
        env.host = "remote_host"
        exists_mock.return_value = False
        path_exists_mock.return_value = True

        collect.file_get(remote_path, local_path)

        exists_mock.assert_called_with(remote_path, True)
        warn_mock.assert_called_with("remote path " + remote_path +
                                     " not found on " + env.host)

    @patch("prestoadmin.collect.requests.get")
    def test_query_info_not_run_on_workers(self, req_get_mock):
        env.host = ["worker1"]
        env.roledefs["worker"] = ["worker1"]
        collect.query_info("any_query_id")
        assert not req_get_mock.called

    @patch('prestoadmin.collect.request_url')
    @patch("prestoadmin.collect.requests.get")
    def test_query_info_fail_invalid_id(self, req_get_mock, requests_url):
        env.host = "myhost"
        env.roledefs["coordinator"] = ["myhost"]
        query_id = "invalid_id"
        req_get_mock.return_value.status_code = requests.codes.ok + 10
        self.assertRaisesRegexp(SystemExit, "Unable to retrieve information. "
                                            "Please check that the query_id "
                                            "is correct, or check that server "
                                            "is up with command: "
                                            "server status",
                                collect.query_info, query_id)

    @patch("prestoadmin.collect.json.dumps")
    @patch("prestoadmin.collect.requests.models.json")
    @patch("__builtin__.open")
    @patch("prestoadmin.collect.os.mkdir")
    @patch("prestoadmin.collect.os.path.exists")
    @patch("prestoadmin.collect.requests.get")
    @patch('prestoadmin.collect.request_url')
    def test_collect_query_info(self, requests_url_mock, requests_get_mock,
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

        mkdir_mock.assert_called_with(TMP_PRESTO_DEBUG)

        open_mock.assert_called_with(query_info_file_name, "w")

        json_dumps_mock.assert_called_with(req_json_mock, indent=4)

        file_obj.write.assert_called_with(json_dumps_mock.return_value)

    @patch("prestoadmin.collect.make_tarfile")
    @patch('prestoadmin.collect.get_connector_info_from')
    @patch("prestoadmin.collect.json.dumps")
    @patch("prestoadmin.collect.requests.models.json")
    @patch('prestoadmin.collect.execute')
    @patch("__builtin__.open")
    @patch("prestoadmin.collect.os.mkdir")
    @patch("prestoadmin.collect.os.path.exists")
    @patch("prestoadmin.collect.requests.get")
    @patch('prestoadmin.collect.request_url')
    def test_collect_system_info(self, requests_url_mock, requests_get_mock,
                                 path_exists_mock,
                                 mkdir_mock, open_mock,
                                 execute_mock, req_json_mock,
                                 json_dumps_mock, conn_info_mock,
                                 make_tarfile_mock):
        downloaded_sys_info_loc = path.join(TMP_PRESTO_DEBUG, "sysinfo")
        node_info_file_name = path.join(downloaded_sys_info_loc,
                                        "node_info.json")
        conn_info_file_name = path.join(downloaded_sys_info_loc,
                                        "connector_info.txt")
        path_exists_mock.return_value = False

        file_obj = open_mock.return_value.__enter__.return_value
        requests_get_mock.return_value.json.return_value = req_json_mock
        requests_get_mock.return_value.status_code = requests.codes.ok
        connector_info = conn_info_mock.return_value

        env.host = "myhost"
        env.roledefs["coordinator"] = ["myhost"]
        collect.system_info()
        mkdir_mock.assert_any_call(TMP_PRESTO_DEBUG)

        mkdir_mock.assert_called_with(downloaded_sys_info_loc)

        open_mock.assert_any_call(node_info_file_name, "w")

        json_dumps_mock.assert_called_with(req_json_mock, indent=4)

        file_obj.write.assert_any_call(json_dumps_mock.return_value)

        open_mock.assert_any_call(conn_info_file_name, "w")

        assert conn_info_mock.called

        file_obj.write.assert_any_call(connector_info + '\n')

        execute_mock.assert_called_with(collect.get_system_info,
                                        downloaded_sys_info_loc, roles=[])

        make_tarfile_mock.assert_called_with(OUTPUT_FILENAME_FOR_SYS_INFO,
                                             downloaded_sys_info_loc)

    @patch("prestoadmin.collect.file_get")
    @patch("prestoadmin.collect.append")
    @patch("prestoadmin.collect.get_presto_version")
    @patch("prestoadmin.collect.get_java_version")
    @patch("prestoadmin.collect.get_platform_information")
    @patch('prestoadmin.collect.run')
    @patch("prestoadmin.collect.exists")
    def test_get_system_info(self, exists_mock, run_collect_mock,
                             plat_info_mock, java_version_mock,
                             server_version_mock,
                             append_mock, file_get_mock):
        downloaded_sys_info_loc = path.join(TMP_PRESTO_DEBUG, "sysinfo")
        version_info_file_name = path.join(TMP_PRESTO_DEBUG,
                                           "version_info.txt")

        platform_info = "platform abcd"
        server_version = "dummy_verion"
        java_version = "java dummy version"
        exists_mock.return_value = False
        plat_info_mock.return_value = platform_info
        java_version_mock.return_value = java_version
        server_version_mock.return_value = server_version

        collect.get_system_info(downloaded_sys_info_loc)

        exists_mock.assert_any_call(TMP_PRESTO_DEBUG)
        run_collect_mock.assert_any_call('mkdir ' + TMP_PRESTO_DEBUG)

        exists_mock.assert_any_call(version_info_file_name)

        append_mock.assert_any_call(version_info_file_name,
                                    'platform information : ' +
                                    platform_info + '\n')

        append_mock.assert_any_call(version_info_file_name,
                                    'Java version: ' +
                                    java_version + '\n')

        append_mock.assert_any_call(version_info_file_name,
                                    'Presto-admin version: ' +
                                    prestoadmin.__version__ + '\n')

        append_mock.assert_any_call(version_info_file_name,
                                    'Presto server version: ' +
                                    server_version + '\n')

        file_get_mock.assert_called_with(version_info_file_name,
                                         downloaded_sys_info_loc)
