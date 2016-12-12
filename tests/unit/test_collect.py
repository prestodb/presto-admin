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
import os
from os import path

import requests
from fabric.api import env
from mock import patch

import prestoadmin
from prestoadmin import collect
from prestoadmin.collect import \
    TMP_PRESTO_DEBUG, \
    PRESTOADMIN_LOG_NAME, \
    OUTPUT_FILENAME_FOR_LOGS, \
    OUTPUT_FILENAME_FOR_SYS_INFO, \
    TMP_PRESTO_DEBUG_REMOTE
from prestoadmin.util.local_config_util import get_log_directory
from tests.unit.base_unit_case import BaseUnitCase, PRESTO_CONFIG


class TestCollect(BaseUnitCase):
    @patch('prestoadmin.collect.lookup_launcher_log_file')
    @patch('prestoadmin.collect.lookup_server_log_file')
    @patch('prestoadmin.collect.get_files')
    @patch("prestoadmin.collect.tarfile.open")
    @patch("prestoadmin.collect.shutil.copy")
    @patch("prestoadmin.collect.ensure_directory_exists")
    def test_collect_logs(self, mkdirs_mock, copy_mock,
                          tarfile_open_mock, get_files_mock, server_log_mock,
                          launcher_log_mock):
        downloaded_logs_loc = path.join(TMP_PRESTO_DEBUG, "logs")

        collect.logs()

        mkdirs_mock.assert_called_with(downloaded_logs_loc)
        copy_mock.assert_called_with(path.join(get_log_directory(),
                                               PRESTOADMIN_LOG_NAME),
                                     downloaded_logs_loc)

        tarfile_open_mock.assert_called_with(OUTPUT_FILENAME_FOR_LOGS, 'w:gz')
        tar = tarfile_open_mock.return_value
        tar.add.assert_called_with(downloaded_logs_loc,
                                   arcname=path.basename(downloaded_logs_loc))

    @patch("prestoadmin.collect.os.makedirs")
    @patch("prestoadmin.collect.get")
    def test_get_files(self, get_mock, makedirs_mock):
        remote_path = "/a/b"
        local_path = "/c/d"
        env.host = "myhost"
        path_with_host_name = path.join(local_path, env.host)

        collect.get_files(remote_path, local_path)

        makedirs_mock.assert_called_with(os.path.join(local_path, env.host))
        get_mock.assert_called_with(remote_path, path_with_host_name, use_sudo=True)

    @patch("prestoadmin.collect.os.makedirs")
    @patch("prestoadmin.collect.warn")
    @patch("prestoadmin.collect.get")
    def test_get_files_warning(self, get_mock, warn_mock, makedirs_mock):
        remote_path = "/a/b"
        local_path = "/c/d"
        env.host = "remote_host"
        get_mock.side_effect = SystemExit

        collect.get_files(remote_path, local_path)

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
    @patch("prestoadmin.collect.os.makedirs")
    @patch("prestoadmin.collect.requests.get")
    @patch('prestoadmin.collect.request_url')
    def test_collect_query_info(self, requests_url_mock, requests_get_mock,
                                mkdir_mock, open_mock,
                                req_json_mock, json_dumps_mock):
        query_id = "1234_abcd"
        query_info_file_name = path.join(TMP_PRESTO_DEBUG,
                                         "query_info_" + query_id + ".json")
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

    @patch('prestoadmin.util.presto_config.PrestoConfig.coordinator_config',
           return_value=PRESTO_CONFIG)
    @patch("prestoadmin.collect.make_tarfile")
    @patch('prestoadmin.collect.get_catalog_info_from')
    @patch("prestoadmin.collect.json.dumps")
    @patch("prestoadmin.collect.requests.models.json")
    @patch('prestoadmin.collect.execute')
    @patch("__builtin__.open")
    @patch("prestoadmin.collect.os.makedirs")
    @patch("prestoadmin.collect.requests.get")
    @patch('prestoadmin.collect.request_url')
    def test_collect_system_info(self, requests_url_mock, requests_get_mock,
                                 makedirs_mock, open_mock,
                                 execute_mock, req_json_mock,
                                 json_dumps_mock, catalog_info_mock,
                                 make_tarfile_mock, mock_presto_config):
        downloaded_sys_info_loc = path.join(TMP_PRESTO_DEBUG, "sysinfo")
        node_info_file_name = path.join(downloaded_sys_info_loc,
                                        "node_info.json")
        conn_info_file_name = path.join(downloaded_sys_info_loc,
                                        "catalog_info.txt")

        file_obj = open_mock.return_value.__enter__.return_value
        requests_get_mock.return_value.json.return_value = req_json_mock
        requests_get_mock.return_value.status_code = requests.codes.ok
        catalog_info = catalog_info_mock.return_value

        env.host = "myhost"
        env.roledefs["coordinator"] = ["myhost"]
        collect.system_info()

        makedirs_mock.assert_called_with(downloaded_sys_info_loc)
        makedirs_mock.assert_called_with(downloaded_sys_info_loc)

        open_mock.assert_any_call(node_info_file_name, "w")

        json_dumps_mock.assert_called_with(req_json_mock, indent=4)

        file_obj.write.assert_any_call(json_dumps_mock.return_value)

        open_mock.assert_any_call(conn_info_file_name, "w")

        assert catalog_info_mock.called

        file_obj.write.assert_any_call(catalog_info + '\n')

        execute_mock.assert_called_with(collect.get_system_info,
                                        downloaded_sys_info_loc, roles=[])

        make_tarfile_mock.assert_called_with(OUTPUT_FILENAME_FOR_SYS_INFO,
                                             downloaded_sys_info_loc)

    @patch("prestoadmin.collect.get_files")
    @patch("prestoadmin.collect.append")
    @patch("prestoadmin.collect.get_presto_version")
    @patch("prestoadmin.collect.get_java_version")
    @patch("prestoadmin.collect.get_platform_information")
    @patch('prestoadmin.collect.run')
    def test_get_system_info(self, run_collect_mock,
                             plat_info_mock, java_version_mock,
                             server_version_mock,
                             append_mock, get_files_mock):
        downloaded_sys_info_loc = path.join(TMP_PRESTO_DEBUG, "sysinfo")
        version_info_file_name = path.join(TMP_PRESTO_DEBUG_REMOTE,
                                           "version_info.txt")

        platform_info = "platform abcd"
        server_version = "dummy_verion"
        java_version = "java dummy version"
        plat_info_mock.return_value = platform_info
        java_version_mock.return_value = java_version
        server_version_mock.return_value = server_version

        collect.get_system_info(downloaded_sys_info_loc)

        run_collect_mock.assert_any_call('mkdir -p ' + TMP_PRESTO_DEBUG_REMOTE)

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

        get_files_mock.assert_called_with(version_info_file_name,
                                          downloaded_sys_info_loc)
