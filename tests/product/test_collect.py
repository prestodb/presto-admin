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
Product tests for presto-admin collect
"""

from os import path

from nose.plugins.attrib import attr

from prestoadmin.collect import OUTPUT_FILENAME_FOR_LOGS, TMP_PRESTO_DEBUG, \
    PRESTOADMIN_LOG_NAME, OUTPUT_FILENAME_FOR_SYS_INFO
from prestoadmin.prestoclient import PrestoClient
from prestoadmin.server import run_sql
from tests.product.base_product_case import BaseProductTestCase


class TestCollect(BaseProductTestCase):
    def do_basic_presto_setup(self):
        self.install_default_presto()
        self.run_prestoadmin('server start')

    @attr('smoketest')
    def test_collect_logs_basic(self):
        self.do_basic_presto_setup()
        actual = self.run_prestoadmin('collect logs')
        expected = 'Downloading logs from all the nodes...\n' + \
                   'logs archive created: ' + OUTPUT_FILENAME_FOR_LOGS + '\n'
        self.assertEqual(expected, actual)
        self.assert_path_exists(self.master, OUTPUT_FILENAME_FOR_LOGS)
        self.assert_path_exists(self.master, TMP_PRESTO_DEBUG)

        downloaded_logs_location = path.join(TMP_PRESTO_DEBUG, "logs")
        self.assert_path_exists(self.master, downloaded_logs_location)

        for host in self.all_hosts():
            host_log_location = path.join(downloaded_logs_location, host)
            self.assert_path_exists(self.master, host_log_location)

        admin_log = path.join(downloaded_logs_location, PRESTOADMIN_LOG_NAME)
        self.assert_path_exists(self.master, admin_log)

    @attr('smoketest')
    def test_collect_system_info_basic(self):
        self.do_basic_presto_setup()
        actual = self.run_prestoadmin('collect system_info')
        expected = 'System info archive created: ' + \
                   OUTPUT_FILENAME_FOR_SYS_INFO + '\n' \
                   + 'Disconnecting from master... done.\n'

        self.assertEqual(expected, actual)
        self.assert_path_exists(self.master, OUTPUT_FILENAME_FOR_SYS_INFO)
        self.assert_path_exists(self.master, TMP_PRESTO_DEBUG)

        downloaded_sys_info_loc = path.join(TMP_PRESTO_DEBUG, "sysinfo")
        self.assert_path_exists(self.master, downloaded_sys_info_loc)

        master_system_info_location = path.join(downloaded_sys_info_loc,
                                                self.master)
        self.assert_path_exists(self.master, master_system_info_location)

        conn_file_name = path.join(downloaded_sys_info_loc,
                                   'connector_info.txt')
        self.assert_path_exists(self.master, conn_file_name)

        version_file_name = path.join(TMP_PRESTO_DEBUG, 'version_info.txt')

        for host in self.all_hosts():
            self.assert_path_exists(host, version_file_name)

        slave0_system_info_loc = path.join(downloaded_sys_info_loc,
                                           self.slaves[0])
        self.assert_path_exists(self.master, slave0_system_info_loc)

        self.assert_path_exists(self.master, OUTPUT_FILENAME_FOR_SYS_INFO)

    @attr('smoketest')
    def test_collect_query_info(self):
        self.do_basic_presto_setup()

        sql_to_run = 'SELECT * FROM system.runtime.nodes WHERE 1234 = 1234'
        query_id = self.get_query_id(sql_to_run)

        actual = self.run_prestoadmin('collect query_info ' + query_id)
        query_info_file_name = path.join(TMP_PRESTO_DEBUG,
                                         'query_info_' + query_id
                                         + '.json')

        expected = 'Gathered query information in file: ' + \
                   query_info_file_name + '\n'

        self.assert_path_exists(self.master, query_info_file_name)
        self.assertEqual(actual, expected)

    def get_query_id(self, sql):
        ips = self.get_ip_address_dict()
        client = PrestoClient(ips[self.master], 'root', 8080)
        run_sql(client, sql)
        query_runtime_info = run_sql(client, 'SELECT query_id FROM '
                                     'system.runtime.queries '
                                     'WHERE query = \'' + sql + '\'')
        for row in query_runtime_info:
            return row[0]

    def test_query_info_invalid_id(self):
        self.do_basic_presto_setup()
        invalid_id = '1234_invalid'
        actual = self.run_prestoadmin('collect query_info ' + invalid_id)
        expected = '\nFatal error: Unable to retrieve information. Please ' \
                   'check that the query_id is correct, or check that ' \
                   'server is up with command: server status\n\nAborting.' \
                   '\n\nWarning: One or more hosts failed while executing ' \
                   'task.\n\n'
        self.assertEqual(actual, expected)

    def test_collect_logs_server_stopped(self):
        self.install_default_presto()

        actual = self.run_prestoadmin('collect logs')
        expected = 'Downloading logs from all the nodes...\n' + \
                   'logs archive created: ' + OUTPUT_FILENAME_FOR_LOGS + '\n'
        self.assertEqual(actual, expected)
        self.assert_path_exists(self.master, OUTPUT_FILENAME_FOR_LOGS)

    def test_collect_system_info_server_stopped(self):
        self.install_default_presto()

        actual = self.run_prestoadmin('collect system_info', raise_error=False)
        expected = '\nFatal error: Unable to access node information. ' \
                   'Please check that server is up with ' \
                   'command: server status\n\nAborting.\n'
        self.assertEqual(actual, expected)
