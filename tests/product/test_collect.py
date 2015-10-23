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
from tests.product.base_product_case import BaseProductTestCase, PrestoError


class TestCollect(BaseProductTestCase):

    def setUp(self):
        super(TestCollect, self).setUp()
        self.setup_cluster(self.STANDALONE_PRESTO_CLUSTER)

    @attr('smoketest')
    def test_collect_logs_basic(self):
        self.run_prestoadmin('server start')
        actual = self.run_prestoadmin('collect logs')
        expected = 'Downloading logs from all the nodes...\n' + \
                   'logs archive created: ' + OUTPUT_FILENAME_FOR_LOGS + '\n'
        self.assertEqual(expected, actual)
        self.assert_path_exists(self.cluster.master,
                                OUTPUT_FILENAME_FOR_LOGS)
        self.assert_path_exists(self.cluster.master,
                                TMP_PRESTO_DEBUG)

        downloaded_logs_location = path.join(TMP_PRESTO_DEBUG, 'logs')
        self.assert_path_exists(self.cluster.master,
                                downloaded_logs_location)

        for host in self.cluster.all_internal_hosts():
            host_log_location = path.join(downloaded_logs_location,
                                          host)
            self.assert_path_exists(self.cluster.master,
                                    host_log_location)

        admin_log = path.join(downloaded_logs_location, PRESTOADMIN_LOG_NAME)
        self.assert_path_exists(self.cluster.master, admin_log)

    @attr('smoketest')
    def test_collect_system_info_basic(self):
        self.run_prestoadmin('server start')
        actual = self.run_prestoadmin('collect system_info')
        expected = 'System info archive created: ' + \
                   OUTPUT_FILENAME_FOR_SYS_INFO + '\n'

        self.assertEqual(expected, actual)
        self.assert_path_exists(self.cluster.master,
                                OUTPUT_FILENAME_FOR_SYS_INFO)
        self.assert_path_exists(self.cluster.master,
                                TMP_PRESTO_DEBUG)

        downloaded_sys_info_loc = path.join(TMP_PRESTO_DEBUG, 'sysinfo')
        self.assert_path_exists(self.cluster.master,
                                downloaded_sys_info_loc)

        master_system_info_location = path.join(
            downloaded_sys_info_loc,
            self.cluster.internal_master)
        self.assert_path_exists(self.cluster.master,
                                master_system_info_location)

        conn_file_name = path.join(downloaded_sys_info_loc,
                                   'connector_info.txt')
        self.assert_path_exists(self.cluster.master,
                                conn_file_name)

        version_file_name = path.join(TMP_PRESTO_DEBUG, 'version_info.txt')

        for host in self.cluster.all_hosts():
            self.assert_path_exists(host, version_file_name)

        slave0_system_info_loc = path.join(
            downloaded_sys_info_loc,
            self.cluster.internal_slaves[0])
        self.assert_path_exists(self.cluster.master,
                                slave0_system_info_loc)

        self.assert_path_exists(self.cluster.master,
                                OUTPUT_FILENAME_FOR_SYS_INFO)

    @attr('smoketest')
    def test_collect_query_info(self):
        self.run_prestoadmin('server start')
        sql_to_run = 'SELECT * FROM system.runtime.nodes WHERE 1234 = 1234'
        query_id = self.retry(lambda: self.get_query_id(sql_to_run))

        actual = self.run_prestoadmin('collect query_info ' + query_id)
        query_info_file_name = path.join(TMP_PRESTO_DEBUG,
                                         'query_info_' + query_id
                                         + '.json')

        expected = 'Gathered query information in file: ' + \
                   query_info_file_name + '\n'

        self.assert_path_exists(self.cluster.master,
                                query_info_file_name)
        self.assertEqual(actual, expected)

    def get_query_id(self, sql):
        ips = self.cluster.get_ip_address_dict()
        client = PrestoClient(ips[self.cluster.master],
                              'root', 8080)
        run_sql(client, sql)
        query_runtime_info = run_sql(client, 'SELECT query_id FROM '
                                     'system.runtime.queries '
                                     'WHERE query = \'' + sql + '\'')
        if not query_runtime_info:
            raise PrestoError('Presto not started up yet.')
        for row in query_runtime_info:
            return row[0]

    def test_query_info_invalid_id(self):
        self.run_prestoadmin('server start')
        invalid_id = '1234_invalid'
        actual = self.run_prestoadmin('collect query_info ' + invalid_id)
        expected = '\nFatal error: [master] Unable to retrieve information. ' \
                   'Please check that the query_id is correct, or check ' \
                   'that server is up with command: server status\n\n' \
                   'Aborting.\n'
        self.assertEqual(actual, expected)

    def test_collect_logs_server_stopped(self):
        actual = self.run_prestoadmin('collect logs')
        expected = 'Downloading logs from all the nodes...\n' + \
                   'logs archive created: ' + OUTPUT_FILENAME_FOR_LOGS + '\n'
        self.assertEqual(actual, expected)
        self.assert_path_exists(self.cluster.master,
                                OUTPUT_FILENAME_FOR_LOGS)

    def test_collect_system_info_server_stopped(self):
        actual = self.run_prestoadmin('collect system_info', raise_error=False)
        message = '\nFatal error: [%s] Unable to access node ' \
            'information. Please check that server is up with ' \
            'command: server status\n\nAborting.\n'
        expected = ''
        for host in self.cluster.all_internal_hosts():
            expected += message % host
        self.assertEqualIgnoringOrder(actual, expected)
