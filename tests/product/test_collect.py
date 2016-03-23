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
import os
from os import path

from nose.plugins.attrib import attr
from nose.tools import nottest

from prestoadmin.collect import OUTPUT_FILENAME_FOR_LOGS, TMP_PRESTO_DEBUG, \
    PRESTOADMIN_LOG_NAME, OUTPUT_FILENAME_FOR_SYS_INFO
from prestoadmin.prestoclient import PrestoClient
from prestoadmin.server import run_sql
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, PrestoError
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestCollect(BaseProductTestCase):
    def setUp(self):
        super(TestCollect, self).setUp()

    @attr('smoketest')
    def test_collect_logs_basic(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        actual = self.run_prestoadmin('collect logs')

        expected = 'Downloading logs from all the nodes...\n' + \
                   'logs archive created: ' + OUTPUT_FILENAME_FOR_LOGS + '\n'
        self.assertLazyMessage(lambda: self.log_msg(actual, expected),
                               self.assertEqual, actual, expected)
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

    def log_msg(self, actual, expected):
        msg = '%s != %s' % (actual, expected)
        return msg

    @attr('smoketest')
    def test_collect_system_info_basic(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        actual = self.run_prestoadmin('collect system_info')
        self.test_basic_system_info(actual)

    @nottest
    def test_basic_system_info(self, actual, coordinator=None, hosts=None):
        if not coordinator:
            coordinator = self.cluster.internal_master
        if not hosts:
            hosts = self.cluster.all_hosts()
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
        coord_system_info_location = path.join(
            downloaded_sys_info_loc,
            coordinator)
        self.assert_path_exists(self.cluster.master,
                                coord_system_info_location)
        conn_file_name = path.join(downloaded_sys_info_loc,
                                   'connector_info.txt')
        self.assert_path_exists(self.cluster.master,
                                conn_file_name)
        version_file_name = path.join(TMP_PRESTO_DEBUG, 'version_info.txt')
        for host in hosts:
            self.assert_path_exists(host, version_file_name)
        slave0_system_info_loc = path.join(
            downloaded_sys_info_loc,
            self.cluster.internal_slaves[0])
        self.assert_path_exists(self.cluster.master,
                                slave0_system_info_loc)
        self.assert_path_exists(self.cluster.master,
                                OUTPUT_FILENAME_FOR_SYS_INFO)

    def test_collect_system_info_dash_h_coord_worker(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        actual = self.run_prestoadmin('collect system_info '
                                      '-H %(master)s,%(slave1)s')
        self.test_basic_system_info(actual,
                                    self.cluster.internal_master,
                                    [self.cluster.master,
                                     self.cluster.slaves[0]])

    def test_collect_system_info_dash_x_two_workers(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        actual = self.run_prestoadmin('collect system_info '
                                      '-x %(slave2)s,%(slave3)s')
        self.test_basic_system_info(actual,
                                    self.cluster.internal_master,
                                    [self.cluster.master,
                                     self.cluster.slaves[0]])

    def test_system_info_pa_separate_node(self):
        installer = StandalonePrestoInstaller(self)
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)
        topology = {"coordinator": "slave1",
                    "workers": ["slave2", "slave3"]}
        self.upload_topology(topology=topology)
        installer.install(coordinator='slave1')
        self.run_prestoadmin('server start')
        actual = self.run_prestoadmin('collect system_info')
        self.test_basic_system_info(
            actual,
            coordinator=self.cluster.internal_slaves[0],
            hosts=self.cluster.slaves)

    @attr('smoketest')
    def test_collect_query_info(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        sql_to_run = 'SELECT * FROM system.runtime.nodes WHERE 1234 = 1234'
        query_id = self.retry(lambda: self.get_query_id(sql_to_run))

        actual = self.run_prestoadmin('collect query_info ' + query_id)
        query_info_file_name = path.join(TMP_PRESTO_DEBUG,
                                         'query_info_' + query_id +
                                         '.json')

        expected = 'Gathered query information in file: ' + \
                   query_info_file_name + '\n'

        self.assert_path_exists(self.cluster.master,
                                query_info_file_name)
        self.assertEqual(actual, expected)

    def test_query_info_pa_separate_node(self):
        installer = StandalonePrestoInstaller(self)
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)
        topology = {"coordinator": "slave1",
                    "workers": ["slave2", "slave3"]}
        self.upload_topology(topology=topology)
        installer.install(coordinator='slave1')
        self.run_prestoadmin('server start')
        sql_to_run = 'SELECT * FROM system.runtime.nodes WHERE 1234 = 1234'
        query_id = self.retry(
            lambda: self.get_query_id(sql_to_run, host=self.cluster.slaves[0]))

        actual = self.run_prestoadmin('collect query_info ' + query_id)
        query_info_file_name = path.join(TMP_PRESTO_DEBUG,
                                         'query_info_' + query_id +
                                         '.json')

        expected = 'Gathered query information in file: ' + \
                   query_info_file_name + '\n'

        self.assert_path_exists(self.cluster.master,
                                query_info_file_name)
        self.assertEqual(actual, expected)

    def get_query_id(self, sql, host=None):
        ips = self.cluster.get_ip_address_dict()
        if host is None:
            host = self.cluster.master
        client = PrestoClient(ips[host],
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
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        invalid_id = '1234_invalid'
        actual = self.run_prestoadmin('collect query_info ' + invalid_id,
                                      raise_error=False)
        expected = '\nFatal error: [master] Unable to retrieve information. ' \
                   'Please check that the query_id is correct, or check ' \
                   'that server is up with command: server status\n\n' \
                   'Aborting.\n'
        self.assertEqual(actual, expected)

    def test_collect_logs_server_stopped(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self._assert_no_logs_downloaded()

    def test_collect_system_info_server_stopped(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        actual = self.run_prestoadmin('collect system_info', raise_error=False)
        message = '\nFatal error: [%s] Unable to access node ' \
                  'information. Please check that server is up with ' \
                  'command: server status\n\nAborting.\n'
        expected = message % self.cluster.internal_master
        self.assertEqualIgnoringOrder(actual, expected)

    def _add_custom_log_location(self, new_log_location):
        for host in self.cluster.all_hosts():
            self.run_script_from_prestoadmin_dir('rm -rf /var/log/presto',
                                                 host)
            self.run_script_from_prestoadmin_dir(
                'mkdir %s; chown -R presto:presto %s'
                % (new_log_location, new_log_location),
                host)
            config_script = 'echo "node.server-log-file=%s/server.log\n' \
                            'node.launcher-log-file=%s/launcher.log" >> ' \
                            '/etc/presto/node.properties' \
                            % (new_log_location, new_log_location)
            self.run_script_from_prestoadmin_dir(config_script, host=host)

    def _collect_logs_and_unzip(self):
        self.run_prestoadmin('collect logs')
        self.assert_path_exists(self.cluster.master,
                                OUTPUT_FILENAME_FOR_LOGS)
        log_filename = path.basename(OUTPUT_FILENAME_FOR_LOGS)
        self.run_script_from_prestoadmin_dir(
            'cp %s .; tar xvf %s' % (OUTPUT_FILENAME_FOR_LOGS, log_filename))

    def test_collect_logs_nonstandard_location(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        version = self.cluster.exec_cmd_on_host(
            self.cluster.master,
            'rpm -q --qf \"%{VERSION}\\n\" presto-server-rpm'
        )
        if '127t' not in version:
            print 'test_collect_logs_nonstandard_location only valid for 127t'
            return
        new_log_location = '/var/presto'
        self._add_custom_log_location(new_log_location)

        self.run_prestoadmin('server start')
        self._collect_logs_and_unzip()

        self.assert_path_exists(self.cluster.master,
                                '/opt/prestoadmin/logs/presto-admin.log')
        for host in self.cluster.all_internal_hosts():
            self.assert_path_exists(
                self.cluster.master,
                '/opt/prestoadmin/logs/%s/server.log' % host
            )
            self.assert_path_exists(
                self.cluster.master,
                '/opt/prestoadmin/logs/%s/launcher.log' % host
            )

    def _assert_no_logs_downloaded(self):
        self._collect_logs_and_unzip()
        self.assert_path_exists(self.cluster.master,
                                '/opt/prestoadmin/logs/presto-admin.log')
        for host in self.cluster.all_internal_hosts():
            self.assert_path_exists(self.cluster.master,
                                    '/opt/prestoadmin/logs/%s' % host)
            self.assert_path_removed(self.cluster.master,
                                     '/opt/prestoadmin/logs/%s/*' % host)

    def test_collect_logs_server_not_installed(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.PA_ONLY_CLUSTER)
        self.upload_topology()
        self._assert_no_logs_downloaded()

    def test_collect_logs_multiple_server_logs(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        self.cluster.write_content_to_host('Stuff that I logged!',
                                           'var/log/presto/server.log-2',
                                           self.cluster.master)
        actual = self.run_prestoadmin('collect logs')

        expected = 'Downloading logs from all the nodes...\n' + \
                   'logs archive created: ' + OUTPUT_FILENAME_FOR_LOGS + '\n'
        self.assertLazyMessage(lambda: self.log_msg(actual, expected),
                               self.assertEqual, actual, expected)

        downloaded_logs_location = path.join(TMP_PRESTO_DEBUG, 'logs')
        self.assert_path_exists(self.cluster.master,
                                downloaded_logs_location)

        for host in self.cluster.all_internal_hosts():
            host_log_location = path.join(downloaded_logs_location,
                                          host)
            self.assert_path_exists(self.cluster.master,
                                    os.path.join(host_log_location,
                                                 'server.log'))

        master_path = os.path.join(downloaded_logs_location,
                                   self.cluster.internal_master,)
        self.assert_path_exists(self.cluster.master,
                                os.path.join(master_path, 'server.log-2'))
