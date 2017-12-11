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
Product tests for start/stop/restart of presto-admin server
"""
from nose.plugins.attrib import attr

from prestoadmin.server import RETRY_TIMEOUT
from prestoadmin.util import constants
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_PRESTO_CLUSTER, STANDALONE_PA_CLUSTER
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestControl(BaseProductTestCase):
    def setUp(self):
        super(TestControl, self).setUp()

    @attr('smoketest')
    def test_server_start_stop_simple(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.assert_simple_start_stop(self.expected_start(),
                                      self.expected_stop())

    @attr('smoketest')
    def test_server_restart_simple(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        expected_output = self.expected_stop()[:] + self.expected_start()[:]
        self.assert_simple_server_restart(expected_output)

    def test_server_start_without_presto(self):
        self.assert_service_fails_without_presto('start')

    def test_server_stop_without_presto(self):
        self.assert_service_fails_without_presto('stop')

    def test_server_restart_without_presto(self):
        self.assert_service_fails_without_presto('restart')

    def assert_service_fails_without_presto(self, service):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)
        self.upload_topology()
        # Start without Presto installed
        start_output = self.run_prestoadmin('server %s' % service,
                                            raise_error=False).splitlines()
        presto_not_installed = self.presto_not_installed_message()
        self.assertEqualIgnoringOrder(presto_not_installed,
                                      '\n'.join(start_output))

    def test_server_start_one_host_started(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.assert_start_with_one_host_started(
            self.cluster.internal_slaves[0])

    def test_server_stop_one_host_started(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.assert_one_host_stopped(self.cluster.internal_master)

    def test_server_restart_nothing_started(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)

        # Restart when the servers aren't started
        expected_output = self.expected_stop(
            not_running=self.cluster.all_internal_hosts())[:] +\
            self.expected_start()[:]
        self.assert_simple_server_restart(expected_output, running_host='')

    def test_start_coordinator_down(self):
        installer = StandalonePrestoInstaller(self)
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        installer.install(coordinator='slave1')
        self.assert_start_coordinator_down(
            self.cluster.slaves[0],
            self.cluster.internal_slaves[0])

    def test_start_worker_down(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.assert_start_worker_down(
            self.cluster.slaves[0],
            self.cluster.internal_slaves[0])

    def assert_start_coordinator_down(self, coordinator, coordinator_internal):
        self.cluster.stop_host(coordinator)
        alive_hosts = self.cluster.all_internal_hosts()[:]
        alive_hosts.remove(self.cluster.get_down_hostname(coordinator_internal))

        # test server start
        start_output = self.run_prestoadmin('server start', raise_error=False)

        # when the coordinator is down, you can't confirm that the server is started
        # on any of the nodes
        expected_start = self.expected_start(failed_hosts=alive_hosts)
        for host in alive_hosts:
            expected_start.append(self.expected_no_status_message(host))
        expected_start.append(self.down_node_connection_error(coordinator_internal))
        for message in expected_start:
            self.assertRegexpMatches(start_output, message, 'expected %s \n '
                                                            'actual %s' % (message, start_output))

        process_per_host = self.get_process_per_host(start_output.splitlines())
        self.assert_started(process_per_host)

    def assert_start_worker_down(self, down_node, down_internal_node):
        self.cluster.stop_host(down_node)
        alive_hosts = self.cluster.all_internal_hosts()[:]
        alive_hosts.remove(self.cluster.get_down_hostname(down_internal_node))

        # test server start
        start_output = self.run_prestoadmin('server start', raise_error=False)

        self.assertRegexpMatches(
            start_output,
            self.down_node_connection_error(down_internal_node)
        )

        expected_start = self.expected_start(start_success=alive_hosts)
        for message in expected_start:
            self.assertRegexpMatches(start_output, message, 'expected %s \n '
                                     'actual %s' % (message, start_output))

        process_per_host = self.get_process_per_host(start_output.splitlines())
        self.assert_started(process_per_host)

    def expected_down_node_output_size(self, expected_output):
        return self.len_down_node_error + len(
            '\n'.join(expected_output).splitlines())

    def assert_simple_start_stop(self, expected_start, expected_stop,
                                 pa_raise_error=True):
        cmd_output = self.run_prestoadmin(
            'server start', raise_error=pa_raise_error)
        cmd_output = cmd_output.splitlines()
        self.assertRegexpMatchesLineByLine(cmd_output, expected_start)
        process_per_host = self.get_process_per_host(cmd_output)
        self.assert_started(process_per_host)
        cmd_output = self.run_prestoadmin('server stop').splitlines()
        self.assertRegexpMatchesLineByLine(cmd_output, expected_stop)
        self.assert_stopped(process_per_host)

    def assert_simple_server_restart(self, expected_output, running_host='all',
                                     pa_raise_error=True):
        if running_host is 'all':
            start_output = self.run_prestoadmin(
                'server start', raise_error=pa_raise_error)
        elif running_host:
            start_output = self.run_prestoadmin('server start -H %s'
                                                % running_host, raise_error=pa_raise_error)
        else:
            start_output = ''

        start_output = start_output.splitlines()

        restart_output = self.run_prestoadmin(
            'server restart', raise_error=pa_raise_error).splitlines()
        self.assertRegexpMatchesLineByLine(restart_output, expected_output)

        if start_output:
            process_per_host = self.get_process_per_host(start_output)
            self.assert_stopped(process_per_host)

        process_per_host = self.get_process_per_host(restart_output)
        self.assert_started(process_per_host)

    def assert_start_with_one_host_started(self, host):
        start_output = self.run_prestoadmin('server start -H %s' % host).splitlines()
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)

        start_output = self.run_prestoadmin(
            'server start', raise_error=False).splitlines()
        started_hosts = self.cluster.all_internal_hosts()
        started_hosts.remove(host)
        started_expected = self.expected_start(start_success=started_hosts)
        started_expected.extend(self.expected_port_error([host]))
        self.assertRegexpMatchesLineByLine(
            start_output,
            started_expected
        )
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)

    def assert_one_host_stopped(self, host):
        start_output = self.run_prestoadmin('server start -H %s' % host) \
            .splitlines()
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)
        stop_output = self.run_prestoadmin('server stop').splitlines()
        not_started_hosts = self.cluster.all_internal_hosts()
        not_started_hosts.remove(host)
        self.assertRegexpMatchesLineByLine(
            stop_output,
            self.expected_stop(not_running=not_started_hosts)
        )
        process_per_host = self.get_process_per_host(start_output)
        self.assert_stopped(process_per_host)

    def expected_port_error(self, hosts=None):
        return_str = []
        for host in hosts:
            return_str += [r'Fatal error: \[%s\] Server failed to start on %s.'
                           r' Port 7070 already in use' % (host, host), r'',
                           r'', r'Aborting.']
        return return_str

    def expected_no_status_message(self, host=None):
        return ('Could not verify server status for: %s\n'
                'This could mean that the server failed to start or that there was no coordinator or worker up.'
                ' Please check ' + constants.DEFAULT_PRESTO_SERVER_LOG_FILE + ' and ' +
                constants.DEFAULT_PRESTO_LAUNCHER_LOG_FILE) % host

    def expected_start(self, start_success=None, already_started=None,
                       failed_hosts=None):
        return_str = []

        # With no args, return message that all started successfully
        if not already_started and not start_success and not failed_hosts:
            start_success = self.cluster.all_internal_hosts()

        if start_success:
            for host in start_success:
                return_str += [r'Waiting to make sure we can connect to the '
                               r'Presto server on %s, please wait. This check'
                               r' will time out after %d minutes if the server'
                               r' does not respond.'
                               % (host, RETRY_TIMEOUT / 60),
                               r'Server started successfully on: %s' % host,
                               r'\[%s\] out: ' % host,
                               r'\[%s\] out: Started as .*' % host,
                               r'\[%s\] out: Starting presto' % host]
        if already_started:
            for host in already_started:
                return_str += [r'Waiting to make sure we can connect to the '
                               r'Presto server on %s, please wait. This check'
                               r' will time out after %d minutes if the server'
                               r' does not respond.'
                               % (host, RETRY_TIMEOUT / 60),
                               r'Server started successfully on: %s' % host,
                               r'\[%s\] out: ' % host,
                               r'\[%s\] out: Already running as .*' % host,
                               r'\[%s\] out: Starting presto' % host]
        if failed_hosts:
            for host in failed_hosts:
                return_str += [r'\[%s\] out: ' % host,
                               r'\[%s\] out: Starting presto' % host]
        return return_str

    def presto_not_installed_message(self):
        return ('Warning: [slave2] Presto is not installed.\n\n\n'
                'Warning: [slave3] Presto is not installed.\n\n\n'
                'Warning: [slave1] Presto is not installed.\n\n\n'
                'Warning: [master] Presto is not installed.\n\n')
