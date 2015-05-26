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

from tests.product.base_product_case import BaseProductTestCase


class TestControl(BaseProductTestCase):

    def test_server_start_stop_simple(self):
        self.install_default_presto()
        self.assert_simple_start_stop(self.expected_start(),
                                      self.expected_stop())

    def test_server_restart_simple(self):
        self.install_default_presto()
        expected_output = list(
            set(self.expected_stop()[:] + self.expected_start()[:]))
        self.assert_simple_server_restart(expected_output)

    def test_server_start_without_topology(self):
        self.assert_service_fails_without_topology('start')

    def test_server_stop_without_topology(self):
        self.assert_service_fails_without_topology('stop')

    def test_server_restart_without_topology(self):
        self.assert_service_fails_without_topology('restart')

    def assert_service_fails_without_topology(self, service):
        self.install_presto_admin()
        # Start without topology added
        cmd_output = self.run_prestoadmin('server %s' % service,
                                          raise_error=False).splitlines()
        self.assertEqual(['Missing topology configuration in /etc/opt/'
                          'prestoadmin/config.json.  More detailed information'
                          ' can be found in /var/log/prestoadmin/'
                          'presto-admin.log'], cmd_output)

    def test_server_start_without_presto(self):
        self.assert_service_fails_without_presto('start')

    def test_server_stop_without_presto(self):
        self.assert_service_fails_without_presto('stop')

    def test_server_restart_without_presto(self):
        self.assert_service_fails_without_presto('restart')

    def assert_service_fails_without_presto(self, service):
        self.install_presto_admin()
        self.upload_topology()
        # Start without Presto installed
        start_output = self.run_prestoadmin('server %s' % service,
                                            raise_error=False).splitlines()
        presto_not_installed = self.presto_not_installed_message()
        self.assertEqualIgnoringOrder(presto_not_installed,
                                      '\n'.join(start_output))

    def test_server_start_various_states(self):
        self.install_default_presto()

        # Coordinator started, workers not; then server start
        process_per_host = \
            self.assert_start_with_one_host_started(self.master)

        # Worker started, coord and other workers not; then server start
        self.run_prestoadmin('server stop').splitlines()
        self.assert_stopped(process_per_host)
        self.assert_start_with_one_host_started(self.slaves[0])

        # All started; then server start
        start_output = self.run_prestoadmin('server start').splitlines()
        self.assertRegexpMatchesLineByLine(
            start_output,
            self.expected_start(already_started=self.all_hosts())
        )
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)

    def test_server_stop_various_states(self):
        self.install_default_presto()

        # Stop with servers not started
        stop_output = self.run_prestoadmin('server stop').splitlines()
        not_started_hosts = self.all_hosts()
        self.assertRegexpMatchesLineByLine(
            stop_output,
            self.expected_stop(not_running=not_started_hosts)
        )

        # Stop with coordinator started, but not workers
        self.assert_one_host_stopped(self.master)

        # Stop with worker started, but nothing else
        self.assert_one_host_stopped(self.slaves[0])

    def test_server_restart_various_states(self):
        self.install_default_presto()

        # Restart when the servers aren't started
        expected_output = list(
            set(self.expected_stop(not_running=self.all_hosts())[:]
                + self.expected_start()[:]))
        self.assert_simple_server_restart(expected_output, running_host='')

        # Restart when a coordinator is started but workers aren't
        not_running_hosts = self.all_hosts()[:]
        not_running_hosts.remove(self.master)
        expected_output = list(
            set(self.expected_stop(not_running=not_running_hosts)
                + self.expected_start()[:]))
        self.assert_simple_server_restart(expected_output,
                                          running_host=self.master)

        # Restart when one worker is started, but nothing else
        not_running_hosts = self.all_hosts()[:]
        not_running_hosts.remove(self.slaves[0])
        expected_output = list(
            set(self.expected_stop(not_running=not_running_hosts)
                + self.expected_start()[:]))
        self.assert_simple_server_restart(expected_output,
                                          running_host=self.slaves[0])

    def test_start_stop_restart_coordinator_down(self):
        self.install_presto_admin()
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.server_install()
        self.assert_start_stop_restart_down_node(self.slaves[0])

    def test_start_stop_restart_worker_down(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        self.assert_start_stop_restart_down_node(self.slaves[0])

    def assert_start_stop_restart_down_node(self, down_node):
        self.stop_and_wait(down_node)
        alive_hosts = self.all_hosts()[:]
        alive_hosts.remove(down_node)

        start_output = self.run_prestoadmin('server start')
        self.assert_parallel_execution_failure([down_node],
                                               'server.start',
                                               self.down_node_connection_error
                                               % {'host': down_node},
                                               start_output)

        expected_start = self.expected_start(start_success=alive_hosts)
        for message in expected_start:
            self.assertRegexpMatches(start_output, message, 'expected %s \n '
                                     'actual %s' % (message, start_output))

        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)

        stop_output = self.run_prestoadmin('server stop')
        self.assert_parallel_execution_failure([down_node],
                                               'server.stop',
                                               self.down_node_connection_error
                                               % {'host': down_node},
                                               stop_output)
        expected_stop = self.expected_stop(running=alive_hosts)
        for message in expected_stop:
            self.assertRegexpMatches(stop_output, message, 'expected %s \n '
                                     'actual %s' % (message, stop_output))
        self.assert_stopped(process_per_host)
        expected_stop = self.expected_stop(running=[],
                                           not_running=alive_hosts)
        restart_output = self.run_prestoadmin('server restart')
        self.assert_parallel_execution_failure([down_node],
                                               'server.restart',
                                               self.down_node_connection_error
                                               % {'host': down_node},
                                               restart_output)
        expected_restart = list(
            set(expected_stop[:] + expected_start[:])) + [r'']
        for message in expected_restart:
            self.assertRegexpMatches(restart_output, message, 'expected %s \n'
                                     ' actual %s' % (message, restart_output))
        process_per_host = self.get_process_per_host(restart_output)
        self.assert_started(process_per_host)

    def test_start_restart_config_file_error(self):
        self.install_default_presto()

        # Remove a required config file so that the server can't start
        self.exec_create_start(self.master, 'mv /etc/presto/config.properties '
                               '/etc/presto/config.properties.bak')

        started_hosts = self.all_hosts()
        started_hosts.remove(self.master)
        expected_start = self.expected_start(start_success=started_hosts,
                                             failed_hosts=[self.master])
        expected_start += [r'\[master\] out: ERROR: Config file is missing: '
                           r'/etc/presto/config.properties', r'', r'',
                           r'Warning: \[master\] sudo\(\) received nonzero '
                           r'return code 4 while executing \'set -m; '
                           r'/etc/rc.d/init.d/presto start\'!']
        expected_stop = self.expected_stop(not_running=[self.master])
        self.assert_simple_start_stop(expected_start, expected_stop)
        expected_restart = list(
            set(expected_stop[:] + expected_start[:-1] +
                [r'Warning: \[master\] sudo\(\) received nonzero '
                 r'return code 4 while executing \'set -m; '
                 r'/etc/rc.d/init.d/presto restart\'!'])) + [r'']
        self.assert_simple_server_restart(expected_restart,
                                          expected_stop=expected_stop)

    def assert_simple_start_stop(self, expected_start, expected_stop):
        cmd_output = self.run_prestoadmin('server start').splitlines()
        self.assertRegexpMatchesLineByLine(cmd_output, expected_start)
        process_per_host = self.get_process_per_host(cmd_output)
        self.assert_started(process_per_host)
        cmd_output = self.run_prestoadmin('server stop').splitlines()
        self.assertRegexpMatchesLineByLine(cmd_output, expected_stop)
        self.assert_stopped(process_per_host)

    def assert_simple_server_restart(self, expected_output,
                                     running_host='all',
                                     expected_stop=''):
        if running_host is 'all':
            start_output = self.run_prestoadmin('server start')
        elif running_host:
            start_output = self.run_prestoadmin('server start -H %s'
                                                % running_host)
        else:
            start_output = ''

        if not expected_stop:
            expected_stop = self.expected_stop()

        restart_output = self.run_prestoadmin('server restart').splitlines()
        self.assertRegexpMatchesLineByLine(restart_output, expected_output)

        if start_output:
            process_per_host = self.get_process_per_host(start_output)
            self.assert_stopped(process_per_host)

        process_per_host = self.get_process_per_host(restart_output)
        self.assert_started(process_per_host)

        cmd_output = self.run_prestoadmin('server stop').splitlines()
        self.assertRegexpMatchesLineByLine(cmd_output, expected_stop)
        self.assert_stopped(process_per_host)

    def assert_start_with_one_host_started(self, host):
        start_output = self.run_prestoadmin('server start -H %s' % host) \
            .splitlines()
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)

        start_output = self.run_prestoadmin('server start').splitlines()
        started_hosts = self.all_hosts()
        started_hosts.remove(host)
        self.assertRegexpMatchesLineByLine(
            start_output,
            self.expected_start(start_success=started_hosts,
                                already_started=[host])
        )
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)
        return process_per_host

    def assert_one_host_stopped(self, host):
        start_output = self.run_prestoadmin('server start -H %s' % host) \
            .splitlines()
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)
        stop_output = self.run_prestoadmin('server stop').splitlines()
        not_started_hosts = self.all_hosts()
        not_started_hosts.remove(host)
        self.assertRegexpMatchesLineByLine(
            stop_output,
            self.expected_stop(not_running=not_started_hosts)
        )
        process_per_host = self.get_process_per_host(start_output)
        self.assert_stopped(process_per_host)

    def expected_start(self, start_success=None, already_started=None,
                       failed_hosts=None):
        return_str = []

        # With no args, return message that all started successfully
        if not already_started and not start_success and not failed_hosts:
            start_success = self.all_hosts()

        if start_success:
            for host in start_success:
                return_str += [r'Checking server status on %s...' % host,
                               r'Server started successfully on: %s' % host,
                               r'\[%s\] out: ' % host,
                               r'\[%s\] out: Started as .*' % host,
                               r'\[%s\] out: Starting presto' % host]
        if already_started:
            for host in already_started:
                return_str += [r'Checking server status on %s...' % host,
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
