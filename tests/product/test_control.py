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
from tests.product.base_product_case import BaseProductTestCase


class TestControl(BaseProductTestCase):

    @attr('smoketest')
    def test_server_start_stop_simple(self):
        self.setup_docker_cluster('presto')
        self.assert_simple_start_stop(self.expected_start(),
                                      self.expected_stop())

    @attr('smoketest')
    def test_server_restart_simple(self):
        self.setup_docker_cluster('presto')
        expected_output = self.expected_stop()[:] + self.expected_start()[:]
        self.assert_simple_server_restart(expected_output)

    def test_server_start_without_topology(self):
        self.assert_service_fails_without_topology('start')

    def test_server_stop_without_topology(self):
        self.assert_service_fails_without_topology('stop')

    def test_server_restart_without_topology(self):
        self.assert_service_fails_without_topology('restart')

    def assert_service_fails_without_topology(self, service):
        self.setup_docker_cluster()
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
        self.setup_docker_cluster()
        self.install_presto_admin()
        self.upload_topology()
        # Start without Presto installed
        start_output = self.run_prestoadmin('server %s' % service,
                                            raise_error=False).splitlines()
        presto_not_installed = self.presto_not_installed_message()
        self.assertEqualIgnoringOrder(presto_not_installed,
                                      '\n'.join(start_output))

    def test_server_start_various_states(self):
        self.setup_docker_cluster('presto')

        # Coordinator started, workers not; then server start
        process_per_host = \
            self.assert_start_with_one_host_started(
                self.docker_cluster.master)

        # Worker started, coord and other workers not; then server start
        self.run_prestoadmin('server stop').splitlines()
        self.assert_stopped(process_per_host)
        self.assert_start_with_one_host_started(
            self.docker_cluster.slaves[0])

        # All started; then server start
        start_output = self.run_prestoadmin('server start').splitlines()
        self.assertRegexpMatchesLineByLine(
            start_output,
            self.expected_port_warn(self.docker_cluster.all_hosts())
        )
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)

    def test_server_stop_various_states(self):
        self.setup_docker_cluster('presto')

        # Stop with servers not started
        stop_output = self.run_prestoadmin('server stop').splitlines()
        not_started_hosts = self.docker_cluster.all_hosts()
        self.assertRegexpMatchesLineByLine(
            stop_output,
            self.expected_stop(not_running=not_started_hosts)
        )

        # Stop with coordinator started, but not workers
        self.assert_one_host_stopped(self.docker_cluster.master)

        # Stop with worker started, but nothing else
        self.assert_one_host_stopped(self.docker_cluster.slaves[0])

    def test_server_restart_various_states(self):
        self.setup_docker_cluster('presto')

        # Restart when the servers aren't started
        expected_output = self.expected_stop(
            not_running=self.docker_cluster.all_hosts())[:] +\
            self.expected_start()[:]
        self.assert_simple_server_restart(expected_output, running_host='')

        # Restart when a coordinator is started but workers aren't
        not_running_hosts = self.docker_cluster.all_hosts()[:]
        not_running_hosts.remove(self.docker_cluster.master)
        expected_output = self.expected_stop(
            not_running=not_running_hosts) + self.expected_start()[:]
        self.assert_simple_server_restart(
            expected_output, running_host=self.docker_cluster.master)

        # Restart when one worker is started, but nothing else
        not_running_hosts = self.docker_cluster.all_hosts()[:]
        not_running_hosts.remove(self.docker_cluster.slaves[0])
        expected_output = self.expected_stop(
            not_running=not_running_hosts) + self.expected_start()[:]
        self.assert_simple_server_restart(
            expected_output, running_host=self.docker_cluster.slaves[0])

    def test_start_stop_restart_coordinator_down(self):
        self.setup_docker_cluster()
        self.install_presto_admin()
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.server_install()
        self.assert_start_stop_restart_down_node(
            self.docker_cluster.slaves[0])

    def test_start_stop_restart_worker_down(self):
        self.setup_docker_cluster()
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        self.assert_start_stop_restart_down_node(
            self.docker_cluster.slaves[0])

    def test_server_start_twice(self):
        self.setup_docker_cluster('presto')
        start_output = self.run_prestoadmin('server start').splitlines()
        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)
        self.run_prestoadmin('server stop -H ' +
                             self.docker_cluster.slaves[0])

        # Start all again
        start_with_warn = self.run_prestoadmin('server start').splitlines()
        expected = self.expected_start(
            start_success=[self.docker_cluster.slaves[0]],
            already_started=[], failed_hosts=[])
        alive_hosts = self.docker_cluster.all_hosts()[:]
        alive_hosts.remove(self.docker_cluster.slaves[0])
        expected.extend(self.expected_port_warn(alive_hosts))
        self.assertRegexpMatchesLineByLine(start_with_warn, expected)

    def assert_start_stop_restart_down_node(self, down_node):
        self.docker_cluster.stop_container_and_wait(down_node)
        alive_hosts = self.docker_cluster.all_hosts()[:]
        alive_hosts.remove(down_node)

        start_output = self.run_prestoadmin('server start')

        self.assertRegexpMatches(start_output, self.down_node_connection_error
                                 % {'host': down_node})

        expected_start = self.expected_start(start_success=alive_hosts)
        for message in expected_start:
            self.assertRegexpMatches(start_output, message, 'expected %s \n '
                                     'actual %s' % (message, start_output))

        process_per_host = self.get_process_per_host(start_output)
        self.assert_started(process_per_host)

        stop_output = self.run_prestoadmin('server stop')
        self.assertRegexpMatches(stop_output, self.down_node_connection_error
                                 % {'host': down_node})
        expected_stop = self.expected_stop(running=alive_hosts)
        for message in expected_stop:
            self.assertRegexpMatches(stop_output, message, 'expected %s \n '
                                     'actual %s' % (message, stop_output))
        self.assert_stopped(process_per_host)
        expected_stop = self.expected_stop(running=[],
                                           not_running=alive_hosts)
        self.assertEqual(len(stop_output.splitlines()),
                         self.expected_down_node_output_size(expected_stop))
        restart_output = self.run_prestoadmin('server restart')
        self.assertRegexpMatches(restart_output,
                                 self.down_node_connection_error
                                 % {'host': down_node})
        expected_restart = list(
            set(expected_stop[:] + expected_start[:]))
        for host in alive_hosts:
            expected_restart += [r'\[%s\] out: ' % host]
        for message in expected_restart:
            self.assertRegexpMatches(restart_output, message, 'expected %s \n'
                                     ' actual %s' % (message, restart_output))
        self.assertEqual(len(restart_output.splitlines()),
                         self.expected_down_node_output_size(expected_restart))
        process_per_host = self.get_process_per_host(restart_output)
        self.assert_started(process_per_host)

    def expected_down_node_output_size(self, expected_output):
        return self.len_down_node_error + len(
            '\n'.join(expected_output).splitlines())

    def test_start_restart_config_file_error(self):
        self.setup_docker_cluster('presto')

        # Remove a required config file so that the server can't start
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master,
            'mv /etc/presto/config.properties '
            '/etc/presto/config.properties.bak')

        started_hosts = self.docker_cluster.all_hosts()
        started_hosts.remove(self.docker_cluster.master)
        expected_start = self.expected_start(
            start_success=started_hosts)
        error_msg = self.escape_for_regex(self.replace_keywords("""
Fatal error: [%(master)s] run() received nonzero return code 2 while \
executing!

Requested: grep http-server.http.port= /etc/presto/config.properties
Executed: /bin/bash -l -c "grep http-server.http.port= /etc/presto/\
config.properties"

=============================== Standard output ===============================

grep: /etc/presto/config.properties: No such file or directory

============================================================================\
====

Aborting.
""")).splitlines()
        expected_start += error_msg
        expected_stop = self.expected_stop(
            not_running=[self.docker_cluster.master])
        self.assert_simple_start_stop(expected_start, expected_stop)
        expected_restart = expected_stop[:] + expected_start[:]
        self.assert_simple_server_restart(expected_restart,
                                          expected_stop=expected_stop)

    def assert_simple_start_stop(self, expected_start, expected_stop):
        cmd_output = self.run_prestoadmin('server start')
        cmd_output = cmd_output.splitlines()
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
        started_hosts = self.docker_cluster.all_hosts()
        started_hosts.remove(host)
        started_expected = self.expected_start(start_success=started_hosts)
        started_expected.extend(self.expected_port_warn([host]))
        self.assertRegexpMatchesLineByLine(
            start_output,
            started_expected
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
        not_started_hosts = self.docker_cluster.all_hosts()
        not_started_hosts.remove(host)
        self.assertRegexpMatchesLineByLine(
            stop_output,
            self.expected_stop(not_running=not_started_hosts)
        )
        process_per_host = self.get_process_per_host(start_output)
        self.assert_stopped(process_per_host)

    def expected_port_warn(self, hosts=None):
        return_str = []
        for host in hosts:
            return_str += [r'Warning: \[%s\] Server failed to start on %s. '
                           r'Port 8080 already in use' % (host, host), r'',
                           r'']
        return return_str

    def expected_start(self, start_success=None, already_started=None,
                       failed_hosts=None):
        return_str = []

        # With no args, return message that all started successfully
        if not already_started and not start_success and not failed_hosts:
            start_success = self.docker_cluster.all_hosts()

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
