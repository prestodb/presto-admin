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
Product tests for presto-admin status commands
"""
import os

from tests.product.base_product_case import BaseProductTestCase, PRESTO_VERSION
from prestoadmin.util.constants import COORDINATOR_DIR, WORKERS_DIR


class TestStatus(BaseProductTestCase):

    def test_status_happy_path(self):
        ips = self.get_ip_address_dict()
        self.install_presto_admin()
        self.upload_topology()
        status_output = self.run_prestoadmin('server status')
        self.check_status(status_output, self.not_installed_status(ips))
        self.server_install()
        status_output = self.run_prestoadmin('server status')
        self.check_status(status_output, self.not_started_status(ips))
        self.run_prestoadmin('server start')
        status_output = self.run_prestoadmin('server status')
        self.check_status(status_output, self.base_status(ips))

        self.run_prestoadmin('server stop')

        # Test with worker not started
        self.run_prestoadmin('server start -H master')
        status_output = self.run_prestoadmin('server status')
        self.check_status(status_output,
                          self.single_node_up_status(ips, self.master))

        # Test with coordinator not started
        self.run_prestoadmin('server stop')
        self.run_prestoadmin('server start -H slave1')
        status_output = self.run_prestoadmin('server status')
        self.check_status(status_output,
                          self.single_node_up_status(ips, self.slaves[0]))

        # Check that the slave sees that it's stopped, even though the
        # discovery server is not up.
        self.run_prestoadmin('server stop')
        status_output = self.run_prestoadmin('server status')
        self.check_status(status_output, self.not_started_status(ips))

    def test_connection_to_coordinator_lost(self):
        ips = self.get_ip_address_dict()
        self.install_presto_admin()
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.server_install()
        self.run_prestoadmin('server start')
        self.stop_and_wait(self.slaves[0])
        status_output = self.run_prestoadmin('server status')
        statuses = self.node_not_available_status(ips, topology,
                                                  self.slaves[0])
        self.check_status(status_output, statuses)

    def test_connection_to_worker_lost(self):
        ips = self.get_ip_address_dict()
        self.install_presto_admin()
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.server_install()
        self.run_prestoadmin('server start')
        self.stop_and_wait(self.slaves[1])
        status_output = self.run_prestoadmin('server status')
        statuses = self.node_not_available_status(ips, topology,
                                                  self.slaves[1])
        self.check_status(status_output, statuses)

    def test_status_port_not_8080(self):
        self.install_presto_admin()
        self.upload_topology()

        port_config = """discovery.uri=http://master:8090
http-server.http.port=8090"""

        # write to master
        config_filename = 'config.properties'
        self.write_content_to_master(port_config,
                                     os.path.join(COORDINATOR_DIR,
                                                  config_filename))

        self.write_content_to_master(port_config,
                                     os.path.join(WORKERS_DIR,
                                                  config_filename))

        self.server_install()
        self.run_prestoadmin('server start')
        cmd_output = self.run_prestoadmin('server status')

        ips = self.get_ip_address_dict()
        self.check_status(cmd_output, self.base_status(ips), 8090)

    def base_status(self, ips, topology=None):
        if not topology:
            topology = {'coordinator': self.master, 'workers':
                        [self.slaves[0], self.slaves[1], self.slaves[2]]}
        statuses = []
        hosts_in_status = [topology['coordinator']] + topology['workers'][:]
        for host in hosts_in_status:
            role = 'coordinator' if host is topology['coordinator']\
                else 'worker'
            status = {'host': host, 'role': role, 'ip': ips[host],
                      'is_running': 'Running'}
            statuses += [status]
        return statuses

    def not_started_status(self, ips):
        statuses = self.base_status(ips)
        for status in statuses:
            status['ip'] = 'Unknown'
            status['is_running'] = 'Not Running'
            status['error_message'] = '\tNo information available'
        return statuses

    def not_installed_status(self, ips):
        statuses = self.base_status(ips)
        for status in statuses:
            status['ip'] = 'Unknown'
            status['is_running'] = 'Not Running'
            status['error_message'] = '\tPresto is not installed.'
        return statuses

    def single_node_up_status(self, ips, node):
        statuses = self.not_started_status(ips)
        for status in statuses:
            if status['host'] is node:
                status['ip'] = ips[node]
                status['is_running'] = 'Running'
                status['error_message'] = ''
        return statuses

    def node_not_available_status(self, ips, topology, node):
        statuses = self.base_status(ips, topology)
        index = -1
        i = 0
        for status in statuses:
            if status['host'] is node:
                index = i
                status['no_status'] = True
                status['is_running'] = 'Not Running'
                error1 = r'\nWarning: Low level socket error connecting to ' \
                         r'host %s on port 22: No route to host ' \
                         r'\(tried 1 time\)\n\nUnderlying exception:\n    ' \
                         r'No route to host\n' % node
                error2 = r'\nWarning: Timed out trying to connect to %s ' \
                         r'\(tried 1 time\)\n\nUnderlying exception:\n    ' \
                         r'timed out\n' % node

                status['error_message'] = '(%s|%s)' % (error1, error2)
                status['unavailable_message'] = '(%s|%s)' % (error1, error2)
                i += 1
        if index >= 0:
            temp = statuses[index]
            statuses.remove(temp)
            statuses.insert(0, temp)

        return statuses

    def check_status(self, cmd_output, statuses, port=8080):
        expected_output = []
        num_bad_hosts = 0
        for status in statuses:
            if 'no_status' in status:
                num_bad_hosts += 1
                expected_output = [status['error_message']] + expected_output
            else:
                expected_output += \
                    ['Server Status:',
                     '\t%s\(IP: %s roles: %s\): %s' %
                     (status['host'], status['ip'], status['role'],
                      status['is_running'])]
                if status['is_running'] is 'Running':
                    expected_output += \
                        ['\tNode URI\(http\): http://%s:%s' % (status['ip'],
                                                               str(port)),
                         '\tPresto Version: ' + PRESTO_VERSION,
                         '\tNode is active: True',
                         '\tConnectors:     system, tpch']
                else:
                    expected_output += [status['error_message']]

        # remove the last 4 lines: "Disconnecting from slave3... Done"
        actual = cmd_output.splitlines()[: (num_bad_hosts - 4)]
        self.assertRegexpMatches('\n'.join(actual), '\n'.join(expected_output))
