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
import re

from nose.plugins.attrib import attr

from tests.product.base_product_case import BaseProductTestCase, PRESTO_VERSION
from prestoadmin.util.constants import COORDINATOR_DIR, WORKERS_DIR


class TestStatus(BaseProductTestCase):

    def setUp(self):
        super(TestStatus, self).setUp()
        self.setup_cluster()

    @attr('smoketest')
    def test_status_happy_path(self):
        ips = self.cluster.get_ip_address_dict()
        self.install_presto_admin(self.cluster)
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
        self.check_status(
            status_output,
            self.single_node_up_status(
                ips, self.cluster.internal_master))

        # Test with coordinator not started
        self.run_prestoadmin('server stop')
        self.run_prestoadmin('server start -H slave1')
        status_output = self.run_prestoadmin('server status')
        self.check_status(
            status_output,
            self.single_node_up_status(
                ips, self.cluster.internal_slaves[0]))

        # Check that the slave sees that it's stopped, even though the
        # discovery server is not up.
        self.run_prestoadmin('server stop')
        status_output = self.run_prestoadmin('server status')
        self.check_status(status_output, self.not_started_status(ips))

    @attr('quarantine')
    def test_connection_to_coordinator_lost(self):
        ips = self.cluster.get_ip_address_dict()
        self.install_presto_admin(self.cluster)
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.server_install()
        self.run_prestoadmin('server start')
        self.cluster.stop_host(
            self.cluster.slaves[0])
        status_output = self.run_prestoadmin('server status')
        statuses = self.node_not_available_status(
            ips, topology, self.cluster.internal_slaves[0])
        self.check_status(status_output, statuses)

    @attr('quarantine')
    def test_connection_to_worker_lost(self):
        ips = self.cluster.get_ip_address_dict()
        self.install_presto_admin(self.cluster)
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.server_install()
        self.run_prestoadmin('server start')
        self.cluster.stop_host(
            self.cluster.slaves[1])
        status_output = self.run_prestoadmin('server status')
        statuses = self.node_not_available_status(
            ips, topology, self.cluster.internal_slaves[1])
        self.check_status(status_output, statuses)

    def test_status_port_not_8080(self):
        self.install_presto_admin(self.cluster)
        self.upload_topology()

        port_config = """discovery.uri=http://master:8090
http-server.http.port=8090"""

        # write to master
        config_filename = 'config.properties'
        self.cluster.write_content_to_host(
            port_config,
            os.path.join(COORDINATOR_DIR, config_filename),
            self.cluster.master
        )

        self.cluster.write_content_to_host(
            port_config,
            os.path.join(WORKERS_DIR, config_filename),
            self.cluster.master
        )

        self.server_install()
        self.run_prestoadmin('server start')
        cmd_output = self.run_prestoadmin('server status')

        ips = self.cluster.get_ip_address_dict()
        self.check_status(cmd_output, self.base_status(ips), 8090)

    def base_status(self, ips, topology=None):
        if not topology:
            topology = {
                'coordinator': self.cluster.internal_master, 'workers':
                [self.cluster.internal_slaves[0],
                 self.cluster.internal_slaves[1],
                 self.cluster.internal_slaves[2]]
            }
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
            if status['host'] == node:
                index = i
                status['no_status'] = True
                status['is_running'] = 'Not Running'
                status['error_message'] = \
                    self.down_node_connection_error % {'host': node}
                i += 1
        if index >= 0:
            temp = statuses[index]
            statuses.remove(temp)
            statuses.insert(0, temp)

        return statuses

    def check_status(self, cmd_output, statuses, port=8080):
        expected_output_without_errors = []
        expected_errors = []
        num_bad_hosts = 0
        for status in statuses:
            if 'no_status' in status:
                num_bad_hosts += 1
                expected_errors += [status['error_message']]
            else:
                expected_output_without_errors += \
                    ['Server Status:',
                     '\t%s\(IP: %s roles: %s\): %s' %
                     (status['host'], status['ip'], status['role'],
                      status['is_running'])]
                if status['is_running'] is 'Running':
                    expected_output_without_errors += \
                        ['\tNode URI\(http\): http://%s:%s' % (status['ip'],
                                                               str(port)),
                         '\tPresto Version: ' + PRESTO_VERSION,
                         '\tNode is active: True',
                         '\tConnectors:     system, tpch']
                else:
                    expected_output_without_errors += [status['error_message']]

        # Error messages can be anywhere, so strip them out
        (actual_output, actual_errors) = \
            self.strip_out_error_messages(cmd_output)
        errors_split_by_line = []
        for error in actual_errors:
            errors_split_by_line += error.splitlines()
        self.assertRegexpMatches(actual_output,
                                 '\n'.join(expected_output_without_errors))
        self.assertRegexpMatchesLineByLine(actual_errors, expected_errors)

    def strip_out_error_messages(self, cmd_output):
        error_regex = self.down_node_connection_error % {'host': '.*'} + '\n'
        error_output = re.findall(cmd_output, error_regex, re.MULTILINE)
        errorless_output = re.sub(error_regex, '', cmd_output)
        return errorless_output, error_output
