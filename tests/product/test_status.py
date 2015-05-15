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

from tests.product.base_product_case import BaseProductTestCase, \
    DOCKER_MOUNT_POINT, LOCAL_MOUNT_POINT, PRESTO_VERSION
from prestoadmin.util.constants import COORDINATOR_DIR, WORKERS_DIR


class TestStatus(BaseProductTestCase):

    def test_status_happy_path(self):
        ips = self.get_ip_address_dict()
        self.install_presto_admin()
        self.upload_topology()
        status_output = self.run_prestoadmin('server status')
        self.check_status_not_installed(status_output, ips)
        self.server_install()
        status_output = self.run_prestoadmin('server status')
        self.check_status_not_started(status_output, ips)
        self.run_prestoadmin('server start')
        status_output = self.run_prestoadmin('server status')
        self.check_status_normal(status_output, ips)

    def test_status_port_not_8080(self):
        self.install_presto_admin()
        self.upload_topology()

        port_config = """discovery.uri=http://master:8090
http-server.http.port=8090"""

        # write to master
        config_filename = 'config.properties'
        config_local_path = os.path.join(LOCAL_MOUNT_POINT % self.master,
                                         config_filename)
        with open(config_local_path, 'w') as config_file:
            config_file.write(port_config)

        # copy new config to correct location
        self.exec_create_start(self.master, 'mkdir -p ' + COORDINATOR_DIR)
        self.exec_create_start(
            self.master,
            'cp ' + os.path.join(DOCKER_MOUNT_POINT, config_filename) + ' ' +
            COORDINATOR_DIR
        )

        self.exec_create_start(self.master, 'mkdir -p ' + WORKERS_DIR)
        self.exec_create_start(
            self.master,
            'cp ' + os.path.join(DOCKER_MOUNT_POINT, config_filename) + ' ' +
            WORKERS_DIR
        )

        self.server_install()
        self.run_prestoadmin('server start')
        cmd_output = self.run_prestoadmin('server status')

        ips = self.get_ip_address_dict()
        self.check_status_normal(cmd_output, ips, 8090)

    def base_status(self, ips):
        statuses = []
        hosts_in_status = [self.master] + self.slaves[:]
        for host in hosts_in_status:
            role = 'coordinator' if host is self.master else 'worker'
            status = {'host': host, 'role': role, 'ip': ips[host],
                      'is_running': 'Running'}
            statuses += [status]
        return statuses

    def check_status(self, cmd_output, statuses, port=8080):
        expected_output = []
        for status in statuses:
            expected_output += \
                ['Server Status:',
                 '\t%s(IP: %s roles: %s): %s' %
                 (status['host'], status['ip'], status['role'],
                  status['is_running'])]
            if status['is_running'] is 'Running':
                expected_output += \
                    ['\tNode URI(http): http://%s:%s' % (status['ip'],
                                                         str(port)),
                     '\tPresto Version: ' + PRESTO_VERSION,
                     '\tNode is active: True',
                     '\tConnectors:     system, tpch']
            else:
                expected_output += [status['error_message']]

        # remove the last 4 lines: "Disconnecting from slave3... Done"
        actual = cmd_output.splitlines()[:-4]
        self.assertEqual(expected_output, actual)

    def check_status_normal(self, cmd_output, ips, port=8080):
        self.check_status(cmd_output, self.base_status(ips), port)

    def check_status_not_started(self, cmd_output, ips, port=8080):
        statuses = self.base_status(ips)
        for status in statuses:
            status['ip'] = 'Unknown'
            status['is_running'] = 'Not Running'
            status['error_message'] = '\tNo information available'

        self.check_status(cmd_output, statuses, port)

    def check_status_not_installed(self, cmd_output, ips, port=8080):
        statuses = self.base_status(ips)
        for status in statuses:
            status['ip'] = 'Unknown'
            status['is_running'] = 'Not Running'
            status['error_message'] = '\tPresto is not installed.'

        self.check_status(cmd_output, statuses, port)
