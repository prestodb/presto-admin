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
        expected_output = ['Server Status:',
                           '\tmaster(IP: ' + ips[self.master] +
                           ' roles: coordinator): Running',
                           '\tNode URI(http): http://' + ips[self.master] +
                           ':8090',
                           '\tPresto Version: ' + PRESTO_VERSION,
                           '\tNode is active: True',
                           '\tConnectors:     system, tpch',
                           'Server Status:',
                           '\tslave1(IP: ' + ips[self.slaves[0]] +
                           ' roles: worker): Running',
                           '\tNode URI(http): http://' + ips[self.slaves[0]] +
                           ':8090',
                           '\tPresto Version: ' + PRESTO_VERSION,
                           '\tNode is active: True',
                           '\tConnectors:     system, tpch',
                           'Server Status:',
                           '\tslave2(IP: ' + ips[self.slaves[1]] +
                           ' roles: worker): Running',
                           '\tNode URI(http): http://' + ips[self.slaves[1]] +
                           ':8090',
                           '\tPresto Version: ' + PRESTO_VERSION,
                           '\tNode is active: True',
                           '\tConnectors:     system, tpch',
                           'Server Status:',
                           '\tslave3(IP: ' + ips[self.slaves[2]] +
                           ' roles: worker): Running',
                           '\tNode URI(http): http://' + ips[self.slaves[2]] +
                           ':8090',
                           '\tPresto Version: ' + PRESTO_VERSION,
                           '\tNode is active: True',
                           '\tConnectors:     system, tpch'
                           ]

        # remove the last 4 lines: "Disconnecting from slave 3... Done"
        actual = cmd_output.splitlines()[:-4]
        self.assertEqual(expected_output, actual)
