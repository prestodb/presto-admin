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
Product tests for presto-admin configuration
"""

import os
from prestoadmin.util import constants
from tests.product import base_product_case
from tests.product.base_product_case import BaseProductTestCase


class TestConfiguration(BaseProductTestCase):

    def test_configuration_deploy_show(self):
        self.install_presto_admin()
        self.upload_topology()

        # configuration show no configuration
        output = self.run_prestoadmin('configuration show')
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_none.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(expected, output)

        # deploy a default configuration, no files in coordinator or workers
        output = self.run_prestoadmin('configuration deploy')
        deploy_template = 'Deploying configuration on: %s\n'
        expected = ''
        for host in self.all_hosts():
            expected += deploy_template % host

        for host in self.all_hosts():
            self.assert_has_default_config(host)

        self.assertEqualIgnoringOrder(output, expected)

        # configuration show default configuration
        output = self.run_prestoadmin('configuration show')
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_default.txt'), 'r') as f:
            expected = f.read()
        self.assertRegexpMatches(output, expected)

        filename = 'config.properties'
        path = os.path.join(constants.COORDINATOR_DIR, filename)
        dummy_property = 'a.dummy.property=\'single-quoted\''
        self.write_content_to_master(dummy_property,
                                     path)

        path = os.path.join(constants.WORKERS_DIR, filename)
        self.write_content_to_master(dummy_property, path)

        # deploy coordinator configuration only.  Has a non-default file
        output = self.run_prestoadmin('configuration deploy coordinator')
        self.assertEqual(output, deploy_template % self.master)
        for container in self.slaves:
            self.assert_has_default_config(container)

        self.assert_file_content(self.master,
                                 os.path.join(constants.REMOTE_CONF_DIR,
                                              'config.properties'),
                                 dummy_property + '\n' +
                                 self.default_coordinator_config_)

        filename = 'node.properties'
        path = os.path.join(constants.WORKERS_DIR, filename)
        self.write_content_to_master('node.environment=test', path)
        path = os.path.join(constants.COORDINATOR_DIR, filename)
        self.write_content_to_master('node.environment=test', path)

        # deploy workers configuration only has non-default file
        output = self.run_prestoadmin('configuration deploy workers')
        expected = ''
        for host in self.slaves:
            expected += deploy_template % host
        self.assertEqualIgnoringOrder(output, expected)

        for container in self.slaves:
            self.assert_file_content(container,
                                     os.path.join(constants.REMOTE_CONF_DIR,
                                                  'config.properties'),
                                     dummy_property + '\n' +
                                     self.default_workers_config_)
            expected = """node.data-dir=/var/lib/presto/data
node.environment=test
plugin.config-dir=/etc/presto/catalog
plugin.dir=/usr/lib/presto/lib/plugin\n"""
            self.assert_node_config(container, expected)

        self.assert_node_config(self.master, self.default_node_properties_)

    def test_deploy_lost_coordinator_connection(self):
        self.install_presto_admin()
        bad_host = self.slaves[0]
        good_hosts = [self.master, self.slaves[1], self.slaves[2]]
        topology = {'coordinator': bad_host,
                    'workers': good_hosts}
        self.upload_topology(topology)
        self.stop_and_wait(bad_host)
        output = self.run_prestoadmin('configuration deploy')
        self.assert_parallel_execution_failure([bad_host],
                                               'configuration.deploy',
                                               self.down_node_connection_error
                                               % {'host': bad_host},
                                               output)
        for host in self.all_hosts():
            self.assertTrue('Deploying configuration on: %s' % host in output)

    def test_deploy_workers_lost_worker_connection(self):
        self.install_presto_admin()
        self.upload_topology()
        bad_host = self.slaves[0]
        self.stop_and_wait(bad_host)
        output = self.run_prestoadmin('configuration deploy')
        self.assert_parallel_execution_failure([bad_host],
                                               'configuration.deploy',
                                               self.down_node_connection_error
                                               % {'host': bad_host},
                                               output)
        for host in self.all_hosts():
            self.assertTrue('Deploying configuration on: %s' % host in output)
