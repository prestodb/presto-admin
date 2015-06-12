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

from nose.plugins.attrib import attr

from prestoadmin.util import constants
from tests.product import base_product_case
from tests.product.base_product_case import BaseProductTestCase


class TestConfiguration(BaseProductTestCase):

    @attr('smoketest')
    def test_configuration_deploy_show(self):
        self.install_presto_admin()
        self.upload_topology()

        # deploy a default configuration, no files in coordinator or workers
        output = self.run_prestoadmin('configuration deploy')
        deploy_template = 'Deploying configuration on: %s\n'
        expected = ''
        for host in self.all_hosts():
            expected += deploy_template % host

        for host in self.all_hosts():
            self.assert_has_default_config(host)

        self.assertEqualIgnoringOrder(output, expected)

        filename = 'config.properties'
        path = os.path.join(constants.COORDINATOR_DIR, filename)
        dummy_prop1 = 'a.dummy.property:\'single-quoted\''
        conf_dummy_prop1 = 'a.dummy.property=\'single-quoted\''
        dummy_prop2 = 'another.dummy=value'
        conf_to_write = '%s\n%s' % (dummy_prop1, dummy_prop2)
        self.write_content_to_master(conf_to_write, path)

        path = os.path.join(constants.WORKERS_DIR, filename)
        self.write_content_to_master(conf_to_write, path)

        # deploy coordinator configuration only.  Has a non-default file
        output = self.run_prestoadmin('configuration deploy coordinator')
        self.assertEqual(output, deploy_template % self.master)
        for container in self.slaves:
            self.assert_has_default_config(container)

        self.assert_file_content(self.master,
                                 os.path.join(constants.REMOTE_CONF_DIR,
                                              'config.properties'),
                                 conf_dummy_prop1 + '\n' +
                                 dummy_prop2 + '\n' +
                                 self.default_coordinator_config_)

        filename = 'node.properties'
        path = os.path.join(constants.WORKERS_DIR, filename)
        self.write_content_to_master('node.environment test', path)
        path = os.path.join(constants.COORDINATOR_DIR, filename)
        self.write_content_to_master('node.environment test', path)

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
                                     conf_dummy_prop1 + '\n' +
                                     dummy_prop2 + '\n' +
                                     self.default_workers_config_)
            expected = """node.data-dir=/var/lib/presto/data
node.environment=test
plugin.config-dir=/etc/presto/catalog
plugin.dir=/usr/lib/presto/lib/plugin\n"""
            self.assert_node_config(container, expected)

        self.assert_node_config(self.master, self.default_node_properties_)

    def test_lost_coordinator_connection(self):
        self.install_presto_admin()
        bad_host = self.slaves[0]
        good_hosts = [self.master, self.slaves[1], self.slaves[2]]
        topology = {'coordinator': bad_host,
                    'workers': good_hosts}
        self.upload_topology(topology)
        self.stop_and_wait(bad_host)
        output = self.run_prestoadmin('configuration deploy')
        self.assertRegexpMatches(output, self.down_node_connection_error %
                                 {'host': bad_host})
        for host in self.all_hosts():
            self.assertTrue('Deploying configuration on: %s' % host in output)
        expected_size = self.len_down_node_error + len(self.all_hosts())
        self.assertEqual(len(output.splitlines()), expected_size)

        output = self.remove_disconnecting_msg(
            self.run_prestoadmin('configuration show config'))
        error = str.join('\n', output.splitlines()[:6])
        self.assertRegexpMatches(error,
                                 self.down_node_connection_error %
                                 {'host': bad_host})
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_down_node.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(str.join('\n', output.splitlines()[6:]), expected)

    def test_deploy_lost_worker_connection(self):
        self.install_presto_admin()
        self.upload_topology()
        bad_host = self.slaves[0]
        self.stop_and_wait(bad_host)
        output = self.run_prestoadmin('configuration deploy')
        self.assertRegexpMatches(output, self.down_node_connection_error %
                                 {'host': bad_host})
        for host in self.all_hosts():
            self.assertTrue('Deploying configuration on: %s' % host in output)
        expected_length = len(self.all_hosts()) + self.len_down_node_error
        self.assertEqual(len(output.splitlines()), expected_length)

    def test_configuration_show(self):
        self.install_presto_admin()
        self.upload_topology()

        # configuration show no configuration
        output = self.remove_disconnecting_msg(
            self.run_prestoadmin('configuration show'))
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_none.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(expected, output)

        self.run_prestoadmin('configuration deploy')

        # configuration show default configuration
        output = self.remove_disconnecting_msg(
            self.run_prestoadmin('configuration show'))
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_default.txt'), 'r') as f:
            expected = f.read()
        self.assertRegexpMatches(output, expected)

        # configuration show node
        output = self.remove_disconnecting_msg(
            self.run_prestoadmin('configuration show node'))
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_node.txt'), 'r') as f:
            expected = f.read()
        self.assertRegexpMatches(output, expected)

        # configuration show jvm
        output = self.remove_disconnecting_msg(
            self.run_prestoadmin('configuration show jvm'))
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_jvm.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(output, expected)

        # configuration show config
        output = self.remove_disconnecting_msg(
            self.run_prestoadmin('configuration show config'))
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_config.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(output, expected)

        # configuration show log no log.properties
        output = self.remove_disconnecting_msg(
            self.run_prestoadmin('configuration show log'))
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_log_none.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(output, expected)

        # configuration show log has log.properties
        log_properties = 'com.facebook.presto=WARN'
        filename = 'log.properties'
        self.write_content_to_master(log_properties,
                                     os.path.join(constants.WORKERS_DIR,
                                                  filename))
        self.write_content_to_master(log_properties,
                                     os.path.join(constants.COORDINATOR_DIR,
                                                  filename))
        self.run_prestoadmin('configuration deploy')

        output = self.remove_disconnecting_msg(
            self.run_prestoadmin('configuration show log'))
        with open(os.path.join(base_product_case.LOCAL_RESOURCES_DIR,
                               'configuration_show_log.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(output, expected)

    def remove_disconnecting_msg(self, output):
        return str.join('\n', output.splitlines()[:-4])
