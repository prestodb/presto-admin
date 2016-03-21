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
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.constants import LOCAL_RESOURCES_DIR


class TestConfiguration(BaseProductTestCase):

    def setUp(self):
        super(TestConfiguration, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)
        self.write_test_configs(self.cluster)

    def deploy_and_assert_default_config(self):
        # deploy a default configuration, no files in coordinator or workers
        output = self.run_prestoadmin('configuration deploy')
        deploy_template = 'Deploying configuration on: %s\n'
        expected = ''
        for host in self.cluster.all_internal_hosts():
            expected += deploy_template % host

        for host in self.cluster.all_hosts():
            self.assert_has_default_config(host)

        self.assertEqualIgnoringOrder(output, expected)

        # redeploy configuration to test the default files that we wrote out
        output = self.run_prestoadmin('configuration deploy')
        deploy_template = 'Deploying configuration on: %s\n'

        for host in self.cluster.all_hosts():
            self.assert_has_default_config(host)

        self.assertEqualIgnoringOrder(output, expected)

    def __write_dummy_config_file(self):
        # deploy coordinator configuration only.  Has a non-default file
        dummy_prop1 = 'a.dummy.property=\'single-quoted\''
        dummy_prop2 = 'another.dummy=value'
        extra_configs = '%s\n%s' % (dummy_prop1, dummy_prop2)
        self.write_test_configs(self.cluster, extra_configs)
        return dummy_prop1, dummy_prop2

    @attr('smoketest')
    def test_configuration_deploy_show(self):
        self.upload_topology()

        self.deploy_and_assert_default_config()

        # deploy coordinator configuration only.  Has a non-default file
        dummy_prop1, dummy_prop2 = self.__write_dummy_config_file()

        output = self.run_prestoadmin('configuration deploy coordinator')
        deploy_template = 'Deploying configuration on: %s\n'
        self.assertEqual(output,
                         deploy_template % self.cluster.internal_master)
        for container in self.cluster.slaves:
            self.assert_has_default_config(container)

        self.assert_file_content(self.cluster.master,
                                 os.path.join(constants.REMOTE_CONF_DIR,
                                              'config.properties'),
                                 dummy_prop1 + '\n' +
                                 dummy_prop2 + '\n' +
                                 self.default_coordinator_test_config_)

        # deploy workers configuration only has non-default file
        filename = 'node.properties'
        path = os.path.join(constants.WORKERS_DIR, filename)
        self.cluster.write_content_to_host(
            'node.environment test', path, self.cluster.master)
        path = os.path.join(constants.COORDINATOR_DIR, filename)
        self.cluster.write_content_to_host(
            'node.environment test', path, self.cluster.master)

        output = self.run_prestoadmin('configuration deploy workers')
        expected = ''
        for host in self.cluster.internal_slaves:
            expected += deploy_template % host
        self.assertEqualIgnoringOrder(output, expected)

        for container in self.cluster.slaves:
            self.assert_file_content(container,
                                     os.path.join(constants.REMOTE_CONF_DIR,
                                                  'config.properties'),
                                     dummy_prop1 + '\n' +
                                     dummy_prop2 + '\n' +
                                     self.default_workers_test_config_)
            expected = 'node.environment=test\n'
            self.assert_node_config(container, expected)

        self.assert_node_config(self.cluster.master,
                                self.default_node_properties_)

    def test_configuration_deploy_using_dash_h_coord_worker(self):
        self.upload_topology()

        self.deploy_and_assert_default_config()

        dummy_prop1, dummy_prop2 = self.__write_dummy_config_file()

        output = self.run_prestoadmin('configuration deploy '
                                      '-H %(master)s,%(slave1)s')
        deploy_template = 'Deploying configuration on: %s\n'
        expected = ''
        for host in [self.cluster.internal_master,
                     self.cluster.internal_slaves[0]]:
            expected += deploy_template % host

        for host in [self.cluster.slaves[1], self.cluster.slaves[2]]:
            self.assert_has_default_config(host)

        self.assertEqualIgnoringOrder(output, expected)

        self.assert_file_content(self.cluster.master,
                                 os.path.join(constants.REMOTE_CONF_DIR,
                                              'config.properties'),
                                 dummy_prop1 + '\n' +
                                 dummy_prop2 + '\n' +
                                 self.default_coordinator_test_config_)
        self.assert_file_content(self.cluster.slaves[0],
                                 os.path.join(constants.REMOTE_CONF_DIR,
                                              'config.properties'),
                                 dummy_prop1 + '\n' +
                                 dummy_prop2 + '\n' +
                                 self.default_workers_test_config_)

    def test_configuration_deploy_using_dash_x_coord_worker(self):
        self.upload_topology()

        self.deploy_and_assert_default_config()

        dummy_prop1, dummy_prop2 = self.__write_dummy_config_file()

        output = self.run_prestoadmin('configuration deploy '
                                      '-x %(master)s,%(slave1)s')
        self.assert_has_default_config(self.cluster.master)
        self.assert_has_default_config(self.cluster.slaves[0])
        deploy_template = 'Deploying configuration on: %s\n'
        expected = ''
        for host in [self.cluster.internal_slaves[1],
                     self.cluster.internal_slaves[2]]:
            expected += deploy_template % host

        self.assertEqualIgnoringOrder(output, expected)

        for slave in [self.cluster.slaves[1], self.cluster.slaves[2]]:
            self.assert_file_content(slave,
                                     os.path.join(constants.REMOTE_CONF_DIR,
                                                  'config.properties'),
                                     dummy_prop1 + '\n' +
                                     dummy_prop2 + '\n' +
                                     self.default_workers_test_config_)

    def test_lost_coordinator_connection(self):
        internal_bad_host = self.cluster.internal_slaves[0]
        bad_host = self.cluster.slaves[0]
        good_hosts = [self.cluster.internal_master,
                      self.cluster.internal_slaves[1],
                      self.cluster.internal_slaves[2]]
        topology = {'coordinator': internal_bad_host,
                    'workers': good_hosts}
        self.upload_topology(topology)
        self.cluster.stop_host(bad_host)
        output = self.run_prestoadmin('configuration deploy',
                                      raise_error=False)
        self.assertRegexpMatches(
            output,
            self.down_node_connection_error(internal_bad_host)
        )
        for host in self.cluster.all_internal_hosts():
            self.assertTrue('Deploying configuration on: %s' % host in output)
        expected_size = self.len_down_node_error + \
            len(self.cluster.all_hosts())
        self.assertEqual(len(output.splitlines()), expected_size)

        output = self.run_prestoadmin('configuration show config',
                                      raise_error=False)
        self.assertRegexpMatches(
            output,
            self.down_node_connection_error(internal_bad_host)
        )
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_down_node.txt'), 'r') as f:
            expected = f.read()
        self.assertRegexpMatches(str.join('\n', output.splitlines()[6:]),
                                 expected)

    def test_deploy_lost_worker_connection(self):
        self.upload_topology()
        internal_bad_host = self.cluster.internal_slaves[0]
        bad_host = self.cluster.slaves[0]
        self.cluster.stop_host(bad_host)
        output = self.run_prestoadmin('configuration deploy',
                                      raise_error=False)
        self.assertRegexpMatches(
            output,
            self.down_node_connection_error(internal_bad_host)
        )
        for host in self.cluster.all_internal_hosts():
            self.assertTrue('Deploying configuration on: %s' % host in output)
        expected_length = len(self.cluster.all_hosts()) + \
            self.len_down_node_error
        self.assertEqual(len(output.splitlines()), expected_length)

    def test_configuration_show(self):
        self.upload_topology()

        # configuration show no configuration
        output = self.run_prestoadmin('configuration show')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_none.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(expected, output)

        self.run_prestoadmin('configuration deploy')

        # configuration show default configuration
        output = self.run_prestoadmin('configuration show')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_default.txt'), 'r') as f:
            expected = f.read()
        self.assertRegexpMatches(output, expected)

        # configuration show node
        output = self.run_prestoadmin('configuration show node')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_node.txt'), 'r') as f:
            expected = f.read()
        self.assertRegexpMatches(output, expected)

        # configuration show jvm
        output = self.run_prestoadmin('configuration show jvm')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_jvm.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(output, expected)

        # configuration show config
        output = self.run_prestoadmin('configuration show config')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_config.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(output, expected)

        # configuration show log no log.properties
        output = self.run_prestoadmin('configuration show log')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_log_none.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(output, expected)

        # configuration show log has log.properties
        log_properties = 'com.facebook.presto=WARN'
        filename = 'log.properties'
        self.cluster.write_content_to_host(
            log_properties,
            os.path.join(constants.WORKERS_DIR, filename),
            self.cluster.master
        )
        self.cluster.write_content_to_host(
            log_properties,
            os.path.join(constants.COORDINATOR_DIR, filename),
            self.cluster.master
        )
        self.run_prestoadmin('configuration deploy')

        output = self.run_prestoadmin('configuration show log')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_log.txt'), 'r') as f:
            expected = f.read()
        self.assertEqual(output, expected)

    def test_configuration_show_coord_worker_using_dash_h(self):
        self.upload_topology()

        self.run_prestoadmin('configuration deploy')

        # show default configuration for master and slave1
        output = self.run_prestoadmin('configuration show '
                                      '-H %(master)s,%(slave1)s')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_default_master_slave1.txt'),
                  'r') as f:
            expected = f.read()
        self.assertRegexpMatches(output, expected)

    def test_configuration_show_coord_worker_using_dash_x(self):
        self.upload_topology()

        self.run_prestoadmin('configuration deploy')

        # show default configuration for all except master and slave1
        output = self.run_prestoadmin('configuration show '
                                      '-x %(master)s,%(slave1)s')
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'configuration_show_default_slave2_slave3.txt'),
                  'r') as f:
            expected = f.read()
        self.assertRegexpMatches(output, expected)
