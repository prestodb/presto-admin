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
Product tests for presto-admin connector support.
"""
import json
import os

from nose.plugins.attrib import attr

from prestoadmin.util import constants
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, \
    docker_only, PrestoError
from tests.product.constants import LOCAL_RESOURCES_DIR
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestConnectors(BaseProductTestCase):
    def setup_cluster_assert_connectors(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        for host in self.cluster.all_hosts():
            self.assert_has_default_connector(host)

        self._assert_connectors_loaded([['system'], ['tpch']])

    @attr('smoketest')
    def test_basic_connector_add_remove(self):
        self.setup_cluster_assert_connectors()

        self.run_prestoadmin('connector remove tpch')
        self.run_prestoadmin('server restart')
        self.assert_path_removed(self.cluster.master,
                                 os.path.join(constants.CONNECTORS_DIR,
                                              'tpch.properties'))
        self._assert_connectors_loaded([['system']])
        for host in self.cluster.all_hosts():
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tcph.properties'))

        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(constants.CONNECTORS_DIR, 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add')
        self.run_prestoadmin('server restart')
        for host in self.cluster.all_hosts():
            self.assert_has_default_connector(host)
        self._assert_connectors_loaded([['system'], ['tpch']])

    def test_connector_add_remove_coord_worker_using_dash_h(self):
        self.setup_cluster_assert_connectors()

        self.run_prestoadmin('connector remove tpch -H %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self.assert_path_removed(self.cluster.master,
                                 os.path.join(constants.CONNECTORS_DIR,
                                              'tpch.properties'))
        self._assert_connectors_loaded([['system']])
        for host in [self.cluster.master, self.cluster.slaves[0]]:
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tpch.properties'))
        self.assert_has_default_connector(self.cluster.slaves[1])
        self.assert_has_default_connector(self.cluster.slaves[2])

        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(constants.CONNECTORS_DIR, 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add tpch -H %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self.assert_has_default_connector(self.cluster.master)
        self.assert_has_default_connector(self.cluster.slaves[1])

    def test_connector_add_remove_coord_worker_using_dash_x(self):
        self.setup_cluster_assert_connectors()

        self.run_prestoadmin('connector remove tpch -x %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self._assert_connectors_loaded([['system'], ['tpch']])
        self.assert_has_default_connector(self.cluster.master)
        self.assert_has_default_connector(self.cluster.slaves[0])
        for host in [self.cluster.slaves[1], self.cluster.slaves[2]]:
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tpch.properties'))

        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(constants.CONNECTORS_DIR, 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add tpch -x %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self._assert_connectors_loaded([['system'], ['tpch']])
        for slave in [self.cluster.slaves[1], self.cluster.slaves[2]]:
            self.assert_has_default_connector(slave)

    @docker_only
    def test_connector_add_wrong_permissions(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)

        # test add connector without read permissions on file
        script = 'chmod 600 /etc/opt/prestoadmin/connectors/tpch.properties;' \
                 ' su app-admin -c "./presto-admin connector add tpch"'
        output = self.run_script_from_prestoadmin_dir(script)
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'connector_permissions_warning.txt'), 'r') as f:
            expected = f.read() % \
                {'master': self.cluster.internal_master,
                 'slave1': self.cluster.internal_slaves[0],
                 'slave2': self.cluster.internal_slaves[1],
                 'slave3': self.cluster.internal_slaves[2]}

        self.assertEqualIgnoringOrder(expected, output)

        # test add connector directory without read permissions on directory
        script = 'chmod 600 /etc/opt/prestoadmin/connectors; ' \
                 'su app-admin -c "./presto-admin connector add"'
        output = self.run_script_from_prestoadmin_dir(script)
        permission_error = '\nWarning: [slave3] Permission denied\n\n\n' \
                           'Warning: [slave2] Permission denied\n\n\n' \
                           'Warning: [slave1] Permission denied\n\n\n' \
                           'Warning: [master] Permission denied\n\n'
        self.assertEqualIgnoringOrder(output, permission_error)

        # test add connector by file without read permissions on directory
        script = 'chmod 600 /etc/opt/prestoadmin/connectors; ' \
                 'su app-admin -c "./presto-admin connector add tpch"'
        not_found_error = self.fatal_error(
            'Configuration for connector tpch not found')
        self.assertRaisesRegexp(OSError, not_found_error,
                                self.run_script_from_prestoadmin_dir, script)

    def test_connector_add_missing_connector(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)

        # test add a connector that does not exist
        not_found_error = self.fatal_error(
            'Configuration for connector tpch not found')
        self.run_prestoadmin('connector remove tpch')
        self.assertRaisesRegexp(OSError, not_found_error,
                                self.run_prestoadmin, 'connector add tpch')

    def test_connector_add_no_dir(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        # test add all connectors when the directory does not exist
        self.cluster.exec_cmd_on_host(
            self.cluster.master,
            'rm -r /etc/opt/prestoadmin/connectors')
        missing_dir_error = self.fatal_error('Cannot add connectors '
                                             'because directory /etc/'
                                             'opt/prestoadmin/connectors '
                                             'does not exist')
        self.assertRaisesRegexp(OSError, missing_dir_error,
                                self.run_prestoadmin, 'connector add')

    def test_connector_add_by_name(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('connector remove tpch')

        # test add connector by name when it exists
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(constants.CONNECTORS_DIR, 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add tpch')
        self.run_prestoadmin('server start')
        for host in self.cluster.all_hosts():
            self.assert_has_default_connector(host)
        self._assert_connectors_loaded([['system'], ['tpch']])

    def test_connector_add_empty_dir(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        output = self.run_prestoadmin('connector remove tpch')
        output = self.run_prestoadmin('connector add')
        expected = """
Warning: [slave3] Directory /etc/opt/prestoadmin/connectors is empty. \
No connectors will be deployed


Warning: [slave2] Directory /etc/opt/prestoadmin/connectors is empty. \
No connectors will be deployed


Warning: [slave1] Directory /etc/opt/prestoadmin/connectors is empty. \
No connectors will be deployed


Warning: [master] Directory /etc/opt/prestoadmin/connectors is empty. \
No connectors will be deployed

"""
        self.assertEqualIgnoringOrder(expected, output)

    def test_connector_add_two_connectors(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('connector remove tpch')

        # test add connectors from directory with more than one connector
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(constants.CONNECTORS_DIR, 'tpch.properties'),
            self.cluster.master
        )
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(constants.CONNECTORS_DIR, 'jmx.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add')
        self.run_prestoadmin('server start')
        for host in self.cluster.all_hosts():
            self.assert_has_default_connector(host)
            self.assert_file_content(host,
                                     '/etc/presto/catalog/jmx.properties',
                                     'connector.name=jmx')
        self._assert_connectors_loaded([['system'], ['jmx'], ['tpch']])

    def fatal_error(self, error):
        message = """
Fatal error: %(error)s

Underlying exception:
    %(error)s

Aborting.
"""
        return message % {'error': error}

    def test_connector_add_lost_host(self):
        installer = StandalonePrestoInstaller(self)
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)
        self.upload_topology()
        installer.install()
        self.run_prestoadmin('connector remove tpch')

        self.cluster.stop_host(
            self.cluster.slaves[0])
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(constants.CONNECTORS_DIR, 'tpch.properties'),
            self.cluster.master
        )
        output = self.run_prestoadmin('connector add tpch', raise_error=False)
        for host in self.cluster.all_internal_hosts():
            deploying_message = 'Deploying tpch.properties connector ' \
                                'configurations on: %s'
            self.assertTrue(deploying_message % host in output,
                            'expected %s \n actual %s'
                            % (deploying_message % host, output))
        self.assertRegexpMatches(
            output,
            self.down_node_connection_error(self.cluster.internal_slaves[0])
        )
        self.assertEqual(len(output.splitlines()),
                         len(self.cluster.all_hosts()) +
                         self.len_down_node_error)
        self.run_prestoadmin('server start', raise_error=False)

        for host in [self.cluster.master,
                     self.cluster.slaves[1],
                     self.cluster.slaves[2]]:
            self.assert_has_default_connector(host)
        self._assert_connectors_loaded([['system'], ['tpch']])

    def test_connector_remove(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        for host in self.cluster.all_hosts():
            self.assert_has_default_connector(host)

        missing_connector_message = """[Errno 1] 
Fatal error: [master] Could not remove connector '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

Aborting.

Fatal error: [slave1] Could not remove connector '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

Aborting.

Fatal error: [slave2] Could not remove connector '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

Aborting.

Fatal error: [slave3] Could not remove connector '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

Aborting.
"""  # noqa

        success_message = """[master] Connector removed. Restart the server \
for the change to take effect
[slave1] Connector removed. Restart the server for the change to take effect
[slave2] Connector removed. Restart the server for the change to take effect
[slave3] Connector removed. Restart the server for the change to take effect"""

        # test remove connector does not exist
        # expect error

        self.assertRaisesMessageIgnoringOrder(
            OSError,
            missing_connector_message % {'name': 'jmx'},
            self.run_prestoadmin,
            'connector remove jmx')

        # test remove connector not in directory, but in presto
        self.cluster.exec_cmd_on_host(
            self.cluster.master,
            'rm /etc/opt/prestoadmin/connectors/tpch.properties'
        )

        output = self.run_prestoadmin('connector remove tpch')
        self.assertEqualIgnoringOrder(success_message, output)

        # test remove connector in directory but not in presto
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(constants.CONNECTORS_DIR, 'tpch.properties'),
            self.cluster.master
        )

        self.assertRaisesMessageIgnoringOrder(
            OSError,
            missing_connector_message % {'name': 'tpch'},
            self.run_prestoadmin,
            'connector remove tpch')

    def test_connector_name_not_found(self):
        self.setup_cluster(NoHadoopBareImageProvider(),
                           self.STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')

        self.cluster.write_content_to_host(
            'connector.noname=example',
            os.path.join(constants.CONNECTORS_DIR, 'example.properties'),
            self.cluster.master
        )

        expected = self.fatal_error('Catalog configuration '
                                    'example.properties does not '
                                    'contain connector.name')
        self.assertRaisesRegexp(OSError, expected, self.run_prestoadmin,
                                'connector add example')

    def get_connector_info(self):
        output = self.cluster.exec_cmd_on_host(
            self.cluster.master,
            "curl --silent -X POST http://localhost:8080/v1/statement -H "
            "'X-Presto-User:$USER' -H 'X-Presto-Schema:metadata' -H "
            "'X-Presto-Catalog:system' -d 'select catalog_name from catalogs'")

        data = self.get_key_value(output, 'data')
        next_uri = self.get_key_value(output, 'nextUri')
        while not data and next_uri:
            output = self.cluster.exec_cmd_on_host(
                self.cluster.master,
                'curl --silent %s' % self.get_key_value(output, 'nextUri')
            )
            data = self.get_key_value(output, 'data')
            next_uri = self.get_key_value(output, 'nextUri')

        if not data:
            raise PrestoError('Could not get catalogs from json output. '
                              'Output was: \n%s' % output)

        return data

    def get_key_value(self, text, key):
        try:
            return json.loads(text)[key]
        except KeyError:
            return ''
        except ValueError as e:
            raise ValueError(e.message + '\n' + text)

    # Presto will be 'query-able' before it has loaded all of its
    # connectors. When presto-admin restarts presto it returns when it
    # can query the server but that doesn't mean that all connectors
    # have been loaded. Thus in order to verify that connectors get
    # correctly added we check continuously within a timeout.
    def _assert_connectors_loaded(self, expected_connectors):
        self.retry(lambda: self.assertEqual(expected_connectors,
                                            self.get_connector_info()))
