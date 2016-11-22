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

from prestoadmin.standalone.config import PRESTO_STANDALONE_USER
from prestoadmin.util import constants
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, \
    PrestoError
from tests.product.cluster_types import STANDALONE_PRESTO_CLUSTER, STANDALONE_PA_CLUSTER
from tests.product.config_dir_utils import get_connectors_directory
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestConnectors(BaseProductTestCase):
    def setUp(self):
        super(TestConnectors, self).setUp()

    def setup_cluster_assert_connectors(self):
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        for host in self.cluster.all_hosts():
            self.assert_has_default_connector(host)

        self._assert_connectors_loaded([['system'], ['tpch']])

    @attr('smoketest')
    def test_connector_add_remove(self):
        self.setup_cluster_assert_connectors()
        self.run_prestoadmin('connector remove tpch')
        self.assert_path_removed(self.cluster.master, os.path.join(get_connectors_directory(), 'tpch.properties'))
        for host in self.cluster.all_hosts():
            self.assert_path_removed(host, os.path.join(constants.REMOTE_CATALOG_DIR, 'tpch.properties'))

        # test add connectors from directory with more than one connector
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_connectors_directory(), 'tpch.properties'),
            self.cluster.master
        )
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(get_connectors_directory(), 'jmx.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add')
        self.run_prestoadmin('server restart')
        for host in self.cluster.all_hosts():
            filepath = '/etc/presto/catalog/jmx.properties'
            self.assert_has_default_connector(host)
            self.assert_connector_perms(host, filepath)
            self.assert_file_content(host, filepath, 'connector.name=jmx')
        self._assert_connectors_loaded([['system'], ['jmx'], ['tpch']])

    def test_connector_add_remove_coord_worker_using_dash_h(self):
        self.setup_cluster_assert_connectors()

        self.run_prestoadmin('connector remove tpch -H %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self.assert_path_removed(self.cluster.master,
                                 os.path.join(get_connectors_directory(),
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
            os.path.join(get_connectors_directory(), 'tpch.properties'),
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
            os.path.join(get_connectors_directory(), 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add tpch -x %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self._assert_connectors_loaded([['system'], ['tpch']])
        for slave in [self.cluster.slaves[1], self.cluster.slaves[2]]:
            self.assert_has_default_connector(slave)

    def test_connector_add_by_name(self):
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('connector remove tpch')

        # test add connector by name when it exists
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_connectors_directory(), 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add tpch')
        self.run_prestoadmin('server start')
        for host in self.cluster.all_hosts():
            self.assert_has_default_connector(host)
        self._assert_connectors_loaded([['system'], ['tpch']])

    def test_connector_add_empty_dir(self):
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('connector remove tpch')
        output = self.run_prestoadmin('connector add')
        expected = [r'',
                    r'Warning: \[slave3\] Directory .*/.prestoadmin/connectors is empty. '
                    r'No connectors will be deployed',
                    r'',
                    r'',
                    r'Warning: \[slave2\] Directory .*/.prestoadmin/connectors is empty. '
                    r'No connectors will be deployed',
                    r'',
                    r'',
                    r'Warning: \[slave1\] Directory .*/.prestoadmin/connectors is empty. '
                    r'No connectors will be deployed',
                    r'',
                    r'',
                    r'Warning: \[master\] Directory .*/.prestoadmin/connectors is empty. '
                    r'No connectors will be deployed',
                    r'']
        self.assertRegexpMatchesLineByLine(output.splitlines(), expected)

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
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PA_CLUSTER)
        self.upload_topology()
        installer.install()
        self.run_prestoadmin('connector remove tpch')

        self.cluster.stop_host(
            self.cluster.slaves[0])
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_connectors_directory(), 'tpch.properties'),
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
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PRESTO_CLUSTER)
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
            'rm %s' % os.path.join(get_connectors_directory(), 'tpch.properties')
        )

        output = self.run_prestoadmin('connector remove tpch')
        self.assertEqualIgnoringOrder(success_message, output)

        # test remove connector in directory but not in presto
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_connectors_directory(), 'tpch.properties'),
            self.cluster.master
        )

        self.assertRaisesMessageIgnoringOrder(
            OSError,
            missing_connector_message % {'name': 'tpch'},
            self.run_prestoadmin,
            'connector remove tpch')

    def test_connector_add_no_presto_user(self):
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PRESTO_CLUSTER)

        for host in self.cluster.all_hosts():
            self.cluster.exec_cmd_on_host(
                host, "userdel %s" % (PRESTO_STANDALONE_USER,), invoke_sudo=True)

        self.assertRaisesRegexp(
            OSError, "User presto does not exist", self.run_prestoadmin,
            'connector add tpch')

    def get_connector_info(self):
        output = self.cluster.exec_cmd_on_host(
            self.cluster.master,
            "curl --silent -X POST http://localhost:8080/v1/statement -H "
            "'X-Presto-User:$USER' -H 'X-Presto-Schema:metadata' -H "
            "'X-Presto-Catalog:system' -H 'X-Presto-Source:presto-admin' "
            "-d 'select catalog_name from catalogs'")

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

    def test_connector_add_remove_non_sudo_user(self):
        self.setup_cluster_assert_connectors()
        self.upload_topology(
            {"coordinator": "master",
             "workers": ["slave1", "slave2", "slave3"],
             "username": "app-admin"}
        )

        self.run_prestoadmin('connector remove tpch -p password')
        self.assert_path_removed(self.cluster.master,
                                 os.path.join(get_connectors_directory(),
                                              'tpch.properties'))
        for host in self.cluster.all_hosts():
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tcph.properties'))

        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(get_connectors_directory(), 'jmx.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('connector add -p password')
        self.run_prestoadmin('server restart -p password')
        for host in self.cluster.all_hosts():
            self.assert_has_jmx_connector(host)
        self._assert_connectors_loaded([['system'], ['jmx']])
