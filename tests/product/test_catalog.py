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
Product tests for presto-admin catalog support.
"""
import os

from nose.plugins.attrib import attr

from prestoadmin.standalone.config import PRESTO_STANDALONE_USER
from prestoadmin.util import constants
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_PRESTO_CLUSTER, STANDALONE_PA_CLUSTER
from tests.product.config_dir_utils import get_catalog_directory
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestCatalog(BaseProductTestCase):
    def setUp(self):
        super(TestCatalog, self).setUp()

    def setup_cluster_assert_catalogs(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        for host in self.cluster.all_hosts():
            self.assert_has_default_catalog(host)

        self._assert_catalogs_loaded([['system'], ['tpch']])

    @attr('smoketest')
    def test_catalog_add_remove(self):
        self.setup_cluster_assert_catalogs()
        self.run_prestoadmin('catalog remove tpch')
        self.assert_path_removed(self.cluster.master, os.path.join(get_catalog_directory(), 'tpch.properties'))
        for host in self.cluster.all_hosts():
            self.assert_path_removed(host, os.path.join(constants.REMOTE_CATALOG_DIR, 'tpch.properties'))

        # test add catalogs from directory with more than one catalog
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_catalog_directory(), 'tpch.properties'),
            self.cluster.master
        )
        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(get_catalog_directory(), 'jmx.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('catalog add')
        self.run_prestoadmin('server restart')
        for host in self.cluster.all_hosts():
            filepath = '/etc/presto/catalog/jmx.properties'
            self.assert_has_default_catalog(host)
            self.assert_config_perms(host, filepath)
            self.assert_file_content(host, filepath, 'connector.name=jmx')
        self._assert_catalogs_loaded([['system'], ['jmx'], ['tpch']])

    def test_catalog_add_remove_coord_worker_using_dash_h(self):
        self.setup_cluster_assert_catalogs()

        self.run_prestoadmin('catalog remove tpch -H %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self.assert_path_removed(self.cluster.master,
                                 os.path.join(get_catalog_directory(),
                                              'tpch.properties'))
        self._assert_catalogs_loaded([['system']])
        for host in [self.cluster.master, self.cluster.slaves[0]]:
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tpch.properties'))
        self.assert_has_default_catalog(self.cluster.slaves[1])
        self.assert_has_default_catalog(self.cluster.slaves[2])

        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_catalog_directory(), 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('catalog add tpch -H %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self.assert_has_default_catalog(self.cluster.master)
        self.assert_has_default_catalog(self.cluster.slaves[1])

    def test_catalog_add_remove_coord_worker_using_dash_x(self):
        self.setup_cluster_assert_catalogs()

        self.run_prestoadmin('catalog remove tpch -x %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self._assert_catalogs_loaded([['system'], ['tpch']])
        self.assert_has_default_catalog(self.cluster.master)
        self.assert_has_default_catalog(self.cluster.slaves[0])
        for host in [self.cluster.slaves[1], self.cluster.slaves[2]]:
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tpch.properties'))

        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_catalog_directory(), 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('catalog add tpch -x %(master)s,%(slave1)s')
        self.run_prestoadmin('server restart')
        self._assert_catalogs_loaded([['system'], ['tpch']])
        for slave in [self.cluster.slaves[1], self.cluster.slaves[2]]:
            self.assert_has_default_catalog(slave)

    def test_catalog_add_by_name(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('catalog remove tpch')

        # test add catalog by name when it exists
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_catalog_directory(), 'tpch.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('catalog add tpch')
        self.run_prestoadmin('server start')
        for host in self.cluster.all_hosts():
            self.assert_has_default_catalog(host)
        self._assert_catalogs_loaded([['system'], ['tpch']])

    def test_catalog_add_empty_dir(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('catalog remove tpch')
        output = self.run_prestoadmin('catalog add')
        expected = [r'',
                    r'Warning: \[slave3\] Directory .*/.prestoadmin/catalog is empty. '
                    r'No catalogs will be deployed',
                    r'',
                    r'',
                    r'Warning: \[slave2\] Directory .*/.prestoadmin/catalog is empty. '
                    r'No catalogs will be deployed',
                    r'',
                    r'',
                    r'Warning: \[slave1\] Directory .*/.prestoadmin/catalog is empty. '
                    r'No catalogs will be deployed',
                    r'',
                    r'',
                    r'Warning: \[master\] Directory .*/.prestoadmin/catalog is empty. '
                    r'No catalogs will be deployed',
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

    def test_catalog_add_lost_host(self):
        installer = StandalonePrestoInstaller(self)
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)
        self.upload_topology()
        installer.install()
        self.run_prestoadmin('catalog remove tpch')

        self.cluster.stop_host(
            self.cluster.slaves[0])
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_catalog_directory(), 'tpch.properties'),
            self.cluster.master
        )
        output = self.run_prestoadmin('catalog add tpch', raise_error=False)
        for host in self.cluster.all_internal_hosts():
            deploying_message = 'Deploying tpch.properties catalog configurations on: %s'
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
            self.assert_has_default_catalog(host)
        self._assert_catalogs_loaded([['system'], ['tpch']])

    def test_catalog_remove(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        for host in self.cluster.all_hosts():
            self.assert_has_default_catalog(host)

        missing_catalog_message = """[Errno 1]
Fatal error: [master] Could not remove catalog '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

Aborting.

Fatal error: [slave1] Could not remove catalog '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

Aborting.

Fatal error: [slave2] Could not remove catalog '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

Aborting.

Fatal error: [slave3] Could not remove catalog '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

Aborting.
"""  # noqa

        success_message = """[master] Catalog removed. Restart the server \
for the change to take effect
[slave1] Catalog removed. Restart the server for the change to take effect
[slave2] Catalog removed. Restart the server for the change to take effect
[slave3] Catalog removed. Restart the server for the change to take effect"""

        # test remove catalog does not exist
        # expect error

        self.assertRaisesMessageIgnoringOrder(
            OSError,
            missing_catalog_message % {'name': 'jmx'},
            self.run_prestoadmin,
            'catalog remove jmx')

        # test remove catalog not in directory, but in presto
        self.cluster.exec_cmd_on_host(
            self.cluster.master,
            'rm %s' % os.path.join(get_catalog_directory(), 'tpch.properties')
        )

        output = self.run_prestoadmin('catalog remove tpch')
        self.assertEqualIgnoringOrder(success_message, output)

        # test remove catalog in directory but not in presto
        self.cluster.write_content_to_host(
            'connector.name=tpch',
            os.path.join(get_catalog_directory(), 'tpch.properties'),
            self.cluster.master
        )

        self.assertRaisesMessageIgnoringOrder(
            OSError,
            missing_catalog_message % {'name': 'tpch'},
            self.run_prestoadmin,
            'catalog remove tpch')

    def test_catalog_add_no_presto_user(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)

        for host in self.cluster.all_hosts():
            self.cluster.exec_cmd_on_host(
                host, "userdel %s" % (PRESTO_STANDALONE_USER,), invoke_sudo=True)

        self.assertRaisesRegexp(
            OSError, "User presto does not exist", self.run_prestoadmin,
            'catalog add tpch')

    def get_catalog_info(self):
        client = self.create_presto_client()
        return client.run_sql('select catalog_name from catalogs')

    # Presto will be 'query-able' before it has loaded all of its
    # catalogs. When presto-admin restarts presto it returns when it
    # can query the server but that doesn't mean that all catalogs
    # have been loaded. Thus in order to verify that catalogs get
    # correctly added we check continuously within a timeout.
    def _assert_catalogs_loaded(self, expected_catalogs):
        self.retry(lambda: self.assertEqual(expected_catalogs.sort(), self.get_catalog_info().sort()))

    def test_catalog_add_remove_non_sudo_user(self):
        self.setup_cluster_assert_catalogs()
        self.upload_topology(
            {"coordinator": "master",
             "workers": ["slave1", "slave2", "slave3"],
             "username": "app-admin"}
        )

        self.run_prestoadmin('catalog remove tpch -p password')
        self.assert_path_removed(self.cluster.master,
                                 os.path.join(get_catalog_directory(),
                                              'tpch.properties'))
        for host in self.cluster.all_hosts():
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tcph.properties'))

        self.cluster.write_content_to_host(
            'connector.name=jmx',
            os.path.join(get_catalog_directory(), 'jmx.properties'),
            self.cluster.master
        )
        self.run_prestoadmin('catalog add -p password')
        self.run_prestoadmin('server restart -p password')
        for host in self.cluster.all_hosts():
            self.assert_has_jmx_catalog(host)
        self._assert_catalogs_loaded([['system'], ['jmx']])
