import json
import os
import re
from prestoadmin.util import constants
from tests.product.base_product_case import BaseProductTestCase, \
    LOCAL_RESOURCES_DIR


class PrestoError(Exception):
    pass


class TestConnectors(BaseProductTestCase):
    def test_basic_connector_add_remove(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        self.run_prestoadmin('server start')
        for host in self.all_hosts():
            self.assert_has_default_connector(host)
        connectors = self.get_connector_info()
        self.assertEqual(connectors, [['system'], ['tpch']])

        self.run_prestoadmin('connector remove tpch')
        self.run_prestoadmin('server restart')
        self.assert_path_removed(self.master,
                                 os.path.join(constants.CONNECTORS_DIR,
                                              'tpch.properties'))
        self.assertEqual(self.get_connector_info(), [['system']])
        for host in self.all_hosts():
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tcph.properties'))

        self.write_content_to_master('connector.name=tpch',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'tpch.properties'))
        self.run_prestoadmin('connector add')
        self.run_prestoadmin('server restart')
        for host in self.all_hosts():
            self.assert_has_default_connector(host)
        self.assertEqual([['system'], ['tpch']], self.get_connector_info())

    def test_connector_add(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()

        # test add connector without read permissions on file
        script = 'chmod 600 /etc/opt/prestoadmin/connectors/tpch.properties;' \
                 ' su app-admin -c "./presto-admin connector add tpch"'
        output = self.run_prestoadmin_script(script)
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                  'connector_permissions_warning.txt'), 'r') as f:
            expected = f.read()

        self.assertEqualIgnoringOrder(expected, output)

        # test add connector directory without read permissions on directory
        script = 'chmod 600 /etc/opt/prestoadmin/connectors; ' \
                 'su app-admin -c "./presto-admin connector add"'
        output = self.run_prestoadmin_script(script)
        permission_error = '\nWarning: [slave3] Permission denied\n\n\n' \
                           'Warning: [slave2] Permission denied\n\n\n' \
                           'Warning: [slave1] Permission denied\n\n\n' \
                           'Warning: [master] Permission denied\n\n'
        self.assertEqualIgnoringOrder(output, permission_error)

        # test add connector by file without read permissions on directory
        script = 'chmod 600 /etc/opt/prestoadmin/connectors; ' \
                 'su app-admin -c "./presto-admin connector add tpch"'
        output = self.run_prestoadmin_script(script)
        self.assert_parallel_execution_failure('connector.add',
                                               'Configuration for connector'
                                               ' tpch not found', output)

        # test add a connector that does not exist
        self.run_prestoadmin('connector remove tpch')
        output = self.run_prestoadmin('connector add tpch')
        self.assert_parallel_execution_failure(
            'connector.add', 'Configuration for connector tpch not found',
            output)

        # test add all connectors when the directory does not exist
        self.exec_create_start(self.master,
                               'rmdir /etc/opt/prestoadmin/connectors')
        output = self.run_prestoadmin('connector add')
        self.assert_parallel_execution_failure(
            'connector.add', 'Cannot add connectors because directory '
                             '/etc/opt/prestoadmin/connectors does not exist',
            output)

        # test add connector by name when it exists
        self.write_content_to_master('connector.name=tpch',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'tpch.properties'))
        self.run_prestoadmin('connector add tpch')
        self.run_prestoadmin('server start')
        for host in self.all_hosts():
            self.assert_has_default_connector(host)
        self.assertEqual([['system'], ['tpch']], self.get_connector_info())

        self.run_prestoadmin('connector remove tpch')
        self.run_prestoadmin('server restart')

        # test add connectors where directory is empty
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

        # test add connectors from directory with more than one connector
        self.write_content_to_master('connector.name=tpch',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'tpch.properties'))
        self.write_content_to_master('connector.name=jmx',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'jmx.properties'))
        self.run_prestoadmin('connector add')
        self.run_prestoadmin('server restart')
        for host in self.all_hosts():
            self.assert_has_default_connector(host)
            self.assert_file_content(host,
                                     '/etc/presto/catalog/jmx.properties',
                                     'connector.name=jmx')
        self.assertEqual([['system'], ['jmx'], ['tpch']],
                         self.get_connector_info())

    def test_connector_add_lost_host(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        self.run_prestoadmin('connector remove tpch')

        self.stop_and_wait(self.slaves[0])
        self.write_content_to_master('connector.name=tpch',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'tpch.properties'))
        output = self.run_prestoadmin('connector add tpch')
        with open(os.path.join(LOCAL_RESOURCES_DIR, 'connector_lost_host.txt'),
                  'r') as f:
            expected = f.read()

        output = re.sub(r'Traceback.*raise NetworkError\(msg, e\)\n', '',
                        output, flags=re.DOTALL)
        for actual, expected in zip(
                sorted(output.splitlines()), sorted(expected.splitlines())):
            self.assertRegexpMatches(actual, expected)
        self.run_prestoadmin('server start')

        for host in [self.master, self.slaves[1], self.slaves[2]]:
            self.assert_has_default_connector(host)
        self.assertEqual([['system'], ['tpch']], self.get_connector_info())

    def test_connector_remove(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        for host in self.all_hosts():
            self.assert_has_default_connector(host)

        missing_connector_message = """
Warning: [slave1] Could not remove connector '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'


Warning: [master] Could not remove connector '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'


Warning: [slave3] Could not remove connector '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'


Warning: [slave2] Could not remove connector '%(name)s'. No such file \
'/etc/presto/catalog/%(name)s.properties'

"""

        success_message = """[master] Connector removed. Restart the server \
for the change to take effect
[slave1] Connector removed. Restart the server for the change to take effect
[slave2] Connector removed. Restart the server for the change to take effect
[slave3] Connector removed. Restart the server for the change to take effect"""

        # test remove connector does not exist
        output = self.run_prestoadmin('connector remove jmx')
        self.assertEqualIgnoringOrder(
            missing_connector_message % {'name': 'jmx'},
            output)

        # test remove connector not in directory, but in presto
        self.exec_create_start(self.master,
                               'rm /etc/opt/prestoadmin/connectors/'
                               'tpch.properties')

        output = self.run_prestoadmin('connector remove tpch')
        self.assertEqualIgnoringOrder(success_message, output)

        # test remove connector in directory but not in presto
        self.write_content_to_master('connector.name=tpch',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'tpch.properties'))
        output = self.run_prestoadmin('connector remove tpch')
        self.assertEqualIgnoringOrder(
            missing_connector_message % {'name': 'tpch'},
            output)

    def get_connector_info(self):
        output = self.exec_create_start(
            self.master,
            "curl --silent -X POST http://localhost:8080/v1/statement -H "
            "'X-Presto-User:$USER' -H 'X-Presto-Schema:metadata' -H "
            "'X-Presto-Catalog:system' -d 'select catalog_name from catalogs'")

        data = self.get_key_value(output, 'data')
        next_uri = self.get_key_value(output, 'nextUri')
        while not data and next_uri:
            output = self.exec_create_start(self.master,
                                            'curl --silent %s'
                                            % self.get_key_value(output,
                                                                 'nextUri'))
            data = self.get_key_value(output, 'data')
            next_uri = self.get_key_value(output, 'nextUri')

        if not data:
            raise PrestoError('Could not get catalogs from json output')

        return data

    def get_key_value(self, text, key):
        try:
            return json.loads(text)[key]
        except KeyError:
            return ''
        except ValueError as e:
            raise ValueError(e.message + '\n' + text)
