import json
import os
from prestoadmin.util import constants
from tests.product.base_product_case import BaseProductTestCase


class PrestoError(Exception):
    pass


class TestConnectors(BaseProductTestCase):
    def test_connector_add_remove(self):
        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        self.run_prestoadmin('server start')
        connectors = self.get_connector_info()
        self.assertEqual(connectors, [['system'], ['tpch']])
        for host in self.all_hosts():
            self.assert_has_default_connector(host)

        self.run_prestoadmin('connector remove tpch')
        self.run_prestoadmin('server restart')
        self.assertEqual(self.get_connector_info(), [['system']])
        self.assert_path_removed(self.master,
                                 os.path.join(constants.CONNECTORS_DIR,
                                              'tpch.properties'))
        for host in self.all_hosts():
            self.assert_path_removed(host,
                                     os.path.join(constants.REMOTE_CATALOG_DIR,
                                                  'tcph.properties'))

        self.write_content_to_master('connector.name=tpch',
                                     os.path.join(constants.CONNECTORS_DIR,
                                                  'tpch.properties'))
        self.run_prestoadmin('connector add')
        self.run_prestoadmin('server restart')
        self.assertEqual([['system'], ['tpch']], self.get_connector_info())
        for host in self.all_hosts():
            self.assert_has_default_connector(host)

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
