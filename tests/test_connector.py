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
tests for connector module
"""
from mock import patch
from prestoadmin.util import constants
import utils
from prestoadmin import configuration, connector


class TestConnector(utils.BaseTestCase):
    @patch("prestoadmin.connector.os.path.isfile")
    def test_add_not_exist(self, isfile_mock):
        isfile_mock.return_value = False
        self.assertRaisesRegexp(configuration.ConfigurationError,
                                "Configuration for connector dummy not found",
                                connector.add, "dummy")

    @patch("prestoadmin.connector.configure.deploy")
    @patch("prestoadmin.connector.os.path.isfile")
    def test_add_exists(self, isfile_mock, deploy_mock):
        isfile_mock.return_value = True
        connector.add("tpch")
        deploy_mock.assert_called_with(["tpch.properties"],
                                       constants.CONNECTORS_DIR,
                                       constants.REMOTE_CATALOG_DIR)

    @patch("prestoadmin.connector.configure.deploy")
    @patch("prestoadmin.connector.os.listdir")
    def test_add_all(self, listdir_mock, deploy_mock):
        catalogs = ["tpch.properties", "another.properties"]
        listdir_mock.return_value = catalogs
        connector.add()
        deploy_mock.assert_called_with(catalogs,
                                       constants.CONNECTORS_DIR,
                                       constants.REMOTE_CATALOG_DIR)

    @patch("prestoadmin.connector.remove_file")
    def test_remove(self, remove_mock):
        connector.remove("tpch")
        remove_mock.assert_called_with(constants.REMOTE_CATALOG_DIR +
                                       "/tpch.properties")
