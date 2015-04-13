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
    @patch("prestoadmin.connector.configuration.get_conf_from_file")
    def test_add_not_exist(self, get_conf_mock):
        get_conf_mock.return_value = {}
        self.assertRaisesRegexp(configuration.ConfigurationError,
                                "Configuration for connector dummy not found",
                                connector.add, "dummy")

    @patch("prestoadmin.connector.configure.configure")
    @patch("prestoadmin.connector.configuration.get_conf_from_file")
    def test_add_exists(self, get_conf_mock, configure_mock):
        connector_conf = {"tpch.properties": {"connector.name": "tpch"},
                          "another.properties": {"connector.name": "another"}}
        get_conf_mock.return_value = connector_conf
        tpch_conf = {"tpch.properties": {"connector.name": "tpch"}}
        connector.add("tpch")
        configure_mock.assert_called_with(tpch_conf, constants.TMP_CONF_DIR,
                                          constants.REMOTE_CATALOG_DIR)

    @patch("prestoadmin.connector.configure.configure")
    @patch("prestoadmin.connector.configuration.get_conf_from_file")
    def test_add_all(self, get_conf_mock, configure_mock):
        connector_conf = {"tpch.properties": {"connector.name": "tpch"},
                          "another.properties": {"connector.name": "another"}}
        get_conf_mock.return_value = connector_conf
        connector.add()
        configure_mock.assert_called_with(connector_conf,
                                          constants.TMP_CONF_DIR,
                                          constants.REMOTE_CATALOG_DIR)

    @patch("prestoadmin.connector.remove_file")
    def test_remove(self, remove_mock):
        connector.remove("tpch")
        remove_mock.assert_called_with(constants.REMOTE_CATALOG_DIR +
                                       "/tpch.properties")
