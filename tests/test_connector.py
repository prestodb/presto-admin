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
import fabric.api
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
    @patch("prestoadmin.connector.os.path.isdir")
    @patch("prestoadmin.connector.os.listdir")
    def test_add_all(self, listdir_mock, isdir_mock, deploy_mock):
        catalogs = ["tpch.properties", "another.properties"]
        listdir_mock.return_value = catalogs
        connector.add()
        deploy_mock.assert_called_with(catalogs,
                                       constants.CONNECTORS_DIR,
                                       constants.REMOTE_CATALOG_DIR)

    @patch("prestoadmin.connector.configure.deploy")
    @patch("prestoadmin.connector.os.path.isdir")
    def test_add_all_fails_if_dir_not_there(self, isdir_mock, deploy_mock):
        isdir_mock.return_value = False
        self.assertRaisesRegexp(configuration.ConfigFileNotFoundError,
                                r"Cannot add connectors because directory .+"
                                r" does not exist",
                                connector.add)
        self.assertFalse(deploy_mock.called)

    @patch("prestoadmin.connector.sudo")
    @patch("prestoadmin.connector.os.path.exists")
    @patch("prestoadmin.connector.os.remove")
    def test_remove(self, local_rm_mock, exists_mock, sudo_mock):
        exists_mock.return_value = True
        fabric.api.env.host = "localhost"
        connector.remove("tpch")
        sudo_mock.assert_called_with("rm " + constants.REMOTE_CATALOG_DIR +
                                     "/tpch.properties")
        local_rm_mock.assert_called_with(constants.CONNECTORS_DIR +
                                         "/tpch.properties")
