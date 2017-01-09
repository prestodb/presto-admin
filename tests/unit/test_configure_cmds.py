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

import os
from fabric.state import env
from mock import patch
from prestoadmin.util import constants
from prestoadmin import configure_cmds
from tests.unit.base_unit_case import BaseUnitCase


class TestConfigureCmds(BaseUnitCase):
    @patch('prestoadmin.configure_cmds.get')
    @patch('prestoadmin.configure_cmds.files.exists')
    def test_config_show(self, mock_file_exists, mock_get):
        mock_file_exists.return_value = True

        configure_cmds.show("Node")
        file_path_node = os.path.join(constants.REMOTE_CONF_DIR,
                                      "node.properties")
        args, kwargs = mock_get.call_args
        self.assertEqual(args[0], file_path_node)

        configure_cmds.show("jvm")
        file_path_jvm = os.path.join(constants.REMOTE_CONF_DIR, "jvm.config")
        args, kwargs = mock_get.call_args
        self.assertEqual(args[0], file_path_jvm)

        configure_cmds.show("conFig")
        file_path_config = os.path.join(constants.REMOTE_CONF_DIR,
                                        "config.properties")
        args, kwargs = mock_get.call_args
        self.assertEqual(args[0], file_path_config)

    @patch('prestoadmin.configure_cmds.configuration_show')
    def test_config_show_all(self, mock_show):
        configure_cmds.show()
        mock_show.assert_any_call("node.properties")
        mock_show.assert_any_call("jvm.config")
        mock_show.assert_any_call("config.properties")
        mock_show.assert_any_call("log.properties", should_warn=False)

    @patch('prestoadmin.configure_cmds.abort')
    @patch('prestoadmin.configure_cmds.warn')
    @patch('prestoadmin.configure_cmds.files.exists')
    def test_config_show_fail(self, mock_file_exists, mock_warn, mock_abort):
        mock_file_exists.return_value = False
        env.host = "any_host"
        configure_cmds.configuration_show("any_path")
        file_path = os.path.join(constants.REMOTE_CONF_DIR, "any_path")
        mock_warn.assert_called_with("No configuration file found "
                                     "for %s at %s" % (env.host, file_path))

        configure_cmds.show("invalid_config")
        mock_abort.assert_called_with("Invalid Argument. Possible values: "
                                      "node, jvm, config, log")

    @patch('prestoadmin.configure_cmds.warn')
    @patch('prestoadmin.configure_cmds.files.exists')
    def test_config_show_fail_no_warn(self, mock_file_exists, mock_warn):
        mock_file_exists.return_value = False
        env.host = "any_host"
        configure_cmds.configuration_show("any_path", should_warn=False)
        self.assertFalse(mock_warn.called)

    @patch('prestoadmin.configure_cmds.abort')
    @patch('prestoadmin.deploy.workers')
    @patch('prestoadmin.deploy.coordinator')
    def test_config_deploy(self, mock_coordinator, mock_workers, mock_abort):
        env.host = "any_host"
        configure_cmds.deploy("invalid_config")
        mock_abort.assert_called_with("Invalid Argument. "
                                      "Possible values: coordinator, workers")

        configure_cmds.deploy()
        mock_workers.assert_called_with()
        mock_coordinator.assert_called_with()

    @patch('prestoadmin.deploy.workers')
    @patch('prestoadmin.deploy.coordinator')
    def test_config_deploy_coord(self, mock_coordinator, mock_workers):
        env.host = "any_host"
        configure_cmds.deploy("coordinator")
        mock_coordinator.assert_called_with()
        assert not mock_workers.called

    @patch('prestoadmin.deploy.workers')
    @patch('prestoadmin.deploy.coordinator')
    def test_config_deploy_workers(self, mock_coordinator, mock_workers):
        env.host = "any_host"
        configure_cmds.deploy("workers")
        mock_workers.assert_called_with()
        assert not mock_coordinator.called
