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

from prestoadmin.util.application import Application
import fabric.api
import fabric.operations
import fabric.utils

from mock import call
from mock import patch
from tests.utils import BaseTestCase

import logging

APPLICATION_NAME = 'foo'


@patch('prestoadmin.util.application.filesystem')
@patch('prestoadmin.util.application.logging.config')
class FabricPatchesTest(BaseTestCase):

    def setUp(self):
        # basicConfig is a noop if there are already handlers
        # present on the root logger, remove them all here
        self.__old_log_handlers = []
        for handler in logging.root.handlers:
            self.__old_log_handlers.append(handler)
            logging.root.removeHandler(handler)
        BaseTestCase.setUp(self)
        # Load prestoadmin so that the monkeypatching is in place

    def tearDown(self):
        # restore the old log handlers
        for handler in logging.root.handlers:
            logging.root.removeHandler(handler)
        for handler in self.__old_log_handlers:
            logging.root.addHandler(handler)
        BaseTestCase.tearDown(self)

    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_warn_api_prints_out_message(self, logger_mock, log_conf_mock,
                                         filesystem_mock):
        with Application(APPLICATION_NAME):
            fabric.api.warn("Test warning.")

        logger_mock.warn.assert_has_calls(
            [
                call('Test warning.\n\nNone\n'),
            ]
        )
        self.assertEqual(
            '\nWarning: Test warning.\n\n',
            self.test_stderr.getvalue()
        )

    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_warn_utils_prints_out_message(self, logger_mock, log_conf_mock,
                                           filesystem_mock):
        with Application(APPLICATION_NAME):
            fabric.utils.warn("Test warning.")

        logger_mock.warn.assert_has_calls(
            [
                call('Test warning.\n\nNone\n'),
                ]
        )
        self.assertEqual(
            '\nWarning: Test warning.\n\n',
            self.test_stderr.getvalue()
        )

    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_warn_utils_prints_out_message_with_host(self, logger_mock,
                                                     log_conf_mock, fs_mock):
        fabric.api.env.host = 'host'
        with Application(APPLICATION_NAME):
            fabric.utils.warn("Test warning.")

        logger_mock.warn.assert_has_calls(
            [
                call('[host] Test warning.\n\nNone\n'),
                ]
        )
        self.assertEqual(
            '\nWarning: [host] Test warning.\n\n',
            self.test_stderr.getvalue()
        )

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_run_api_logs_stdout(self, logger_mock, run_command_mock,
                                 logging_config_mock, filesystem_mock):
        self._execute_operation_test(run_command_mock, logger_mock,
                                     fabric.api.run)

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_run_op_logs_stdout(self, logger_mock, run_command_mock,
                                logging_config_mock, filesystem_mock):
        self._execute_operation_test(run_command_mock, logger_mock,
                                     fabric.operations.run)

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_sudo_api_logs_stdout(self, logger_mock, run_command_mock,
                                  logging_config_mock, filesystem_mock):
        self._execute_operation_test(run_command_mock, logger_mock,
                                     fabric.api.sudo)

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_sudo_op_logs_stdout(self, logger_mock, run_command_mock,
                                 logging_config_mock, filesystem_mock):
        self._execute_operation_test(run_command_mock, logger_mock,
                                     fabric.operations.sudo)

    def _execute_operation_test(self, run_command_mock, logger_mock, func):
        out = fabric.operations._AttributeString('Test warning')
        out.command = 'echo "Test warning"'
        out.real_command = '/bin/bash echo "Test warning"'
        out.stderr = ''
        run_command_mock.return_value = out

        fabric.api.env.host_string = 'localhost'
        with Application(APPLICATION_NAME):
            func('echo "Test warning"')
            pass

        logger_mock.info.assert_has_calls(
            [
                call('\nCOMMAND: echo "Test warning"\nFULL COMMAND: /bin/bash'
                     ' echo "Test warning"\nSTDOUT: Test warning\nSTDERR: '),
                ]
        )
