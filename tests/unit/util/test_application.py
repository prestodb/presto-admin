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
import sys
import logging

from mock import patch
from mock import call

from prestoadmin.util import constants
from prestoadmin.util.application import Application
from prestoadmin.util.local_config_util import get_log_directory

from tests.base_test_case import BaseTestCase

APPLICATION_NAME = 'foo'


@patch('prestoadmin.util.application.filesystem')
@patch('prestoadmin.util.application.logging.config')
class ApplicationTest(BaseTestCase):

    def setUp(self):
        # basicConfig is a noop if there are already handlers
        # present on the root logger, remove them all here
        self.__old_log_handlers = []
        for handler in logging.root.handlers:
            self.__old_log_handlers.append(handler)
            logging.root.removeHandler(handler)

    def tearDown(self):
        # restore the old log handlers
        for handler in logging.root.handlers:
            logging.root.removeHandler(handler)
        for handler in self.__old_log_handlers:
            logging.root.addHandler(handler)

    @patch('prestoadmin.util.application.os.path.exists')
    def test_configures_default_log_file(
        self,
        path_exists_mock,
        logging_mock,
        filesystem_mock
    ):
        path_exists_mock.return_value = True

        with Application(APPLICATION_NAME):
            pass

        file_path = os.path.join(
            get_log_directory(),
            APPLICATION_NAME + '.log'
        )
        self.__assert_logging_setup_with_file(
            file_path,
            filesystem_mock,
            logging_mock
        )

        path_exists_mock.assert_called_once_with(
            constants.LOGGING_CONFIG_FILE_NAME
        )

    def __assert_logging_setup_with_file(
        self,
        log_file_path,
        filesystem_mock,
        logging_mock
    ):
        parent_dirs_mock = filesystem_mock.ensure_parent_directories_exist
        parent_dirs_mock.assert_called_once_with(log_file_path)

        file_config_mock = logging_mock.fileConfig
        file_config_mock.assert_called_once_with(
            constants.LOGGING_CONFIG_FILE_NAME,
            defaults={'log_file_path': log_file_path},
            disable_existing_loggers=False
        )

    @patch('prestoadmin.util.application.os.path.exists')
    def test_configures_custom_log_file(
        self,
        path_exists_mock,
        logging_mock,
        filesystem_mock
    ):
        path_exists_mock.return_value = True

        log_file_path = 'bar.log'
        with Application(
            APPLICATION_NAME,
            log_file_path=log_file_path
        ):
            pass

        file_path = os.path.join(
            get_log_directory(),
            log_file_path
        )
        self.__assert_logging_setup_with_file(
            file_path,
            filesystem_mock,
            logging_mock
        )

    @patch('prestoadmin.util.application.os.path.exists')
    @patch('prestoadmin.util.application.sys.stderr')
    def test_configures_invalid_log_file(
        self,
        stderr_mock,
        path_exists_mock,
        logging_mock,
        filesystem_mock
    ):
        path_exists_mock.return_value = True

        expected_error = FakeError('Error')
        logging_mock.fileConfig.side_effect = expected_error

        try:
            with Application(APPLICATION_NAME):
                pass
        except SystemExit as e:
            self.assertEqual('Error', e.message)

        stderr_mock.write.assert_has_calls(
            [
                call('Please run %s with sudo.\n' % APPLICATION_NAME),
            ]
        )

    @patch('prestoadmin.util.application.os.path.exists')
    def test_configures_absolute_path_to_log_file(
        self,
        path_exists_mock,
        logging_mock,
        filesystem_mock
    ):
        path_exists_mock.return_value = True

        log_file_path = '/tmp/bar.log'
        with Application(
            APPLICATION_NAME,
            log_file_path=log_file_path
        ):
            pass

        self.__assert_logging_setup_with_file(
            log_file_path,
            filesystem_mock,
            logging_mock
        )

    @patch('prestoadmin.util.application.os.path.exists')
    def test_uses_logging_configs_in_order(
        self,
        path_exists_mock,
        logging_mock,
        filesystem_mock
    ):
        path_exists_mock.side_effect = [False, True]

        log_file_path = '/tmp/bar.log'
        with Application(
            APPLICATION_NAME,
            log_file_path=log_file_path
        ):
            pass

        parent_dirs_mock = filesystem_mock.ensure_parent_directories_exist
        parent_dirs_mock.assert_called_once_with(log_file_path)

        file_config_mock = logging_mock.fileConfig
        file_config_mock.assert_called_once_with(
            log_file_path + '.ini',
            defaults={'log_file_path': log_file_path},
            disable_existing_loggers=False
        )

    @patch('prestoadmin.util.application.sys.stderr')
    def test_handles_errors(
        self,
        stderr_mock,
        logging_mock,
        filesystem_mock
    ):
        def should_fail():
            with Application(APPLICATION_NAME):
                raise Exception('User facing error message')

        self.assertRaises(SystemExit, should_fail)

        stderr_mock.write.assert_has_calls(
            [
                call('User facing error message'),
                call('\n')
            ]
        )

    @patch('prestoadmin.util.application.logger')
    def test_handles_system_abnormal_exits(
        self,
        logger_mock,
        logging_mock,
        filesystem_mock
    ):
        def should_exit():
            with Application(APPLICATION_NAME):
                sys.exit(2)

        self.assertRaises(SystemExit, should_exit)
        logger_mock.debug.assert_has_calls(
            [
                call('Application exiting with status %d', 2),
            ]
        )

    @patch('prestoadmin.util.application.logger')
    def test_handles_system_normal_exits(
        self,
        logger_mock,
        logging_mock,
        filesystem_mock
    ):
        def should_exit():
            with Application(APPLICATION_NAME):
                sys.exit()

        self.assertRaises(SystemExit, should_exit)
        logger_mock.debug.assert_has_calls(
            [
                call('Application exiting with status %d', 0),
            ]
        )

    @patch('prestoadmin.util.application.logger')
    def test_handles_system_exit_none(
        self,
        logger_mock,
        logging_mock,
        filesystem_mock
    ):
        def should_exit_zero_with_none():
            with Application(APPLICATION_NAME):
                sys.exit(None)

        self.assertRaises(SystemExit, should_exit_zero_with_none)
        logger_mock.debug.assert_has_calls(
            [
                call('Application exiting with status %d', 0),
            ]
        )

    @patch('prestoadmin.util.application.logger')
    def test_handles_system_exit_string(
            self,
            logger_mock,
            logging_mock,
            filesystem_mock
    ):
        def should_exit_one_with_str():
            with Application(APPLICATION_NAME):
                sys.exit("exit")

        self.assertRaises(SystemExit, should_exit_one_with_str)
        logger_mock.debug.assert_has_calls(
            [
                call('Application exiting with status %d', 1),
                ]
        )


class FakeError(Exception):
    pass
