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

from prestoadmin.util import constants
from prestoadmin.util.application import Application
from prestoadmin.util.application import NullHandler
from prestoadmin.util.exception import UserVisibleError

from unittest import TestCase
from mock import patch
from mock import call

import os
import sys
import logging


APPLICATION_NAME = 'foo'


@patch('prestoadmin.util.application.filesystem')
@patch('prestoadmin.util.application.logging.config')
class ApplicationTest(TestCase):

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
            constants.PRESTOADMIN_LOG,
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
            constants.PRESTOADMIN_LOG,
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

        expected_error = FakeError()
        logging_mock.fileConfig.side_effect = expected_error

        with Application(APPLICATION_NAME):
            pass

        stderr_mock.write.assert_has_calls(
            [
                call(
                    'Unable to configure logging using file {path}, '
                    'no messages will be logged.\nError Message: {msg}'.format(
                        path=constants.LOGGING_CONFIG_FILE_NAME,
                        msg=str(expected_error)
                    )
                )
            ]
        )

        found = False
        for handler in logging.getLogger().handlers:
            if isinstance(handler, NullHandler):
                found = True
                break
        self.assertTrue(found)

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
    def test_handles_unexpected_errors(
        self,
        stderr_mock,
        logging_mock,
        filesystem_mock
    ):
        def should_fail():
            with Application(APPLICATION_NAME):
                raise ValueError('Invalid value!')

        self.assertRaises(SystemExit, should_fail)

        stderr_mock.write.assert_has_calls(
            [
                call('An unexpected error occurred.'),
                call('\n')
            ]
        )

    @patch('prestoadmin.util.application.sys.stderr')
    def test_handles_user_visible_errors(
        self,
        stderr_mock,
        logging_mock,
        filesystem_mock
    ):
        def should_fail():
            with Application(APPLICATION_NAME):
                raise UserVisibleError('User facing error message')

        self.assertRaises(SystemExit, should_fail)

        stderr_mock.write.assert_has_calls(
            [
                call('User facing error message'),
                call('\n')
            ]
        )

    @patch('prestoadmin.util.application.sys.stderr')
    def test_handles_system_abnormal_exits(
        self,
        stderr_mock,
        logging_mock,
        filesystem_mock
    ):
        def should_exit():
            with Application(APPLICATION_NAME):
                sys.exit(2)

        self.assertRaises(SystemExit, should_exit)

    @patch('prestoadmin.util.application.sys.stderr')
    def test_handles_system_normal_exits(
        self,
        stderr_mock,
        logging_mock,
        filesystem_mock
    ):
        def should_exit():
            with Application(APPLICATION_NAME):
                sys.exit()

        self.assertRaises(SystemExit, should_exit)


class FakeError(Exception):
    pass
