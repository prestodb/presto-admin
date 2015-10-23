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

from prestoadmin.util.fabric_application import FabricApplication

from tests.base_test_case import BaseTestCase

from mock import patch

import sys
import logging


APPLICATION_NAME = 'foo'


@patch('prestoadmin.util.application.logging.config')
class FabricApplicationTest(BaseTestCase):

    def setUp(self):
        # basicConfig is a noop if there are already handlers
        # present on the root logger, remove them all here
        self.__old_log_handlers = []
        for handler in logging.root.handlers:
            self.__old_log_handlers.append(handler)
            logging.root.removeHandler(handler)
        super(FabricApplicationTest, self).setUp(capture_output=True)

    def tearDown(self):
        # restore the old log handlers
        for handler in logging.root.handlers:
            logging.root.removeHandler(handler)
        for handler in self.__old_log_handlers:
            logging.root.addHandler(handler)
        BaseTestCase.tearDown(self)

    @patch('prestoadmin.util.fabric_application.disconnect_all', autospec=True)
    def test_disconnect_all(self, disconnect_mock, logging_conf_mock):
        def should_disconnect():
            with FabricApplication(APPLICATION_NAME):
                sys.exit()

        self.assertRaises(SystemExit, should_disconnect)
        disconnect_mock.assert_called_with()

    @patch('prestoadmin.util.application.logger')
    @patch('prestoadmin.util.filesystem.os.makedirs')
    def test_keyboard_interrupt(self, make_dirs_mock, logger_mock,
                                logging_conf_mock):
        def should_not_error():
            with FabricApplication(APPLICATION_NAME):
                raise KeyboardInterrupt

        try:
            should_not_error()
        except SystemExit as e:
            self.assertEqual(0, e.code)
            self.assertEqual("Stopped.\n", self.test_stderr.getvalue())
        else:
            self.fail('Keyboard interrupt did not cause a system exit.')

    def test_handles_errors(self, logging_mock):
        def should_fail():
            with FabricApplication(APPLICATION_NAME):
                raise Exception('error message')

        self.assertRaises(SystemExit, should_fail)
        self.assertEqual(self.test_stderr.getvalue(), 'error message\n')
