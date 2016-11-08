#!/usr/bin/env python
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
Installers for installing mode.json onto clusters
"""

import json

from overrides import overrides
from prestoadmin import config
from prestoadmin.mode import VALID_MODES, MODE_KEY, MODE_STANDALONE
from tests.base_installer import BaseInstaller
from tests.product.config_dir_utils import get_mode_config_path


class BaseModeInstaller(BaseInstaller):
    def __init__(self, testcase, mode):
        self.testcase = testcase
        testcase.assertIn(mode, VALID_MODES)
        self.mode = mode
        self.json = config.json_to_string(self._get_mode_cfg(self.mode))

    @staticmethod
    def _get_mode_cfg(mode):
        return {MODE_KEY: mode}

    @staticmethod
    @overrides
    def get_dependencies():
        return []

    @overrides
    def install(self):
        self.testcase.cluster.write_content_to_host(
            self.json, get_mode_config_path(), self.testcase.cluster.master)

    @overrides
    def get_keywords(self, *args, **kwargs):
        return {}

    @staticmethod
    def _assert_installed(testcase, expected_mode):
        json_str = testcase.cluster.exec_cmd_on_host(
            testcase.cluster.master, 'cat %s' % get_mode_config_path())

        actual_mode_cfg = json.loads(json_str)
        testcase.assertEqual(
            BaseModeInstaller._get_mode_cfg(
                expected_mode), actual_mode_cfg)


class StandaloneModeInstaller(BaseModeInstaller):
    def __init__(self, testcase):
        super(StandaloneModeInstaller, self).__init__(
            testcase, MODE_STANDALONE)

    @staticmethod
    def assert_installed(testcase):
        BaseModeInstaller._assert_installed(testcase, MODE_STANDALONE)
