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
Module to test hadoop config parsing.
"""

import os
import sys
from xml.parsers.expat import ExpatError

from prestoadmin import main_dir
from prestoadmin.util.exception import ConfigurationError
from prestoadmin.util.hadoop_conf import get_config
from prestoadmin.util.version_util import VersionRange, VersionRangeList

from tests.unit.base_unit_case import BaseUnitCase

TEST_CONFIG_DIR = os.path.join(main_dir, 'tests', 'unit', 'resources')


EXPECTED = {
    'fs.defaultFS': 'hdfs://master/',
    'hadoop.home': '/usr/lib/hadoop',
    'slider.test.agent.enabled': 'true',
    'slider.zookeeper.quorum': 'master:5181',
    'yarn.application.classpath': '/etc/hadoop/conf,/usr/lib/hadoop/*,'
                                  '/usr/lib/hadoop/lib/*,'
                                  '/usr/lib/hadoop-hdfs/*,'
                                  '/usr/lib/hadoop-hdfs/lib/*,'
                                  '/usr/lib/hadoop-yarn/*,'
                                  '/usr/lib/hadoop-yarn/lib/*,'
                                  '/usr/lib/hadoop-mapreduce/*,'
                                  '/usr/lib/hadoop-mapreduce/lib/*',
    'yarn.resourcemanager.address': 'master:8050',
    'yarn.resourcemanager.scheduler.address': 'master:8030',
    'zk.home': '/usr/lib/zookeeper'}


class HadoopConfTest(BaseUnitCase):
    # release is a string, not a number. Strip it and serial out.
    def _python_version(self):
        M, m, u, r, s = sys.version_info
        return M, m, u

    def test_good(self):
        config = get_config(os.path.join(TEST_CONFIG_DIR, 'slider-client.xml'))
        self.assertEqual(EXPECTED, config)

    def test_unclosed_property(self):
        expected_exceptions = VersionRangeList(
            VersionRange((2, 6, 0), (2, 7, 0), ExpatError),
            VersionRange((2, 7, 0), (sys.maxint,), ConfigurationError))
        expected_exception = expected_exceptions.for_version(
            self._python_version())
        config_path = os.path.join(TEST_CONFIG_DIR,
                                   'slider-client-unclosed-property.xml')
        self.assertRaises(expected_exception, get_config, config_path)

    @staticmethod
    def _lazy_parse_error():
        # ParseError is new in Python 2.7. We can't import it at the top level
        # or the Unit tests will fail under Python 2.6
        from xml.etree.ElementTree import ParseError
        return ParseError

    def test_unclosed_configuration(self):
        expected_exceptions = VersionRangeList(
            VersionRange((2, 6, 0), (2, 7, 0), lambda: ExpatError),
            VersionRange((2, 7, 0), (sys.maxint,), self._lazy_parse_error))
        expected_exception = expected_exceptions.for_version(
            self._python_version())()
        config_path = os.path.join(TEST_CONFIG_DIR,
                                   'slider-client-unclosed-configuration.xml')
        self.assertRaises(expected_exception, get_config, config_path)

    def test_missing_name(self):
        config_path = os.path.join(TEST_CONFIG_DIR,
                                   'slider-client-missing-name.xml')
        self.assertRaises(ConfigurationError, get_config, config_path)

    def test_missing_value(self):
        config_path = os.path.join(TEST_CONFIG_DIR,
                                   'slider-client-missing-value.xml')
        self.assertRaises(ConfigurationError, get_config, config_path)

    def test_nx_file(self):
        config_path = os.path.join(TEST_CONFIG_DIR, 'NX_FILE')
        self.assertRaises(IOError, get_config, config_path)
