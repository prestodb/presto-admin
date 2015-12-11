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
from xml.etree.ElementTree import ParseError

from prestoadmin import main_dir
from prestoadmin.util.exception import ConfigurationError
from prestoadmin.util.hadoop_conf import get_config

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
    def test_good(self):
        config = get_config(os.path.join(TEST_CONFIG_DIR, 'slider-client.xml'))
        self.assertEqual(EXPECTED, config)

    def test_unclosed_property(self):
        config_path = os.path.join(TEST_CONFIG_DIR,
                                   'slider-client-unclosed-property.xml')
        self.assertRaises(ConfigurationError, get_config, config_path)

    def test_unclosed_configuration(self):
        config_path = os.path.join(TEST_CONFIG_DIR,
                                   'slider-client-unclosed-configuration.xml')
        self.assertRaises(ParseError, get_config, config_path)

    def test_missing_name(self):
        config_path = os.path.join(TEST_CONFIG_DIR,
                                   'slider-client-missing-name.xml')
        self.assertRaises(ConfigurationError, get_config, config_path)

    def test_missing_value(self):
        config_path = os.path.join(TEST_CONFIG_DIR,
                                   'slider-client-missing-value.xml')
        self.assertRaises(ConfigurationError, get_config, config_path)
