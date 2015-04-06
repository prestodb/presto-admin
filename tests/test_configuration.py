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
import re

from prestoadmin import configuration as config
import prestoadmin
import utils


class TestConfiguration(utils.BaseTestCase):
    def test_file_does_not_exist(self):
        self.assertRaisesRegexp(config.ConfigFileNotFoundError,
                                "Missing configuration file at",
                                config.get_conf_from_file,
                                "does/not/exist/conf.json")

    def test_invalid_json(self):
        self.assertRaisesRegexp(config.ConfigurationError,
                                "Expecting , delimiter: line 3 column 3 "
                                "\(char 19\)",
                                config.get_conf_from_file,
                                prestoadmin.main_dir +
                                "/tests/files/invalid_json_conf.json")

    def test_fill_defaults_no_missing(self):
        orig = {"key1": "val1", "key2": "val2", "key3": "val3"}
        defaults = {"key1": "default1", "key2": "default2"}
        filled = orig.copy()
        config.fill_defaults(filled, defaults)
        self.assertEqual(filled, orig)

    def test_fill_defaults(self):
        orig = {"key1": "val1", "key3": "val3"}
        defaults = {"key1": "default1", "key2": "default2"}
        filled = orig.copy()
        config.fill_defaults(filled, defaults)
        self.assertEqual(filled,
                         {"key1": "val1", "key2": "default2", "key3": "val3"})

    def test_valid_conf(self):
        conf = {"node.properties": {}, "jvm.config": [],
                "config.properties": {}}
        self.assertEqual(config.validate_presto_conf(conf), conf)

    def test_invalid_conf(self):
        conf = {"jvm.config": [],
                "config.properties": {}}
        self.assertRaisesRegexp(config.ConfigurationError,
                                "Missing configuration for required file:",
                                config.validate_presto_conf,
                                conf)

    def test_invalid_node_type(self):
        conf = {"node.properties": "", "jvm.config": [],
                "config.properties": {}}
        self.assertRaisesRegexp(config.ConfigurationError,
                                "node.properties must be an object with key-"
                                "value property pairs",
                                config.validate_presto_conf,
                                conf)

    def test_invalid_jvm_type(self):
        conf = {"node.properties": {}, "jvm.config": {},
                "config.properties": {}}
        self.assertRaisesRegexp(config.ConfigurationError,
                                re.escape("jvm.config must contain a json "
                                          "array of jvm arguments ([arg1, "
                                          "arg2, arg3])"),
                                config.validate_presto_conf,
                                conf)

    def test_invalid_config_type(self):
        conf = {"node.properties": {}, "jvm.config": [],
                "config.properties": []}
        self.assertRaisesRegexp(config.ConfigurationError,
                                "config.properties must be an object with key-"
                                "value property pairs",
                                config.validate_presto_conf,
                                conf)
