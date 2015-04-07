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
Tests the workers module
"""

import utils

from fabric.api import env
from prestoadmin import configuration, workers
from mock import patch


class TestWorkers(utils.BaseTestCase):
    def test_build_defaults(self):
        env.roledefs['coordinator'] = 'a'
        env.roledefs['workers'] = ["b", "c"]
        actual_default = workers.build_defaults()
        expected = {"node.properties": {"node.environment": "presto",
                                        "node.data-dir": "/var/lib/presto/"
                                                         "data"},
                    "jvm.config": ["-server",
                                   "-Xmx1G",
                                   "-XX:+UseConcMarkSweepGC",
                                   "-XX:+ExplicitGCInvokesConcurrent",
                                   "-XX:+CMSClassUnloadingEnabled",
                                   "-XX:+AggressiveOpts",
                                   "-XX:+HeapDumpOnOutOfMemoryError",
                                   "-XX:OnOutOfMemoryError=kill -9 %p",
                                   "-XX:ReservedCodeCacheSize=150M"],
                    "config.properties": {"coordinator": "false",
                                          "discovery.uri": "http://a:8080",
                                          "http-server.http.port": "8080",
                                          "task.max-memory": "1GB"}
                    }

        self.assertEqual(actual_default, expected)

    def test_validate_valid(self):
        conf = {"node.properties": {},
                "jvm.config": [],
                "config.properties": {"coordinator": "false"}}
        self.assertEqual(conf, workers.validate(conf))

    def test_validate_default(self):
        env.roledefs['coordinator'] = 'localhost'
        conf = workers.build_defaults()
        self.assertEqual(conf, workers.validate(conf))

    def test_invalid_conf(self):
        conf = {"node.propoerties": {}}
        self.assertRaisesRegexp(configuration.ConfigurationError,
                                "Missing configuration for required file: ",
                                workers.validate, conf)

    def test_invalid_conf_coordinator(self):
        conf = {"node.properties": {},
                "jvm.config": [],
                "config.properties": {"coordinator": "true"}
                }

        self.assertRaisesRegexp(configuration.ConfigurationError,
                                "Coordinator must be false in the "
                                "worker's config.properties",
                                workers.validate, conf)

    @patch('prestoadmin.configuration.get_conf_from_file',
           side_effect=configuration.ConfigFileNotFoundError)
    def test_conf_not_exists_is_default(self, get_conf_from_file_mock):
        env.roledefs['coordinator'] = ["j"]
        self.assertEqual(workers.get_conf(), workers.build_defaults())

    @patch('prestoadmin.workers._get_conf_from_file')
    def test_get_conf_empty_is_default(self, get_conf_from_file_mock):
        env.roledefs['coordinator'] = ["j"]
        get_conf_from_file_mock.return_value = {}
        self.assertEqual(workers.get_conf(), workers.build_defaults())

    @patch('prestoadmin.workers._get_conf_from_file')
    def test_get_conf(self, get_conf_from_file_mock):
        env.roledefs['coordinator'] = ["j"]
        file_conf = {"node.properties": {"my-property": "value",
                                         "node.environment": "test"}}
        get_conf_from_file_mock.return_value = file_conf
        expected = {"node.properties": {"my-property": "value",
                                        "node.environment": "test",
                                        "node.data-dir": "/var/lib/presto/"
                                                         "data"},
                    "jvm.config": ["-server",
                                   "-Xmx1G",
                                   "-XX:+UseConcMarkSweepGC",
                                   "-XX:+ExplicitGCInvokesConcurrent",
                                   "-XX:+CMSClassUnloadingEnabled",
                                   "-XX:+AggressiveOpts",
                                   "-XX:+HeapDumpOnOutOfMemoryError",
                                   "-XX:OnOutOfMemoryError=kill -9 %p",
                                   "-XX:ReservedCodeCacheSize=150M"],
                    "config.properties": {"coordinator": "false",
                                          "discovery.uri": "http://j:8080",
                                          "http-server.http.port": "8080",
                                          "task.max-memory": "1GB"}
                    }
        self.assertEqual(workers.get_conf(), expected)

    @patch('prestoadmin.workers._get_conf_from_file')
    def test_get_conf_invalid(self, get_conf_from_file_mock):
        env.roledefs['coordinator'] = ["j"]
        file_conf = {"node.properties": "my string"}
        get_conf_from_file_mock.return_value = file_conf
        self.assertRaisesRegexp(configuration.ConfigurationError,
                                "node.properties must be an object with "
                                "key-value property pairs",
                                workers.get_conf)
