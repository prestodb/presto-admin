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
Tests the coordinator module
"""
from fabric.api import env
from mock import patch

from prestoadmin import coordinator
from prestoadmin.util.exception import ConfigurationError
from tests.base_test_case import BaseTestCase


class TestCoordinator(BaseTestCase):
    def test_build_defaults(self):
        env.roledefs['coordinator'] = 'a'
        env.roledefs['workers'] = ['b', 'c']
        actual_default = coordinator.build_defaults()
        expected = {'node.properties':
                    {'node.environment': 'presto',
                     'node.data-dir': '/var/lib/presto/data',
                     'plugin.config-dir': '/etc/presto/catalog',
                     'plugin.dir': '/usr/lib/presto/lib/plugin'},
                    'jvm.config': ['-server',
                                   '-Xmx1G',
                                   '-XX:-UseBiasedLocking',
                                   '-XX:+UseG1GC',
                                   '-XX:+ExplicitGCInvokesConcurrent',
                                   '-XX:+HeapDumpOnOutOfMemoryError',
                                   '-XX:+UseGCOverheadLimit',
                                   '-XX:OnOutOfMemoryError=kill -9 %p',
                                   '-DHADOOP_USER_NAME=hive'],
                    'config.properties': {
                        'coordinator': 'true',
                        'discovery-server.enabled': 'true',
                        'discovery.uri': 'http://a:8080',
                        'http-server.http.port': '8080',
                        'task.max-memory': '1GB',
                        'node-scheduler.include-coordinator': 'false',
                        'query.max-memory': '50GB',
                        'query.max-memory-per-node': '1GB'}
                    }

        self.assertEqual(actual_default, expected)

    def test_defaults_coord_is_worker(self):
        env.roledefs['coordinator'] = ['a']
        env.roledefs['worker'] = ['a', 'b', 'c']
        actual_default = coordinator.build_defaults()
        expected = {'node.properties': {
                    'node.environment': 'presto',
                    'node.data-dir': '/var/lib/presto/data',
                    'plugin.config-dir': '/etc/presto/catalog',
                    'plugin.dir': '/usr/lib/presto/lib/plugin'},
                    'jvm.config': ['-server',
                                   '-Xmx1G',
                                   '-XX:-UseBiasedLocking',
                                   '-XX:+UseG1GC',
                                   '-XX:+ExplicitGCInvokesConcurrent',
                                   '-XX:+HeapDumpOnOutOfMemoryError',
                                   '-XX:+UseGCOverheadLimit',
                                   '-XX:OnOutOfMemoryError=kill -9 %p',
                                   '-DHADOOP_USER_NAME=hive'],
                    'config.properties': {
                        'coordinator': 'true',
                        'discovery-server.enabled': 'true',
                        'discovery.uri': 'http://a:8080',
                        'http-server.http.port': '8080',
                        'task.max-memory': '1GB',
                        'node-scheduler.include-coordinator': 'true',
                        'query.max-memory': '50GB',
                        'query.max-memory-per-node': '1GB'}
                    }

        self.assertEqual(actual_default, expected)

    def test_validate_valid(self):
        conf = {'node.properties': {},
                'jvm.config': [],
                'config.properties': {'coordinator': 'true',
                                      'discovery.uri': 'http://uri'}}
        self.assertEqual(conf, coordinator.validate(conf))

    def test_validate_default(self):
        env.roledefs['coordinator'] = 'localhost'
        env.roledefs['workers'] = ['localhost']
        conf = coordinator.build_defaults()
        self.assertEqual(conf, coordinator.validate(conf))

    def test_invalid_conf(self):
        conf = {'node.propoerties': {}}
        self.assertRaisesRegexp(ConfigurationError,
                                'Missing configuration for required file: ',
                                coordinator.validate, conf)

    def test_invalid_conf_coordinator(self):
        conf = {'node.properties': {},
                'jvm.config': [],
                'config.properties': {'coordinator': 'false',
                                      'discovery.uri': 'http://uri'}
                }

        self.assertRaisesRegexp(ConfigurationError,
                                'Coordinator cannot be false in the '
                                'coordinator\'s config.properties',
                                coordinator.validate, conf)

    def test_invalid_discovery_uri_coordinator(self):
        conf = {'node.properties': {},
                'jvm.config': [],
                'config.properties': {'coordinator': 'true'}
                }

        self.assertRaisesRegexp(ConfigurationError,
                                'Must have discovery.uri defined in '
                                'config.properties.',
                                coordinator.validate, conf)
        conf['config.properties']['discovery.uri'] = 'thrift://foo'
        self.assertRaisesRegexp(ConfigurationError,
                                'discovery.uri must start with http://, '
                                'current URI is: thrift://foo',
                                coordinator.validate, conf)

    @patch('prestoadmin.coordinator._get_conf')
    def test_get_conf_empty_is_default(self, get_conf_from_file_mock):
        env.roledefs['coordinator'] = 'j'
        env.roledefs['workers'] = ['K', 'L']
        get_conf_from_file_mock.return_value = {}
        self.assertEqual(coordinator.get_conf(), coordinator.build_defaults())

    @patch('prestoadmin.coordinator.get_presto_conf')
    def test_get_conf(self, get_conf_from_file_mock):
        env.roledefs['coordinator'] = 'j'
        env.roledefs['workers'] = ['K', 'L']
        file_conf = {'node.properties': {'my-property': 'value',
                                         'node.environment': 'test'}}
        get_conf_from_file_mock.return_value = file_conf
        expected = {'node.properties':
                    {'my-property': 'value',
                     'node.environment': 'test',
                     'node.data-dir': '/var/lib/presto/data',
                     'plugin.config-dir': '/etc/presto/catalog',
                     'plugin.dir': '/usr/lib/presto/lib/plugin'},
                    'jvm.config': ['-server',
                                   '-Xmx1G',
                                   '-XX:-UseBiasedLocking',
                                   '-XX:+UseG1GC',
                                   '-XX:+ExplicitGCInvokesConcurrent',
                                   '-XX:+HeapDumpOnOutOfMemoryError',
                                   '-XX:+UseGCOverheadLimit',
                                   '-XX:OnOutOfMemoryError=kill -9 %p',
                                   '-DHADOOP_USER_NAME=hive'],
                    'config.properties': {
                        'coordinator': 'true',
                        'discovery-server.enabled': 'true',
                        'discovery.uri': 'http://j:8080',
                        'http-server.http.port': '8080',
                        'task.max-memory': '1GB',
                        'node-scheduler.include-coordinator': 'false',
                        'query.max-memory': '50GB',
                        'query.max-memory-per-node': '1GB'}
                    }

        self.assertEqual(coordinator.get_conf(), expected)
