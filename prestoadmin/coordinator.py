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
Module for the presto coordinator's configuration.
Loads and validates the coordinator.json file and creates the files needed
to deploy on the presto cluster
"""
import copy
import logging

from fabric.api import env

from prestoadmin.node import Node
from prestoadmin.presto_conf import validate_presto_conf
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigurationError

_LOGGER = logging.getLogger(__name__)


class Coordinator(Node):
    DEFAULT_PROPERTIES = {'node.properties':
                          {'node.environment': 'presto',
                           'node.data-dir': '/var/lib/presto/data',
                           'node.launcher-log-file':
                               '/var/log/presto/launcher.log',
                           'node.server-log-file':
                               '/var/log/presto/server.log',
                           'plugin.config-dir': '/etc/presto/catalog',
                           'plugin.dir': '/usr/lib/presto/lib/plugin'},
                          'jvm.config': ['-server',
                                         '-Xmx16G',
                                         '-XX:-UseBiasedLocking',
                                         '-XX:+UseG1GC',
                                         '-XX:+ExplicitGCInvokesConcurrent',
                                         '-XX:+HeapDumpOnOutOfMemoryError',
                                         '-XX:+UseGCOverheadLimit',
                                         '-XX:OnOutOfMemoryError=kill -9 %p',
                                         '-XX:ReservedCodeCacheSize=512M',
                                         '-DHADOOP_USER_NAME=hive'],
                          'config.properties': {
                              'coordinator': 'true',
                              'discovery-server.enabled': 'true',
                              'http-server.http.port': '8080',
                              'node-scheduler.include-coordinator': 'false',
                              'query.max-memory': '50GB',
                              'query.max-memory-per-node': '8GB'}
                          }

    def _get_conf_dir(self):
        return constants.COORDINATOR_DIR

    def default_config(self, filename):
        try:
            conf = copy.deepcopy(self.DEFAULT_PROPERTIES[filename])
        except KeyError:
            raise ConfigurationError('Invalid configuration file name: %s' %
                                     filename)
        if filename == 'config.properties':
            coordinator = env.roledefs['coordinator'][0]
            workers = env.roledefs['worker']
            if coordinator in workers:
                conf['node-scheduler.include-coordinator'] = 'true'
            conf['discovery.uri'] = 'http://%s:8080' % coordinator
        return conf

    @staticmethod
    def validate(conf):
        validate_presto_conf(conf)
        if 'coordinator' not in conf['config.properties']:
            raise ConfigurationError('Must specify coordinator=true in '
                                     'coordinator\'s config.properties')
        if conf['config.properties']['coordinator'] != 'true':
            raise ConfigurationError('Coordinator cannot be false in the '
                                     'coordinator\'s config.properties.')
        return conf
