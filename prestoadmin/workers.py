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
Module for the presto worker`'s configuration.
Loads and validates the workers.json file and creates the files needed
to deploy on the presto cluster
"""

import copy
import logging
import urlparse

from fabric.api import env

from prestoadmin.node import Node
from prestoadmin.presto_conf import validate_presto_conf
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigurationError
import prestoadmin.util.fabricapi as util

_LOGGER = logging.getLogger(__name__)


class Worker(Node):
    DEFAULT_PROPERTIES = {'node.properties':
                          {'node.environment': 'presto',
                           'node.data-dir': '/var/lib/presto/data',
                           'plugin.config-dir': '/etc/presto/catalog',
                           'plugin.dir': '/usr/lib/presto/lib/plugin'},
                          'jvm.config': ['-server',
                                         '-Xmx2G',
                                         '-XX:-UseBiasedLocking',
                                         '-XX:+UseG1GC',
                                         '-XX:+ExplicitGCInvokesConcurrent',
                                         '-XX:+HeapDumpOnOutOfMemoryError',
                                         '-XX:+UseGCOverheadLimit',
                                         '-XX:OnOutOfMemoryError=kill -9 %p',
                                         '-DHADOOP_USER_NAME=hive'],
                          'config.properties': {'coordinator': 'false',
                                                'http-server.http.port':
                                                    '8080',
                                                'query.max-memory': '50GB',
                                                'query.max-memory-per-node':
                                                    '1GB'}
                          }

    def _get_conf_dir(self):
        return constants.WORKERS_DIR

    def build_defaults(self):
        conf = copy.deepcopy(self.DEFAULT_PROPERTIES)
        coordinator = util.get_coordinator_role()[0]
        conf['config.properties']['discovery.uri'] = 'http://' + coordinator \
                                                     + ':8080'
        return conf

    @staticmethod
    def is_localhost(hostname):
        return hostname in ['localhost', '127.0.0.1', '::1']

    @staticmethod
    def validate(conf):
        validate_presto_conf(conf)
        if conf['config.properties']['coordinator'] != 'false':
            raise ConfigurationError('Coordinator must be false in the '
                                     'worker\'s config.properties')
        uri = urlparse.urlparse(conf['config.properties']['discovery.uri'])
        if Worker.is_localhost(uri.hostname) and len(env.roledefs['all']) > 1:
            raise ConfigurationError(
                'discovery.uri should not be localhost in a '
                'multi-node cluster, but found ' + urlparse.urlunparse(uri) +
                '.  You may have encountered this error by '
                'choosing a coordinator that is localhost and a worker that '
                'is not.  The default discovery-uri is '
                'http://<coordinator>:8080')
        return conf
