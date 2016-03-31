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
Module for setting and validating the presto-admin config
"""
from fabric.api import env

from overrides import overrides

from prestoadmin import config
from prestoadmin.util import constants
from prestoadmin.util.base_config import BaseConfig, SingleConfigItem
from prestoadmin.util.exception import ConfigurationError
import prestoadmin.util.fabricapi as util
from prestoadmin.util.validators import validate_username, validate_port, \
    validate_host

# Created by the presto-server RPM package.
PRESTO_STANDALONE_USER = 'presto'
PRESTO_STANDALONE_GROUP = 'presto'
PRESTO_STANDALONE_USER_GROUP = "%s:%s" % (PRESTO_STANDALONE_USER,
                                          PRESTO_STANDALONE_GROUP)

# CONFIG KEYS
USERNAME = 'username'
PORT = 'port'
COORDINATOR = 'coordinator'
WORKERS = 'workers'

STANDALONE_CONFIG_LOADED = 'standalone_config_loaded'

PRESTO_ADMIN_PROPERTIES = ['username', 'port', 'coordinator', 'workers',
                           'java8_home']

DEFAULT_PROPERTIES = {USERNAME: 'root',
                      PORT: 22,
                      COORDINATOR: 'localhost',
                      WORKERS: ['localhost']}


def validate_coordinator(coordinator):
    validate_host(coordinator)
    return coordinator


def validate_workers_for_prompt(workers):
    return validate_workers(workers.split())


_TOPOLOGY_CONFIG = [
    SingleConfigItem(
        USERNAME, 'Enter user name for SSH connection to all nodes:',
        default=DEFAULT_PROPERTIES[USERNAME], validate=validate_username),
    SingleConfigItem(
        PORT, 'Enter port number for SSH connections to all nodes:',
        default=DEFAULT_PROPERTIES['port'], validate=validate_port),
    SingleConfigItem(
        COORDINATOR, 'Enter host name or IP address for coordinator node. '
        'Enter an external host name or ip address if this is a multi-node '
        'cluster:', default=DEFAULT_PROPERTIES['coordinator'],
        validate=validate_coordinator),
    SingleConfigItem(
        WORKERS, 'Enter host names or IP addresses for worker nodes separated '
        'by spaces:', default=' '.join(DEFAULT_PROPERTIES['workers']),
        validate=validate_workers_for_prompt)
]


def validate_java8_home(java8_home):
    return java8_home


def validate(conf):
    for key in conf.keys():
        if key not in PRESTO_ADMIN_PROPERTIES:
            raise ConfigurationError('Invalid property: ' + key)

    try:
        username = conf['username']
    except KeyError:
        pass
    else:
        conf['username'] = validate_username(username)

    try:
        java8_home = conf['java8_home']
    except KeyError:
        pass
    else:
        conf['java8_home'] = validate_java8_home(java8_home)

    try:
        coordinator = conf['coordinator']
    except KeyError:
        pass
    else:
        conf['coordinator'] = validate_coordinator(coordinator)

    try:
        workers = conf['workers']
    except KeyError:
        pass
    else:
        conf['workers'] = validate_workers(workers)

    try:
        port = conf['port']
    except KeyError:
        pass
    else:
        conf['port'] = validate_port(port)
    return conf


def validate_workers(workers):
    if not isinstance(workers, list):
        raise ConfigurationError('Workers must be of type list.  Found ' +
                                 str(type(workers)) + '.')

    if len(workers) < 1:
        raise ConfigurationError('Must specify at least one worker')

    for worker in workers:
        validate_host(worker)
    return workers


class StandaloneConfig(BaseConfig):

    def __init__(self):
        super(StandaloneConfig, self).__init__(constants.TOPOLOGY_CONFIG_PATH,
                                               _TOPOLOGY_CONFIG)

    @overrides
    def read_conf(self):
        conf = self._get_conf_from_file()
        config.fill_defaults(conf, DEFAULT_PROPERTIES)
        validate(conf)
        return conf

    def _get_conf_from_file(self):
        return config.get_conf_from_json_file(self.config_path)

    @overrides
    def is_config_loaded(self):
        return STANDALONE_CONFIG_LOADED in env and \
            env[STANDALONE_CONFIG_LOADED]

    @overrides
    def set_config_loaded(self):
        env[STANDALONE_CONFIG_LOADED] = True

    @overrides
    def set_env_from_conf(self, conf):
        env.user = conf['username']
        env.port = conf['port']
        try:
            env.java8_home = conf['java8_home']
        except KeyError:
            env.java8_home = None
        env.roledefs['coordinator'] = [conf['coordinator']]
        env.roledefs['worker'] = conf['workers']
        env.roledefs['all'] = self._dedup_list(util.get_coordinator_role() +
                                               util.get_worker_role())

        env.hosts = env.roledefs['all'][:]

    @staticmethod
    def _dedup_list(host_list):
        deduped_list = []
        for item in host_list:
            if item not in deduped_list:
                deduped_list.append(item)
        return deduped_list
