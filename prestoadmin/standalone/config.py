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
import re

from fabric.api import env
from overrides import overrides

import prestoadmin.util.fabricapi as util
from prestoadmin import config
from prestoadmin.prestoclient import CERTIFICATE_ALIAS
from prestoadmin.util.base_config import BaseConfig, SingleConfigItem
from prestoadmin.util.exception import ConfigurationError
from prestoadmin.util.local_config_util import get_topology_path
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
                           'java8_home', CERTIFICATE_ALIAS]

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
        COORDINATOR,
        'Enter host name or IP address for coordinator node. '
        'Enter an external host name or ip address if this is a multi-node '
        'cluster:',
        default=DEFAULT_PROPERTIES['coordinator'],
        validate=validate_coordinator),
    SingleConfigItem(
        WORKERS,
        'Enter host names or IP addresses for worker nodes separated by spaces:',
        default=' '.join(DEFAULT_PROPERTIES['workers']),
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
        workers = [h for host in workers for h in _expand_host(host)]
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


def _expand_host(host):
    match = re.match("(.*)\[(\d{1,})-(\d{1,})\](.*)", host)
    if match is not None and len(match.groups()) == 4:
        prefix, start, end, suffix = match.groups()
        if int(start) > int(end):
            raise ValueError("the range must be in ascending order")
        if len(start) == len(end) and len(start) > 1:
            number_format = "{0:0" + str(len(start)) + "d}"
            host_list = [number_format.format(i) for i in range(int(start), int(end) + 1)]
            return [_format_hostname(prefix, i, suffix) for i in host_list]
        else:
            return [_format_hostname(prefix, i, suffix) for i in range(int(start), int(end) + 1)]
    else:
        return [host]


def _format_hostname(prefix, number, suffix):
    return "{prefix}{num}{suffix}".format(prefix=prefix, num=number, suffix=suffix)


class StandaloneConfig(BaseConfig):
    def __init__(self):
        super(StandaloneConfig, self).__init__(get_topology_path(), _TOPOLOGY_CONFIG)

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
        return STANDALONE_CONFIG_LOADED in env and env[STANDALONE_CONFIG_LOADED]

    @overrides
    def set_config_loaded(self):
        env[STANDALONE_CONFIG_LOADED] = True

    @overrides
    def set_env_from_conf(self, conf):
        self.config = conf
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
        env.conf = conf

    @staticmethod
    def _dedup_list(host_list):
        deduped_list = []
        for item in host_list:
            if item not in deduped_list:
                deduped_list.append(item)
        return deduped_list
