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
from functools import wraps
import pprint

import fabric

from fabric.api import task, env, runs_once

from fabric.context_managers import settings

from prestoadmin import config
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigurationError,\
    ConfigFileNotFoundError
import prestoadmin.util.fabricapi as util
from prestoadmin.util.validators import validate_username, validate_port, \
    validate_host

__all__ = ['show']

PRESTO_ADMIN_PROPERTIES = ['username', 'port', 'coordinator', 'workers',
                           'java8_home']
DEFAULT_PROPERTIES = {'username': 'root',
                      'port': 22,
                      'coordinator': 'localhost',
                      'workers': ['localhost']}


def write(conf):
    config.write(config.json_to_string(conf), constants.CONFIG_PATH)


def requires_topology(func):
    @wraps(func)
    def required(*args, **kwargs):
        if 'topology_config_not_found' in env \
                and env.topology_config_not_found:
            raise ConfigFileNotFoundError('Missing topology configuration ' +
                                          'in ' + constants.CONFIG_PATH + '.')
        return func(*args, **kwargs)
    return required


def set_topology_if_missing():
    # this setting is here to avoid errors about prompts in parallel mode
    with settings(parallel=False):
        if 'topology_config_not_found' in env and \
                env.topology_config_not_found:
            set_conf_interactive()
            set_env_from_conf()
            env.topology_config_not_found = None


@task
@runs_once
@requires_topology
def show():
    """
    Shows the current topology configuration for the cluster (including the
    coordinators, workers, SSH port, and SSH username)
    """
    pprint.pprint(get_conf_from_fabric(), width=1)


def get_conf_from_fabric():
    return {'coordinator': util.get_coordinator_role()[0],
            'workers': util.get_worker_role(),
            'port': env.port,
            'username': env.user}


def get_conf():
    conf = _get_conf_from_file()
    config.fill_defaults(conf, DEFAULT_PROPERTIES)
    validate(conf)
    return conf


def _get_conf_from_file():
    return config.get_conf_from_json_file(constants.CONFIG_PATH)


def set_conf_interactive():
    write(build_conf_interactive())
    del env['topology_config_not_found']


def build_conf_interactive():
    return {'username': prompt_for_username(),
            'port': prompt_for_port(),
            'coordinator': prompt_for_coordinator(),
            'workers': prompt_for_workers()}


def prompt_for_username():
    return fabric.operations.prompt('Enter user name for SSH connection to '
                                    'all nodes:',
                                    default=DEFAULT_PROPERTIES['username'],
                                    validate=validate_username)


def prompt_for_port():
    return fabric.operations.prompt('Enter port number for SSH connections to '
                                    'all nodes:',
                                    default=DEFAULT_PROPERTIES['port'],
                                    validate=validate_port)


def prompt_for_coordinator():
    return fabric.operations.prompt('Enter host name or IP address for '
                                    'coordinator node.  Enter an external '
                                    'host name or ip address if this is a '
                                    'multi-node cluster:',
                                    default=DEFAULT_PROPERTIES['coordinator'],
                                    validate=validate_coordinator)


def prompt_for_workers():
    return fabric.operations.prompt('Enter host names or IP addresses for '
                                    'worker nodes separated by spaces:',
                                    default=' '.join(
                                        DEFAULT_PROPERTIES['workers']),
                                    validate=validate_workers_for_prompt)


def validate_java8_home(java8_home):
    return java8_home


def validate_workers_for_prompt(workers):
    return validate_workers(workers.split())


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


def validate_coordinator(coordinator):
    validate_host(coordinator)
    return coordinator


def validate_workers(workers):
    if not isinstance(workers, list):
        raise ConfigurationError('Workers must be of type list.  Found ' +
                                 str(type(workers)) + '.')

    if len(workers) < 1:
        raise ConfigurationError('Must specify at least one worker')

    for worker in workers:
        validate_host(worker)
    return workers


def dedup_list(host_list):
    deduped_list = []
    for item in host_list:
        if item not in deduped_list:
            deduped_list.append(item)
    return deduped_list


def set_env_from_conf():
    conf = get_conf()

    env.user = conf['username']
    env.port = conf['port']
    try:
        env.java8_home = conf['java8_home']
    except KeyError:
        env.java8_home = None
    env.roledefs['coordinator'] = [conf['coordinator']]
    env.roledefs['worker'] = conf['workers']
    env.roledefs['all'] = dedup_list(util.get_coordinator_role() +
                                     util.get_worker_role())

    # This ensures that we honor a hosts list passed on the command line.
    if not env.hosts:
        env.hosts = env.roledefs['all'][:]
