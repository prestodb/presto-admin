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
Module for setting and validating the presto-admin slider config
"""

import errno

from collections import namedtuple
from functools import wraps

from fabric.context_managers import settings
from fabric.state import env
from fabric.operations import prompt, run, sudo

from prestoadmin import config
from prestoadmin.config import ConfigFileNotFoundError

from prestoadmin.util.validators import validate_host
from prestoadmin.util.validators import validate_port
from prestoadmin.util.validators import validate_username

SLIDER_CONFIG_LOADED = 'slider_config_loaded'
SLIDER_CONFIG_PATH = '/etc/opt/prestoadmin/slider/config.json'
SLIDER_MASTER = 'slider_master'

_HOST = 'slider_master'
_ADMIN_USER = 'admin'
_SSH_PORT = 'ssh_port'

_DIR = 'slider_directory'
_APPNAME = 'slider_appname'
_SLIDER_USER = 'slider_user'
_JAVA_HOME = 'JAVA_HOME'
_HADOOP_CONF = 'HADOOP_CONF'

SliderConfigItem = namedtuple('SliderConfigItem',
                              ['text', 'default', 'validate'])
_SLIDER_CONFIG = {
    _HOST: SliderConfigItem('Enter the hostname for the slider master:',
                            None, validate_host),
    _ADMIN_USER: SliderConfigItem('Enter the user name to use when ' +
                                    'installing slider on the slider master:',
                                    'root', validate_username),
    _SSH_PORT: SliderConfigItem('Enter the port number for SSH connections ' +
                                'to the slider master',
                                22, validate_port),
    _DIR: SliderConfigItem('Enter the directory to install slider into on ' +
                           'the slider master:',
                           '/opt/slider', None),
    _APPNAME: SliderConfigItem('Enter a name for the presto slider app',
                               'presto', None),
    _SLIDER_USER: SliderConfigItem('Enter a user name for conducting slider ' +
                            'operations on the slider master',
                            'yarn', None),
    _JAVA_HOME: SliderConfigItem('Enter the value of JAVA_HOME to use when' +
                                 'running slider on the slider master:',
                                 '/usr/lib/jvm/java', None),
    _HADOOP_CONF: SliderConfigItem('Enter the location of the Hadoop ' +
                                   'configuration on the slider master:',
                                   '/etc/hadoop', None)
}


def requires_conf(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        get_conf_if_missing()
        func(*args, **kwargs)
    return wrapper


def write(conf):
    config.write(config.json_to_string(conf), SLIDER_CONFIG_PATH)


def get_conf_if_missing():
    with settings(parallel=False):
        if SLIDER_CONFIG_LOADED not in env:
            conf = {}
            try:
                conf = load_conf(SLIDER_CONFIG_PATH)
            except ConfigFileNotFoundError:
                conf = get_conf_interactive()
                store_conf(conf, SLIDER_CONFIG_PATH)

            set_env_from_conf(conf)
            env.SLIDER_CONFIG_LOADED = True

def load_conf(path):
    return config.get_conf_from_json_file(path)


def store_conf(conf, path):
    config.write(config.json_to_string(conf), path)

def get_conf_interactive():
    conf = {}
    prompt_related(conf, (_HOST, _SSH_PORT, _ADMIN_USER), _SLIDER_CONFIG,
                   validate_can_connect,
                   'Connection failed for %%(%s)s@%%(%s)s:%%(%s)d. Re-enter ' +
                   'connection information.')

    sci_prompt(conf, _DIR, _SLIDER_CONFIG)
    sci_prompt(conf, _APPNAME, _SLIDER_CONFIG)

    prompt_related(conf, (_HOST, _SSH_PORT, _ADMIN_USER, _SLIDER_USER),
                   _SLIDER_CONFIG, validate_can_sudo,
                   'Connection failed for %%(%s)s@%%(%s)s:%%(%s)d. Enter ' +
                   'a new username and try again.',
                   fixed_keys=[_HOST, _SSH_PORT, _ADMIN_USER])

    sci_prompt(conf, _JAVA_HOME, _SLIDER_CONFIG)
    sci_prompt(conf, _HADOOP_CONF, _SLIDER_CONFIG)
    return conf


def set_env_from_conf(conf):
    env.user = conf[_ADMIN_USER]
    env.port = conf[_SSH_PORT]
    env.roledefs[SLIDER_MASTER] = [conf[_HOST]]
    env.roledefs['all'] = env.roledefs[SLIDER_MASTER]

    env.slider_dir = conf[_DIR]

    # TODO: make this conditional once mode switching is done
    env.hosts = env.roledefs['all'][:]


def sci_prompt(conf, key, sci_map):
    sci = sci_map[key]
    conf[key] = prompt(sci.text, default=sci.default, validate=sci.validate)


def prompt_related(conf, keys, sci_map, validate, validate_failed_text,
                   fixed_keys=[]):
    while True:
        for key in keys:
            if key in fixed_keys:
                continue
            sci = sci_map[key]
            #
            # The first time through the loop, the default is the program
            # default. Subsequent times it is what the user has already entered.
            #
            conf[key] = prompt(sci.text, default=conf.get(key, sci.default),
                                 validate=sci.validate)
        if validate(conf, keys):
            break
        print (validate_failed_text % keys) % conf


def validate_can_connect(conf, keys):
    host, port, user = [conf[key] for key in keys]
    with settings(host_string='%s@%s:%d' % (user, host, port), user=user):
        return run('exit 0').succeeded

def validate_can_sudo(conf, keys):
    host, port, conn_user, sudo_user = [conf[key] for key in keys]
    with settings(host_string='%s@%s:%d' % (conn_user, host, port),
                  user=sudo_user):
        return sudo('exit 0', user=sudo_user).succeeded

