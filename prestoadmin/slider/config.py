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

from functools import wraps
import os

from fabric.context_managers import settings
from fabric.state import env
from fabric.operations import prompt
from fabric.tasks import execute

from prestoadmin import config
from prestoadmin.config import ConfigFileNotFoundError

from prestoadmin.util.constants import LOCAL_CONF_DIR
from prestoadmin.util.validators import validate_host, validate_port, \
    validate_username, validate_can_connect, validate_can_sudo

SLIDER_CONFIG_LOADED = 'slider_config_loaded'
SLIDER_CONFIG_DIR = os.path.join(LOCAL_CONF_DIR, 'slider')
SLIDER_CONFIG_PATH = os.path.join(SLIDER_CONFIG_DIR, 'config.json')
SLIDER_MASTER = 'slider_master'

HOST = 'slider_master'
ADMIN_USER = 'admin'
SSH_PORT = 'ssh_port'

DIR = 'slider_directory'
APPNAME = 'slider_appname'
INSTANCE_NAME = 'slider_instname'
SLIDER_USER = 'slider_user'
JAVA_HOME = 'JAVA_HOME'
HADOOP_CONF = 'HADOOP_CONF'

# This key comes from the server install step, NOT a user prompt. Accordingly,
# there is no SliderConfigItem for it in _SLIDER_CONFIG
PRESTO_PACKAGE = 'presto_slider_package'


class SingleConfigItem(object):
    def __init__(self, key, prompt, default=None, validate=None):
        self.key = key
        self.prompt = prompt
        self.default = default
        self.validate = validate

    def prompt_user(self, conf):
        conf[self.key] = prompt(self.prompt,
                                default=conf.get(self.key, self.default),
                                validate=self.validate)

    def collect_prompts(self, l):
        l.append((self.prompt, self.key))


class MultiConfigItem(object):
    def __init__(self, items, validate, validate_keys,
                 validate_failed_text):
        self.items = items
        self.validate = validate
        self.validate_keys = validate_keys
        self.validate_failed_text = validate_failed_text

    def prompt_user(self, conf):
        while True:
            for item in self.items:
                item.prompt_user(conf)

            validate_args = [conf[k] for k in self.validate_keys]
            if self.validate(*validate_args):
                break
            print (self.validate_failed_text % self.validate_keys) % conf

    def collect_prompts(self, l):
        for item in self.items:
            item.collect_prompts(l)


_SLIDER_CONFIG = [
    MultiConfigItem([
        SingleConfigItem(HOST, 'Enter the hostname for the slider master:',
                         'localhost', validate_host),
        SingleConfigItem(ADMIN_USER, 'Enter the user name to use when ' +
                         'installing slider on the slider master:',
                         'root', validate_username),
        SingleConfigItem(SSH_PORT, 'Enter the port number for SSH ' +
                         'connections to the slider master', 22,
                         validate_port)],
                    validate_can_connect, (ADMIN_USER, HOST, SSH_PORT),
                    'Connection failed for %%(%s)s@%%(%s)s:%%(%s)d. ' +
                    'Re-enter connection information.'),

    SingleConfigItem(DIR, 'Enter the directory to install slider into on ' +
                     'the slider master:', '/opt/slider', None),

    MultiConfigItem([
        SingleConfigItem(SLIDER_USER, 'Enter a user name for conducting ' +
                         'slider operations on the slider master ', 'yarn',
                         validate_username)],
                    validate_can_sudo,
                    (SLIDER_USER, ADMIN_USER, HOST, SSH_PORT),
                    'Failed to sudo to user %%(%s)s while connecting as ' +
                    '%%(%s)s@%%(%s)s:%%(%s)d. Enter a new username and try' +
                    'again.'),

    SingleConfigItem(JAVA_HOME, 'Enter the value of JAVA_HOME to use when' +
                     'running slider on the slider master:',
                     '/usr/lib/jvm/java', None),
    SingleConfigItem(HADOOP_CONF, 'Enter the location of the Hadoop ' +
                     'configuration on the slider master:',
                     '/etc/hadoop/conf', None),
    SingleConfigItem(APPNAME, 'Enter a name for the presto slider application',
                     'PRESTO', None)]


def requires_conf(task):
    @wraps(task)
    def wrapper(*args, **kwargs):
        if get_conf_if_missing():
            # If the config wasn't already loaded, we have to execute() the
            # task so that Fabric regenerates the host list and other env stuff
            # that govern execution based on the changes to env that loading
            # the config caused.
            return execute(task, *args, **kwargs)
        else:
            # If the config was already loaded, we can call task() directly.
            return task(*args, **kwargs)
    return wrapper


def write(conf):
    config.write(config.json_to_string(conf), SLIDER_CONFIG_PATH)


def get_conf_if_missing():
    """ Loads the configuration if it hasn't already been loaded. Returns True
    if the config was loaded for the first time, and False otherwise.
    """
    with settings(parallel=False):
        if SLIDER_CONFIG_LOADED in env and env[SLIDER_CONFIG_LOADED]:
            return False

        conf = {}
        try:
            conf = load_conf(SLIDER_CONFIG_PATH)
        except ConfigFileNotFoundError:
            conf = get_conf_interactive()
            store_conf(conf, SLIDER_CONFIG_PATH)

        set_env_from_conf(conf)
        env.SLIDER_CONFIG_LOADED = True
        return True


def load_conf(path):
    return config.get_conf_from_json_file(path)


def store_conf(conf, path):
    config.write(config.json_to_string(conf), path)


def get_conf_interactive():
    conf = {}
    for item in _SLIDER_CONFIG:
        item.prompt_user(conf)
    return conf


def set_env_from_conf(conf):
    env.user = conf[ADMIN_USER]
    env.port = conf[SSH_PORT]
    env.roledefs[SLIDER_MASTER] = [conf[HOST]]
    env.roledefs['all'] = env.roledefs[SLIDER_MASTER]

    env.conf = conf

    # TODO: make this conditional once mode switching is done
    env.hosts = env.roledefs['all'][:]
