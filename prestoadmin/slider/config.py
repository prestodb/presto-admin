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

import os

from fabric.state import env

from prestoadmin.util.base_config import BaseConfig, SingleConfigItem, \
    MultiConfigItem
from prestoadmin.util.constants import LOCAL_CONF_DIR
from prestoadmin.util.validators import validate_host, validate_port, \
    validate_username, validate_can_connect, validate_can_sudo

SLIDER_CONFIG_LOADED = 'slider_config_loaded'
SLIDER_CONFIG_PATH = os.path.join(LOCAL_CONF_DIR, 'slider', 'config.json')
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
                     '/etc/hadoop/conf', None)]


class SliderConfig(BaseConfig):

    def __init__(self):
        super(SliderConfig, self).__init__(SLIDER_CONFIG_PATH, _SLIDER_CONFIG)

    def is_config_loaded(self):
        return SLIDER_CONFIG_LOADED in env and env[SLIDER_CONFIG_LOADED]

    def set_config_loaded(self):
        env[SLIDER_CONFIG_LOADED] = True

    def set_env_from_conf(self, conf):
        env.user = conf[ADMIN_USER]
        env.port = conf[SSH_PORT]
        env.roledefs[SLIDER_MASTER] = [conf[HOST]]
        env.roledefs['all'] = env.roledefs[SLIDER_MASTER]

        env.conf = conf

        env.hosts = env.roledefs['all'][:]
