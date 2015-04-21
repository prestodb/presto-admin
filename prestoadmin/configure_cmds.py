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
import logging
import os
from StringIO import StringIO
from fabric.contrib import files
from fabric.decorators import task
from fabric.operations import get
from fabric.state import env
from fabric.utils import abort, warn
from prestoadmin import configure
from prestoadmin.util import constants

CONFIG_PROPERTIES = "config.properties"
JVM_CONFIG = "jvm.config"
NODE_PROPERTIES = "node.properties"

_LOGGER = logging.getLogger(__name__)

__all__ = ['deploy', 'show']


@task
def deploy(rolename=None):
    """
    Deploy configuration on the remote hosts.

    By default, deploys configuration for all roles on the remote hosts
    Possible arguments:
        coordinator: Deploy the coordinator configuration to the coordinator
        node
        workers: Deploy workers configuration to the worker nodes. This will
        not deploy configuration for a coordinator that is also a worker
    :param rolename: [coordinator|workers]
    :return:
    """
    if rolename is None:
        _LOGGER.info("Running configure all")
        configure.coordinator()
        configure.workers()
    else:
        if rolename.lower() == 'coordinator':
            configure.coordinator()
        elif rolename.lower() == 'workers':
            configure.workers()
        else:
            abort("Invalid Argument. Possible values: coordinator, workers")


def configuration_show(file_name):
    file_path = os.path.join(constants.REMOTE_CONF_DIR, file_name)
    if not files.exists(file_path):
        warn("No configuration file found for %s at %s"
             % (env.host, file_path))
    else:
        print ("\n%s: Configuration file at %s:" % (env.host, file_path))
        file_content_buffer = StringIO()
        get(file_path, file_content_buffer)
        config_values = file_content_buffer.getvalue()
        file_content_buffer.close()
        print config_values


@task
def show(config=None):
    """
    Print to the user the contents of the configuration deployed

    It takes arguments: node jvm or config. If no arguments given. then
    prints out all the three
    :param config: [node|jvm|config]
    :return:
    """
    file_name = ''

    if config is None:
        configuration_show(NODE_PROPERTIES)
        configuration_show(JVM_CONFIG)
        configuration_show(CONFIG_PROPERTIES)
    else:
        if config.lower() == 'node':
            file_name = NODE_PROPERTIES
        elif config.lower() == 'jvm':
            file_name = JVM_CONFIG
        elif config.lower() == 'config':
            file_name = CONFIG_PROPERTIES
        else:
            abort("Invalid Argument. Possible values: node, jvm, config")

        configuration_show(file_name)
