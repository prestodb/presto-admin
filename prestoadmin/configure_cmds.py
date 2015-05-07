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
Module for various configuration management tasks using presto-admin
"""
import logging
import os
from StringIO import StringIO
from fabric.contrib import files
from fabric.decorators import task, serial
from fabric.operations import get
from fabric.state import env
from fabric.utils import abort, warn
from prestoadmin import configure
from prestoadmin.topology import requires_topology
from prestoadmin.util import constants

CONFIG_PROPERTIES = "config.properties"
LOG_PROPERTIES = "log.properties"
JVM_CONFIG = "jvm.config"
NODE_PROPERTIES = "node.properties"

_LOGGER = logging.getLogger(__name__)

__all__ = ['deploy', 'show']


@task
@requires_topology
def deploy(rolename=None):
    """
    Deploy configuration on the remote hosts.

    Possible arguments are -
        coordinator - Deploy the coordinator configuration to the coordinator
        node
        workers - Deploy workers configuration to the worker nodes. This will
        not deploy configuration for a coordinator that is also a worker

    If no rolename is specified, then configuration for all roles will be
    deployed

    Parameters:
        rolename - [coordinator|workers]
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
        file_content_buffer = StringIO()
        get(file_path, file_content_buffer)
        config_values = file_content_buffer.getvalue()
        file_content_buffer.close()
        print ("\n%s: Configuration file at %s:" % (env.host, file_path))
        print config_values


@task
@serial
def show(config_type=None):
    """
    Print to the user the contents of the configuration deployed

    Possible arguments are node,jvm,config or log.

    If no config_type is specified, then all four configurations will be
    printed

    Parameters:
        config_type: [node|jvm|config|log]
    """
    file_name = ''
    if config_type is None:
        configuration_show(NODE_PROPERTIES)
        configuration_show(JVM_CONFIG)
        configuration_show(CONFIG_PROPERTIES)
        configuration_show(LOG_PROPERTIES)
    else:
        if config_type.lower() == 'node':
            file_name = NODE_PROPERTIES
        elif config_type.lower() == 'jvm':
            file_name = JVM_CONFIG
        elif config_type.lower() == 'config':
            file_name = CONFIG_PROPERTIES
        elif config_type.lower() == 'log':
            file_name = LOG_PROPERTIES
        else:
            abort("Invalid Argument. Possible values: node, jvm, config, log")

        configuration_show(file_name)
