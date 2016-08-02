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
from contextlib import closing
from fabric.contrib import files
from fabric.decorators import task, serial
from fabric.operations import get
from fabric.state import env
from fabric.utils import abort, warn
import prestoadmin.deploy
from prestoadmin.standalone.config import StandaloneConfig, \
    PRESTO_STANDALONE_USER_GROUP
from prestoadmin.util import constants
from prestoadmin.util.base_config import requires_config
from prestoadmin.util.fabricapi import put_secure
from prestoadmin.util.filesystem import ensure_parent_directories_exist

__all__ = ['show']


CONFIG_PROPERTIES = "config.properties"
LOG_PROPERTIES = "log.properties"
JVM_CONFIG = "jvm.config"
NODE_PROPERTIES = "node.properties"

ALL_CONFIG = [CONFIG_PROPERTIES, LOG_PROPERTIES, JVM_CONFIG, NODE_PROPERTIES]

_LOGGER = logging.getLogger(__name__)

__all__ = ['deploy', 'show']


@task
@requires_config(StandaloneConfig)
def deploy(rolename=None):
    """
    Deploy configuration on the remote hosts.

    Possible arguments are -
        coordinator - Deploy the coordinator configuration to the coordinator
        node
        workers - Deploy workers configuration to the worker nodes. This will
        not deploy configuration for a coordinator that is also a worker

    If no rolename is specified, then configuration for all roles will be
    deployed.  If there is no presto configuration file found in the
    configuration directory, default files will be deployed

    Parameters:
        rolename - [coordinator|workers]
    """
    if rolename is None:
        _LOGGER.info("Running configuration deploy")
        prestoadmin.deploy.coordinator()
        prestoadmin.deploy.workers()
    else:
        if rolename.lower() == 'coordinator':
            prestoadmin.deploy.coordinator()
        elif rolename.lower() == 'workers':
            prestoadmin.deploy.workers()
        else:
            abort("Invalid Argument. Possible values: coordinator, workers")


def gather_directory(target_directory, allow_overwrite=False):
    fetch_all(target_directory, allow_overwrite=allow_overwrite)


def deploy_all(source_directory, should_warn=True):
    host_config_dir = os.path.join(source_directory, env.host)
    for file_name in ALL_CONFIG:
        local_config_file = os.path.join(host_config_dir, file_name)
        if not os.path.exists(local_config_file):
            if should_warn:
                warn("No configuration file found for %s at %s"
                     % (env.host, local_config_file))
            continue
        remote_config_file = os.path.join(constants.REMOTE_CONF_DIR, file_name)
        put_secure(PRESTO_STANDALONE_USER_GROUP, 0600, local_config_file,
                   remote_config_file, use_sudo=True)


def fetch_all(target_directory, allow_overwrite=False):
    host_config_dir = os.path.join(target_directory, env.host)
    for file_name in ALL_CONFIG:
        local_config_file = os.path.join(host_config_dir, file_name)
        ensure_parent_directories_exist(local_config_file)
        if not allow_overwrite and os.path.exists(local_config_file):
            abort("Refusing to overwrite %s. Use 'overwrite' parameter "
                  "to overwrite." % (local_config_file))
        configuration_fetch(file_name, local_config_file)


def configuration_fetch(file_name, config_destination, should_warn=True):
    remote_file_path = os.path.join(constants.REMOTE_CONF_DIR, file_name)
    if not files.exists(remote_file_path):
        if should_warn:
            warn("No configuration file found for %s at %s"
                 % (env.host, remote_file_path))
        return None
    else:
        get(remote_file_path, config_destination, use_sudo=True)
        return remote_file_path


def configuration_show(file_name, should_warn=True):
    with closing(StringIO()) as file_content_buffer:
        file_path = configuration_fetch(file_name, file_content_buffer,
                                        should_warn)
        if file_path is None:
            return
        config_values = file_content_buffer.getvalue()
        file_content_buffer.close()
        print ("\n%s: Configuration file at %s:" % (env.host, file_path))
        print config_values


@task
@requires_config(StandaloneConfig)
@serial
def show(config_type=None):
    """
    Print to the user the contents of the configuration files deployed

    If no config_type is specified, then all four configurations will be
    printed.  No warning will be printed for a missing log.properties since
    it is not a required configuration file.

    Parameters:
        config_type: [node|jvm|config|log]
    """
    file_name = ''
    if config_type is None:
        configuration_show(NODE_PROPERTIES)
        configuration_show(JVM_CONFIG)
        configuration_show(CONFIG_PROPERTIES)
        configuration_show(LOG_PROPERTIES, should_warn=False)
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
