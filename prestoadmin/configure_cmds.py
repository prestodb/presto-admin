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
from fabric.operations import get, sudo
from fabric.state import env
from fabric.utils import abort, warn

import prestoadmin.deploy
from prestoadmin.standalone.config import StandaloneConfig
from prestoadmin.util import constants
from prestoadmin.util.base_config import requires_config
from prestoadmin.util.constants import CONFIG_PROPERTIES, LOG_PROPERTIES, \
    JVM_CONFIG, NODE_PROPERTIES

__all__ = ['show']

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


"""
gather/deploy_config_directory are used for server upgrade when we want to
preserve any existing configuration files across the upgrade exactly as they
were before the upgrade.

In order to preserve not just the data, but also the metadata, we tar up the
contents of /etc/presto to a temporary tar archive under /tmp. After the
upgrade, we untar it into /etc/presto and delete the archive.
"""


def gather_config_directory():
    """
    For the benefit of the next person to hack this, a list of some things
    that didn't work:
    - passing combine_stderr=False to sudo. Dunno why, still got them
    combined in the output.
    - using a StringIO object cfg = StringIO() and passing stdout=cfg. Got
    the host information at the start of the line.
    - sucking the tar archive over the network into memory instead of
    writing it out to a temporary file on the remote host. Since fabric
    doesn't provide a stdin= kwarg, there's no way to send back a tar
    archive larger than we can fit in a single bash command (~2MB on a
    good day), meaning if /etc/presto contains any large files, we'd end
    up having to send the archive to a temp file anyway.
    """
    result = sudo(
        'tarfile=`mktemp /tmp/presto_config-XXXXXXX.tar`; '
        'tar -c -z -C %s -f "${tarfile}" . && echo "${tarfile}"' % (
            constants.REMOTE_CONF_DIR,))
    return result


def deploy_config_directory(tarfile):
    sudo('tar -C "%s" -x -v -f "%s" ; rm "%s"' %
         (constants.REMOTE_CONF_DIR, tarfile, tarfile))


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
