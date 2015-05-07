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
Module for presto connector configurations
"""
from fabric.api import task, env
from fabric.operations import sudo, os

from prestoadmin.util import constants
import config
import configure
import logging
import fabric.utils

_LOGGER = logging.getLogger(__name__)

__all__ = ['add', 'remove']


@task
def add(name=None):
    """
    Deploy configuration for a connector onto a cluster.

    E.g.: 'presto-admin connector add tpch'
    deploys a configuration file for the tpch connector.  The configuration is
    defined by tpch.properties in connectors.json.

    If no connector name is specified, then  configurations for all connectors
    in the connector directory will be deployed

    Parameters:
        name - Name of the connector to be added
    """
    if name:
        filename = name + ".properties"
        if not os.path.isfile(
                os.path.join(constants.CONNECTORS_DIR, filename)):
            raise config.ConfigFileNotFoundError(
                "Configuration for connector " + name + " not found")
        filenames = [filename]
    elif not os.path.isdir(constants.CONNECTORS_DIR):
        message = ("Cannot add connectors because directory %s does not exist"
                   % constants.CONNECTORS_DIR)
        raise config.ConfigFileNotFoundError(message)
    else:
        filenames = os.listdir(constants.CONNECTORS_DIR)
        if not filenames:
            fabric.utils.warn(
                "Directory %s is empty. No connectors will be deployed" %
                constants.CONNECTORS_DIR)

    _LOGGER.info("Adding connector configurations: " + str(filenames))

    configure.deploy(filenames, constants.CONNECTORS_DIR,
                     constants.REMOTE_CATALOG_DIR)


@task
def remove(name):
    """
    Remove a connector from the cluster.

    Parameters:
        name - Name of the connector to be removed
    """
    _LOGGER.info('[' + env.host + '] Removing connector: ' + name)
    ret = remove_file(os.path.join(constants.REMOTE_CATALOG_DIR,
                                   name + '.properties'))
    if ret.succeeded:
        print('Connector removed. Restart the server for the change to take '
              'effect')
    else:
        print('Failed to remove connector ' + name + '.')

    local_path = os.path.join(constants.CONNECTORS_DIR, name + ".properties")
    if os.path.exists(local_path):
        os.remove(local_path)


def remove_file(name):
    return sudo("rm " + name)
