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

from fabric.api import task
from fabric.operations import sudo, os

from prestoadmin.util import constants
import configuration
import configure
import logging

_LOGGER = logging.getLogger(__name__)

__all__ = ['add', 'remove']


@task
def add(name=None):
    """
    Deploy configuration for a connector onto a cluster. Use the name argument
    to add a particular connector.

    E.g.: 'presto-admin connector add tpch'
    deploys a configuration file for the tpch connector.  The configuration is
    defined by tpch.properties in connectors.json.

    If no connector name is specified, then  configurations for all connectors
    in connectors.json will be deployed.
    """
    conf = configuration.get_conf_from_file(constants.CONNECTORS_CONFIG_FILE)
    if name:
        prop = name + ".properties"
        try:
            value = conf[prop]
            conf = {prop: value}
        except KeyError:
            raise configuration.ConfigurationError(
                "Configuration for connector " + name + " not found")
    _LOGGER.info("Adding connector configurations: " + str(conf.keys()))
    configure.configure(conf, constants.TMP_CONF_DIR,
                        constants.REMOTE_CATALOG_DIR)


@task
def remove(name):
    """
    Remove a connector from the cluster.
    Usage: presto-admin connector remove <name>
    """
    _LOGGER.info("Removing connector: " + name)
    remove_file(os.path.join(constants.REMOTE_CATALOG_DIR,
                             name + ".properties"))
    print "Connector removed. Restart the server for the change to take effect"


def remove_file(name):
    sudo("rm " + name)
