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
import logging
import errno

from fabric.api import task, env
from fabric.context_managers import hide
from fabric.operations import sudo, os, put
import fabric.utils

from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigFileNotFoundError, \
    ConfigurationError

_LOGGER = logging.getLogger(__name__)

__all__ = ['add', 'remove']
COULD_NOT_REMOVE = 'Could not remove connector'


def deploy_files(filenames, local_dir, remote_dir):
    _LOGGER.info('Deploying configurations for ' + str(filenames))
    sudo('mkdir -p ' + remote_dir)
    for name in filenames:
        put(os.path.join(local_dir, name), remote_dir, use_sudo=True)


def validate(filenames):
    for name in filenames:
        file_path = os.path.join(constants.CONNECTORS_DIR, name)
        _LOGGER.info('Validating connector configuration: ' + str(name))
        with open(file_path) as f:
            file_content = f.read()
            if 'connector.name' not in file_content:
                message = ('Catalog configuration %s does not contain '
                           'connector.name' % name)
                raise ConfigurationError(message)


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
        filename = name + '.properties'
        if not os.path.isfile(
                os.path.join(constants.CONNECTORS_DIR, filename)):
            raise ConfigFileNotFoundError(
                'Configuration for connector ' + name + ' not found')
        filenames = [filename]
    elif not os.path.isdir(constants.CONNECTORS_DIR):
        message = ('Cannot add connectors because directory %s does not exist'
                   % constants.CONNECTORS_DIR)
        raise ConfigFileNotFoundError(message)
    else:
        try:
            filenames = os.listdir(constants.CONNECTORS_DIR)
            validate(filenames)
        except OSError as e:
            fabric.utils.warn(e.strerror)
            return
        if not filenames:
            fabric.utils.warn(
                'Directory %s is empty. No connectors will be deployed' %
                constants.CONNECTORS_DIR)
            return

    _LOGGER.info('Adding connector configurations: ' + str(filenames))
    print('Deploying %s connector configurations on: %s ' %
          (', '.join(filenames), env.host))

    deploy_files(filenames, constants.CONNECTORS_DIR,
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
        if COULD_NOT_REMOVE in ret:
            fabric.utils.warn(ret)
        else:
            print('[%s] Connector removed. Restart the server for the change '
                  'to take effect' % env.host)
    else:
        fabric.utils.warn('Failed to remove connector ' + name + '.\n\t'
                          + ret)

    local_path = os.path.join(constants.CONNECTORS_DIR, name + '.properties')
    try:
        os.remove(local_path)
    except OSError as e:
        if e.errno == errno.ENOENT:
            pass
        else:
            raise


def remove_file(path):

    script = ('if [ -f %(path)s ] ; '
              'then rm %(path)s ; '
              'else echo "%(could_not_remove)s \'%(name)s\'. '
              'No such file \'%(path)s\'"; fi')

    with hide('stderr', 'stdout'):
        return sudo(script %
                    {'path': path,
                     'name': os.path.splitext(os.path.basename(path))[0],
                     'could_not_remove': COULD_NOT_REMOVE})
