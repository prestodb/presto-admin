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
from fabric.contrib import files
from fabric.operations import sudo, os, get
import fabric.utils

from prestoadmin.standalone.config import StandaloneConfig, \
    PRESTO_STANDALONE_USER_GROUP
from prestoadmin.util import constants
from prestoadmin.util.base_config import requires_config
from prestoadmin.util.exception import ConfigFileNotFoundError, \
    ConfigurationError
from prestoadmin.util.fabricapi import put_secure
from prestoadmin.util.filesystem import ensure_directory_exists

_LOGGER = logging.getLogger(__name__)

__all__ = ['add', 'remove']
COULD_NOT_REMOVE = 'Could not remove connector'


def deploy_files(filenames, local_dir, remote_dir, user_group, mode=0600):
    _LOGGER.info('Deploying configurations for ' + str(filenames))
    sudo('mkdir -p ' + remote_dir)
    for name in filenames:
        put_secure(user_group, mode, os.path.join(local_dir, name), remote_dir,
                   use_sudo=True)


def gather_connectors(local_config_dir, allow_overwrite=False):
    local_catalog_dir = os.path.join(local_config_dir, env.host, 'catalog')
    if not allow_overwrite and os.path.exists(local_catalog_dir):
        fabric.utils.error("Refusing to overwrite %s. Use 'overwrite' "
                           "option to overwrite." % local_catalog_dir)
    ensure_directory_exists(local_catalog_dir)
    if files.exists(constants.REMOTE_CATALOG_DIR):
        return get(constants.REMOTE_CATALOG_DIR, local_catalog_dir, use_sudo=True)
    else:
        return []


def validate(filenames):
    for name in filenames:
        file_path = os.path.join(constants.CONNECTORS_DIR, name)
        _LOGGER.info('Validating connector configuration: ' + str(name))
        try:
            with open(file_path) as f:
                file_content = f.read()
            if 'connector.name' not in file_content:
                message = ('Catalog configuration %s does not contain '
                           'connector.name' % name)
                raise ConfigurationError(message)

        except IOError, e:
            fabric.utils.error(message='Error validating ' + file_path,
                               exception=e)
            return False

    return True


@task
@requires_config(StandaloneConfig)
def add(name=None):
    """
    Deploy configuration for a connector onto a cluster.

    E.g.: 'presto-admin connector add tpch'
    deploys a configuration file for the tpch connector.  The configuration is
    defined by tpch.properties in /etc/opt/prestoadmin/connectors directory.

    If no connector name is specified, then  configurations for all connectors
    in the connectors directory will be deployed

    Parameters:
        name - Name of the connector to be added
    """
    if name:
        filename = name + '.properties'
        config_path = os.path.join(constants.CONNECTORS_DIR, filename)
        if not os.path.isfile(config_path):
            raise ConfigFileNotFoundError(
                config_path=config_path,
                message='Configuration for connector ' + name + ' not found')
        filenames = [filename]
    elif not os.path.isdir(constants.CONNECTORS_DIR):
        message = ('Cannot add connectors because directory %s does not exist'
                   % constants.CONNECTORS_DIR)
        raise ConfigFileNotFoundError(config_path=constants.CONNECTORS_DIR,
                                      message=message)
    else:
        try:
            filenames = os.listdir(constants.CONNECTORS_DIR)
        except OSError as e:
            fabric.utils.error(e.strerror)
            return
        if not filenames:
            fabric.utils.warn(
                'Directory %s is empty. No connectors will be deployed' %
                constants.CONNECTORS_DIR)
            return

    if not validate(filenames):
        return
    filenames.sort()
    _LOGGER.info('Adding connector configurations: ' + str(filenames))
    print('Deploying %s connector configurations on: %s ' %
          (', '.join(filenames), env.host))

    deploy_files(filenames, constants.CONNECTORS_DIR,
                 constants.REMOTE_CATALOG_DIR, PRESTO_STANDALONE_USER_GROUP)


@task
@requires_config(StandaloneConfig)
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
            fabric.utils.error(ret)
        else:
            print('[%s] Connector removed. Restart the server for the change '
                  'to take effect' % env.host)
    else:
        fabric.utils.error('Failed to remove connector ' + name + '.\n\t' +
                           ret)

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
