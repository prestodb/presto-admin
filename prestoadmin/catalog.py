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
Module for presto catalog configurations
"""
import errno
import logging

import fabric.utils
from fabric.api import task, env
from fabric.context_managers import hide
from fabric.contrib import files
from fabric.operations import sudo, os, get

from prestoadmin.deploy import secure_create_directory
from prestoadmin.standalone.config import StandaloneConfig, \
    PRESTO_STANDALONE_USER_GROUP
from prestoadmin.util import constants
from prestoadmin.util.base_config import requires_config
from prestoadmin.util.exception import ConfigFileNotFoundError, \
    ConfigurationError
from prestoadmin.util.fabricapi import put_secure
from prestoadmin.util.filesystem import ensure_directory_exists
from prestoadmin.util.local_config_util import get_catalog_directory

_LOGGER = logging.getLogger(__name__)

__all__ = ['add', 'remove']
COULD_NOT_REMOVE = 'Could not remove catalog'


# we deploy catalog files with 0600 permissions because they can contain passwords
# that should not be world readable
def deploy_files(filenames, local_dir, remote_dir, user_group, mode=0600):
    _LOGGER.info('Deploying configurations for ' + str(filenames))
    secure_create_directory(remote_dir, PRESTO_STANDALONE_USER_GROUP)
    for name in filenames:
        put_secure(user_group, mode, os.path.join(local_dir, name), remote_dir,
                   use_sudo=True)


def gather_catalogs(local_config_dir, allow_overwrite=False):
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
        file_path = os.path.join(get_catalog_directory(), name)
        _LOGGER.info('Validating catalog configuration: ' + str(name))
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
    Deploy configuration for a catalog onto a cluster.

    E.g.: 'presto-admin catalog add tpch'
    deploys a configuration file for the tpch connector.  The configuration is
    defined by tpch.properties in the local catalog directory, which defaults to
    ~/.prestoadmin/catalog.

    If no catalog name is specified, then  configurations for all catalogs
    in the catalog directory will be deployed

    Parameters:
        name - Name of the catalog to be added
    """
    catalog_dir = get_catalog_directory()
    if name:
        filename = name + '.properties'
        config_path = os.path.join(catalog_dir, filename)
        if not os.path.isfile(config_path):
            raise ConfigFileNotFoundError(
                config_path=config_path,
                message='Configuration for catalog ' + name + ' not found')
        filenames = [filename]
    elif not os.path.isdir(catalog_dir):
        message = ('Cannot add catalogs because directory %s does not exist'
                   % catalog_dir)
        raise ConfigFileNotFoundError(config_path=catalog_dir,
                                      message=message)
    else:
        try:
            filenames = os.listdir(catalog_dir)
        except OSError as e:
            fabric.utils.error(e.strerror)
            return
        if not filenames:
            fabric.utils.warn(
                'Directory %s is empty. No catalogs will be deployed' %
                catalog_dir)
            return

    if not validate(filenames):
        return
    filenames.sort()
    _LOGGER.info('Adding catalog configurations: ' + str(filenames))
    print('Deploying %s catalog configurations on: %s ' %
          (', '.join(filenames), env.host))

    deploy_files(filenames, catalog_dir,
                 constants.REMOTE_CATALOG_DIR, PRESTO_STANDALONE_USER_GROUP)


@task
@requires_config(StandaloneConfig)
def remove(name):
    """
    Remove a catalog from the cluster.

    Parameters:
        name - Name of the catalog to be removed
    """
    _LOGGER.info('[' + env.host + '] Removing catalog: ' + name)
    ret = remove_file(os.path.join(constants.REMOTE_CATALOG_DIR,
                                   name + '.properties'))
    if ret.succeeded:
        if COULD_NOT_REMOVE in ret:
            fabric.utils.error(ret)
        else:
            print('[%s] Catalog removed. Restart the server for the change '
                  'to take effect' % env.host)
    else:
        fabric.utils.error('Failed to remove catalog ' + name + '.\n\t' +
                           ret)

    local_path = os.path.join(get_catalog_directory(), name + '.properties')
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
