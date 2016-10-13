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
from fabric.context_managers import settings, hide
from fabric.operations import sudo
from fabric.tasks import execute
from prestoadmin.util.exception import ConfigurationError
from prestoadmin.util.constants import DEFAULT_PRESTO_LAUNCHER_LOG_FILE,\
    DEFAULT_PRESTO_SERVER_LOG_FILE, REMOTE_CONF_DIR, REMOTE_CATALOG_DIR
import prestoadmin.util.validators

_LOGGER = logging.getLogger(__name__)

NODE_CONFIG_FILE = REMOTE_CONF_DIR + '/node.properties'
GENERAL_CONFIG_FILE = REMOTE_CONF_DIR + '/config.properties'


def lookup_port(host):
    """
    Get the http port from config.properties http-server.http.port property
    if available.
    If the property is missing return default port 8080.
    If the file is missing or cannot parse the port number,
    throw ConfigurationError
    :param host:
    :return:
    """
    port = lookup_in_config('http-server.http.port', GENERAL_CONFIG_FILE, host)
    if not port:
        _LOGGER.info('Could not find property http-server.http.port.'
                     'Defaulting to 8080.')
        return 8080
    try:
        port = port.split('=', 1)[1]
        port = prestoadmin.util.validators.validate_port(port)
        _LOGGER.info('Looked up port ' + str(port) + ' on host ' +
                     host)
        return port
    except ConfigurationError as e:
        raise ConfigurationError(e.message +
                                 ' for property '
                                 'http-server.http.port on host ' +
                                 host + '.')


def lookup_server_log_file(host):
    try:
        return lookup_string_config('node.server-log-file', NODE_CONFIG_FILE,
                                    host, DEFAULT_PRESTO_SERVER_LOG_FILE)
    except:
        return DEFAULT_PRESTO_SERVER_LOG_FILE


def lookup_launcher_log_file(host):
    try:
        return lookup_string_config('node.launcher-log-file', NODE_CONFIG_FILE,
                                    host, DEFAULT_PRESTO_LAUNCHER_LOG_FILE)
    except:
        return DEFAULT_PRESTO_LAUNCHER_LOG_FILE


def lookup_catalog_directory(host):
    try:
        return lookup_string_config('catalog.config-dir', NODE_CONFIG_FILE,
                                    host, REMOTE_CATALOG_DIR)
    except:
        return REMOTE_CATALOG_DIR


def lookup_string_config(config_value, config_file, host, default=''):
    value = lookup_in_config(config_value, config_file, host)
    if value:
        return value.split('=', 1)[1]
    else:
        return default


def lookup_in_config(config_key, config_file, host):
    with settings(hide('stdout', 'warnings', 'aborts')):
        config_value = execute(sudo, 'grep %s= %s' % (config_key, config_file),
                               user='presto',
                               warn_only=True, host=host)[host]

    if isinstance(config_value, Exception) or config_value.return_code == 2:
        raise ConfigurationError('Could not access config file %s on '
                                 'host %s' % (config_file, host))

    return config_value
