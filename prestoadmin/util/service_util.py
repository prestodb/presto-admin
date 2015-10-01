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
from fabric.operations import run
from fabric.tasks import execute
from prestoadmin import topology
from prestoadmin.util.exception import ConfigurationError
from prestoadmin.util import constants
import prestoadmin.util.validators

_LOGGER = logging.getLogger(__name__)


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
    config_file = constants.REMOTE_CONF_DIR + '/config.properties'
    with settings(hide('stdout', 'warnings', 'aborts')):
        try:
            port = execute(run, 'grep http-server.http.port= ' + config_file,
                           warn_only=True, host=host)[host]
        except:
            raise ConfigurationError('Configuration file %s does not exist on '
                                     'host %s' % (config_file, host))

    if isinstance(port, Exception):
        raise ConfigurationError('Configuration file %s does not exist on '
                                 'host %s' % (config_file, host))
    else:
        if str(port) is '':
            _LOGGER.info('Could not find property http-server.http.port.'
                         'Defaulting to 8080.')
            return 8080
        try:
            port = port.split('=', 1)[1]
            port = int(port)
            prestoadmin.util.validators.validate_port(str(port))
            _LOGGER.info('Looked up port ' + str(port) + ' on host '
                         + host)
            return port
        except ValueError:
            raise ConfigurationError('Unable to coerce http-server.http'
                                     '.port \'%s\' to an int. Failed to '
                                     'connect to %s.' % (port, host))
        except ConfigurationError as e:
            raise ConfigurationError(e.message +
                                     ' for property '
                                     'http-server.http.port on host '
                                     + host + '.')
