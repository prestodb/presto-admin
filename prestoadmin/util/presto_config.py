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
import os
from StringIO import StringIO

from fabric.context_managers import settings, hide
from fabric.operations import get, run
from fabric.state import env
from fabric.utils import error

from prestoadmin.config import get_conf_from_properties_data
from prestoadmin.util.constants import REMOTE_CONF_DIR, CONFIG_PROPERTIES

HTTP_ENABLED_KEY = 'http-server.http.enabled'
HTTPS_ENABLED_KEY = 'http-server.https.enabled'
HTTP_PORT_KEY = 'http-server.http.port'
HTTPS_PORT_KEY = 'http-server.https.port'
CLIENT_KEYSTORE_PATH_KEY = 'internal-communication.https.keystore.path'
CLIENT_KEYSTORE_PASSWORD_KEY = 'internal-communication.https.keystore.key'
AUTHENTICATION_KEY = 'http-server.authentication.type'
LDAP_CLIENT_USER_KEY = 'internal-communication.authentication.ldap.user'
LDAP_CLIENT_PASSWORD_KEY = 'internal-communication.authentication.ldap.password'

_LOGGER = logging.getLogger(__name__)
# properties file literals
PROPERTIES_TRUE = 'true'
PROPERTIES_FALSE = 'false'


class PrestoConfig:
    # Defaults from Presto
    default_config = {
        HTTP_ENABLED_KEY: PROPERTIES_TRUE,
        HTTPS_ENABLED_KEY: PROPERTIES_FALSE,
        HTTP_PORT_KEY: '8080',
        HTTPS_PORT_KEY: '8443',
        CLIENT_KEYSTORE_PATH_KEY: None,
        CLIENT_KEYSTORE_PASSWORD_KEY: None,
        LDAP_CLIENT_USER_KEY: None,
        LDAP_CLIENT_PASSWORD_KEY: None
    }

    def __init__(self, config_properties, config_path, config_host):
        self.config_path = config_path
        self.config_host = config_host
        if not config_properties:
            self.config_properties = self.default_config
        else:
            self.config_properties = config_properties

    @staticmethod
    def from_file(config, config_path=None, config_host=None):
        presto_config_dict = get_conf_from_properties_data(config)
        return PrestoConfig(presto_config_dict, config_path, config_host)

    @staticmethod
    def coordinator_config():
        config_path = os.path.join(REMOTE_CONF_DIR, CONFIG_PROPERTIES)
        config_host = env.roledefs['coordinator'][0]
        try:
            data = StringIO()
            with settings(host_string='%s@%s' % (env.user, config_host)):
                with hide('stderr', 'stdout'):
                    temp_dir = run('mktemp -d /tmp/prestoadmin.XXXXXXXXXXXXXX')
                try:
                    get(config_path, data, use_sudo=True, temp_dir=temp_dir)
                finally:
                    run('rm -rf %s' % temp_dir)

            data.seek(0)
            return PrestoConfig.from_file(data, config_path, config_host)
        except:
            _LOGGER.info('Could not find Presto config.')
            return PrestoConfig(None, config_path, config_host)

    def _lookup(self, key):
        result = self.config_properties.get(key, self.default_config[key])
        if not result:
            error(
                    "Key %s is not configured in coordinator configuration"
                    "%s on host %s and has no default" %
                    (key, self.config_host, self.config_path))
        return result

    def use_https(self):
        http_enabled = self._lookup(HTTP_ENABLED_KEY) == PROPERTIES_TRUE
        https_enabled = self._lookup(HTTPS_ENABLED_KEY) == PROPERTIES_TRUE

        return https_enabled and not http_enabled

    def get_client_keystore_path(self):
        return self._lookup(CLIENT_KEYSTORE_PATH_KEY)

    def get_client_keystore_password(self):
        return self._lookup(CLIENT_KEYSTORE_PASSWORD_KEY)

    def get_https_port(self):
        return int(self._lookup(HTTPS_PORT_KEY))

    def get_http_port(self):
        return int(self._lookup(HTTP_PORT_KEY))

    def use_ldap(self):
        if not self.use_https():
            return False

        if AUTHENTICATION_KEY in self.config_properties:
            return self.config_properties[AUTHENTICATION_KEY] == 'LDAP'
        return False

    def get_ldap_user(self):
        return self._lookup(LDAP_CLIENT_USER_KEY)

    def get_ldap_password(self):
        return self._lookup(LDAP_CLIENT_PASSWORD_KEY)
