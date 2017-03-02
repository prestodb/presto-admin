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
Simple client to communicate with a Presto server.
"""
import json
import logging
import os
import socket
import urlparse
from httplib import HTTPConnection, HTTPException
from tempfile import mkstemp

from StringIO import StringIO
from fabric.operations import get
from fabric.state import env
from fabric.utils import error
from jks import jks, base64, textwrap
from prestoadmin.util.constants import REMOTE_CONF_DIR, CONFIG_PROPERTIES
from prestoadmin.util.exception import InvalidArgumentError
from prestoadmin.util.httpscacertconnection import HTTPSCaCertConnection
from prestoadmin.util.local_config_util import get_coordinator_directory, get_topology_path
from prestoadmin.util.presto_config import PrestoConfig, LDAP_CLIENT_USER_KEY, LDAP_CLIENT_PASSWORD_KEY

_LOGGER = logging.getLogger(__name__)
URL_TIMEOUT_MS = 5000
NUM_ROWS = 1000
DATA_RESP = "data"
NEXT_URI_RESP = "nextUri"

CERTIFICATE_ALIAS = 'certificate_alias'


class PrestoClient:
    def __init__(self, server, user, coordinator_config=None):
        # immutable stuff
        self.server = server
        self.user = user
        if (coordinator_config is None):
            coordinator_config = PrestoConfig.coordinator_config()
        self.coordinator_config = coordinator_config
        self.port = PrestoClient._get_configured_port(self.coordinator_config)

        # mutable stuff
        self.ca_file_path = ""
        self.keystore_data = ""
        self.rows = []
        self.next_uri = ''
        self.response_from_server = {}

    @staticmethod
    def _remove_silently(path):
        try:
            os.remove(path)
        except:
            pass

    def close(self):
        PrestoClient._remove_silently(self.ca_file_path)

    def _clear_old_results(self):
        if self.rows:
            self.rows = []

        if self.next_uri:
            self.next_uri = ''

        if self.response_from_server:
            self.response_from_server = {}

    def run_sql(self, sql, schema="default", catalog="hive"):
        """
        Execute a query connecting to Presto server using passed parameters.

        Args:
            sql: SQL query to be executed
            schema: Presto schema to be used while executing query
                (default=default)
            catalog: Catalog to be used by the server

        Returns:
            list of rows or None if client was unable to connect to Presto
        """
        status = self._execute_query(sql, schema, catalog)
        if status:
            return self._get_rows()
        else:
            return None

    def _execute_query(self, sql, schema, catalog):
        if not sql:
            raise InvalidArgumentError("SQL query missing")

        if not self.server:
            raise InvalidArgumentError("Server IP missing")

        if not self.user:
            raise InvalidArgumentError("Username missing")

        self._clear_old_results()

        headers = {"X-Presto-Catalog": catalog,
                   "X-Presto-Schema": schema,
                   "X-Presto-User": self.user,
                   "X-Presto-Source": "presto-admin"}
        answer = ''
        try:
            _LOGGER.info("Connecting to server at: " + self.server +
                         ":" + str(self.port) + " as user " + self.user +
                         " to execute query " + sql)
            conn = self._get_connection()
            self._add_auth_headers(headers)
            conn.request("POST", "/v1/statement", sql, headers)
            response = conn.getresponse()

            if response.status != 200:
                conn.close()
                _LOGGER.error("Connection error: " +
                              str(response.status) + " " + response.reason)
                return False

            answer = response.read()
            conn.close()

            self.response_from_server = json.loads(answer)
            _LOGGER.info("Query executed successfully: %s" % (sql))
            return True
        except (HTTPException, socket.error) as e:
            _LOGGER.error("Error connecting to presto server at: " +
                          self.server + ":" + str(self.port) + ' ' + e.message)
            return False
        except ValueError as e:
            _LOGGER.error('Error connecting to Presto server: ' + e.message +
                          ' error from server: ' + answer)
            raise e

    def _get_response_from(self, uri):
        """
        Sends a GET request to the Presto server at the specified next_uri
        and updates the response

        Remove the scheme and host/port from the uri; the connection itself
        has that information.
        """
        parts = list(urlparse.urlsplit(uri))
        parts[0] = None
        parts[1] = None
        location = urlparse.urlunsplit(parts)
        conn = self._get_connection()
        headers = {"X-Presto-User": self.user}
        self._add_auth_headers(headers)
        conn.request("GET", location, headers=headers)
        response = conn.getresponse()

        if response.status != 200:
            conn.close()
            _LOGGER.error("Error making GET request to %s: %s %s" %
                          (uri, response.status, response.reason))
            return False

        answer = response.read()
        conn.close()

        self.response_from_server = json.loads(answer)
        _LOGGER.info("GET request successful for uri: " + uri)
        return True

    def _build_results_from_response(self):
        """
        Build result from the response

        The reponse_from_server may contain up to 3 uri's.
        1. link to fetch the next packet of data ('nextUri')
        2. TODO: information about the query execution ('infoUri')
        3. TODO: cancel the query ('partialCancelUri').
        """
        if NEXT_URI_RESP in self.response_from_server:
            self.next_uri = self.response_from_server[NEXT_URI_RESP]
        else:
            self.next_uri = ""

        if DATA_RESP in self.response_from_server:
            if self.rows:
                self.rows.extend(self.response_from_server[DATA_RESP])
            else:
                self.rows = self.response_from_server[DATA_RESP]

    def _get_rows(self, num_of_rows=NUM_ROWS):
        """
        Get the rows returned from the query.

        The client sends GET requests to the server using the 'nextUri'
        from the previous response until the servers response does not
        contain anymore 'nextUri's.  When there is no 'nextUri' the query is
        finished

        Note that this can only be called once and does not page through
        the results.

        Parameters:
            num_of_rows: to be retrieved. 1000 by default
        """
        if num_of_rows == 0:
            return []

        self._build_results_from_response()

        if not self._get_next_uri():
            return []

        while self._get_next_uri():
            if not self._get_response_from(self._get_next_uri()):
                return []
            if (len(self.rows) <= num_of_rows):
                self._build_results_from_response()
        return self.rows

    def _get_next_uri(self):
        return self.next_uri

    def _get_connection(self):
        if self.coordinator_config.use_https():
            return self._get_https_connection()
        else:
            return HTTPConnection(self.server, self.port, False, URL_TIMEOUT_MS)

    @staticmethod
    def _get_configured_port(coordinator_config):
        if coordinator_config.use_https():
            return coordinator_config.get_https_port()
        else:
            return coordinator_config.get_http_port()

    def _get_https_connection(self):
        ca_file_path = self._get_pem()
        result = HTTPSCaCertConnection(
                self.server, self.port, None, None, ca_file_path, False, URL_TIMEOUT_MS)
        return result

    def _fetch_keystore_data(self):
        if not self.keystore_data:
            remote_keystore_path = self.coordinator_config.get_client_keystore_path()
            keystore_data = StringIO()
            get(remote_keystore_path, keystore_data, use_sudo=True)
            keystore_data.seek(0)
            self.keystore_data = keystore_data.getvalue()
        return self.keystore_data

    def _pem_string(self, der_bytes, type):
        result = "-----BEGIN %s-----\n" % type
        result += "\r\n".join(
            textwrap.wrap(base64.b64encode(der_bytes).decode('ascii'), 64))
        result += "\n-----END %s-----\n" % type
        return result

    def _write_pem_file(self, directory, der_bytes_list, type):
        prefix = os.path.join(directory,
                              '%s-' % type.lower().replace(' ', '-'))
        fd, pem_path = mkstemp('.pem', prefix)
        # https://www.digicert.com/ssl-support/pem-ssl-creation.htm
        with open(pem_path, 'w') as pem_file:
            for der_bytes in der_bytes_list:
                pem_file.write(self._pem_string(der_bytes, type))
        os.close(fd)
        return pem_path

    def _get_pem(self):
        keystore_data = self._fetch_keystore_data()

        keystore = jks.KeyStore.loads(
                keystore_data,
                self.coordinator_config.get_client_keystore_password())

        if len(keystore.private_keys.items()) == 1:
            _, private_key = keystore.private_keys.items()[0]
        else:
            private_key = self._get_private_key(keystore)
        if not self.ca_file_path:
            """
            Each member of the cert chain is a tuple (cert_type, cert_data)
            We only need to write the data out to the .PEM file.

            This usage is shown in the example in the README.md on github:
            https://github.com/kurtbrose/pyjks
            """
            self.ca_file_path = self._write_pem_file(
                    get_coordinator_directory(),
                    [cert[1] for cert in private_key.cert_chain], 'CERTIFICATE')

        return self.ca_file_path

    def _get_private_key(self, keystore):
        all_keys = ", ".join(keystore.private_keys.keys())
        try:
            alias = env.conf[CERTIFICATE_ALIAS]
        except KeyError:
            error('Multiple keys found in %s. Set %s in %s. Available aliases are %s' %
                  (self.coordinator_config.get_client_keystore_path(),
                   CERTIFICATE_ALIAS, get_topology_path(), all_keys))

        try:
            return keystore.private_keys[alias]
        except KeyError:
            error('No alias %s found in %s. Available aliases are %s' %
                  (alias, self.coordinator_config.get_client_keystore_path(),
                   all_keys))

    def _add_auth_headers(self, headers):
        if self.coordinator_config.use_ldap():
            if self.coordinator_config.use_ldap():
                auth_headers = self._create_auth_headers(
                    self.coordinator_config.get_ldap_user(),
                    self.coordinator_config.get_ldap_password())
                headers.update(auth_headers)
                _LOGGER.info("Using LDAP = %s" % self.coordinator_config.use_ldap())

    @staticmethod
    def _create_auth_headers(user, password):
        if not user:
            error('LDAP user (taken from %s in %s on the coordinator) cannot be null or empty' %
                  (LDAP_CLIENT_USER_KEY, os.path.join(REMOTE_CONF_DIR, CONFIG_PROPERTIES)))
            return {}
        if not password:
            error('LDAP password (taken from %s in %s on the coordinator) cannot be null or empty' %
                  (LDAP_CLIENT_PASSWORD_KEY, os.path.join(REMOTE_CONF_DIR, CONFIG_PROPERTIES)))
            return {}

        if ':' in user:
            error("LDAP user cannot contain ':': %s" % user)

        # base64 encode the username and password
        auth = base64.encodestring('%s:%s' % (user, password)).replace('\n', '')
        return {'Authorization': 'Basic %s' % auth}
