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
from httplib import HTTPConnection, HTTPException
import logging
import json
import socket

from urllib2 import HTTPError, urlopen, URLError

from prestoadmin.util.remote_config_util import lookup_port

from prestoadmin.util.exception import InvalidArgumentError


_LOGGER = logging.getLogger(__name__)
URL_TIMEOUT_MS = 5000
NUM_ROWS = 1000
DATA_RESP = "data"
NEXT_URI_RESP = "nextUri"


class PrestoClient:
    response_from_server = {}
    # rows returned by the query
    rows = []
    next_uri = ""

    def __init__(self, server, user, port=None):
        self.server = server
        self.user = user
        self.port = port

    def clear_old_results(self):
        if self.rows:
            self.rows = []

        if self.next_uri:
            self.next_uri = ''

        if self.response_from_server:
            self.response_from_server = {}

    def execute_query(self, sql, schema="default", catalog="hive"):
        """
        Execute a query connecting to Presto server using passed parameters.

        Client sends http POST request to the Presto server, page:
        "/v1/statement". Header information should
        include: X-Presto-Catalog, X-Presto-Schema,  X-Presto-User

        Args:
            sql: SQL query to be executed
            schema: Presto schema to be used while executing query
                (default=default)
            catalog: Catalog to be used by the server

        Returns:
            True or False exit status
        """
        if not sql:
            raise InvalidArgumentError("SQL query missing")

        if not self.server:
            raise InvalidArgumentError("Server IP missing")

        if not self.user:
            raise InvalidArgumentError("Username missing")

        if not self.port:
            self.port = lookup_port(self.server)

        self.clear_old_results()

        headers = {"X-Presto-Catalog": catalog,
                   "X-Presto-Schema": schema,
                   "X-Presto-User": self.user}
        answer = ''
        try:
            _LOGGER.info("Connecting to server at: " + self.server +
                         ":" + str(self.port) + " as user " + self.user +
                         " to execute query " + sql)
            conn = HTTPConnection(self.server, self.port, False,
                                  URL_TIMEOUT_MS)
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

    def get_response_from(self, uri):
        """
        Sends a GET request to the Presto server at the specified next_uri
        and updates the response
        """
        try:
            conn = urlopen(uri, None, URL_TIMEOUT_MS)
            answer = conn.read()
            conn.close()

            self.response_from_server = json.loads(answer)
            _LOGGER.info("GET request successful for uri: " + uri)
            return True
        except (HTTPError, URLError) as e:
            _LOGGER.error("Error opening the presto response uri: " +
                          str(e.reason))
            return False

    def build_results_from_response(self):
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

    def get_rows(self, num_of_rows=NUM_ROWS):
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

        self.build_results_from_response()

        if not self.get_next_uri():
            return []

        while self.get_next_uri():
            if not self.get_response_from(self.get_next_uri()):
                return []
            if (len(self.rows) <= num_of_rows):
                self.build_results_from_response()
        return self.rows

    def get_next_uri(self):
        return self.next_uri
