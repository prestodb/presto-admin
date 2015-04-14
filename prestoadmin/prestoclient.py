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
from httplib import HTTPConnection, HTTPException
import logging
import socket
import json
from prestoadmin.util.exception import InvalidArgumentError

_LOGGER = logging.getLogger(__name__)
URL_TIMEOUT_MS = 5000


def execute_query(sql, server, user, port=8080, schema="default",
                  catalog="hive"):
    """
    Execute a query connecting to Presto server using passed parameters

    :param server: IP Address where the server runs
    :param port: Port running the presto server (default=8080)
    :param sql: sql query to be executed
    :param schema: Presto schema to be used while executing query
    (default=default)
    :param catalog: Catalog to be used by the server
    :return: True or False exit status
    """
    if sql == "":
        raise InvalidArgumentError("SQL query missing")

    if server == "":
        raise InvalidArgumentError("Server IP missing")

    if user == "":
        raise InvalidArgumentError("Username missing")

    headers = {"X-Presto-Catalog": catalog,
               "X-Presto-Schema": schema,
               "X-Presto-User": user}

    try:
        _LOGGER.info("Connecting to server at: " + server +
                     ":" + str(port) + "/" + user)
        conn = HTTPConnection(server, port, False, URL_TIMEOUT_MS)
        conn.request("POST", "/v1/statement", sql, headers)
        response = conn.getresponse()

        if response.status != 200:
            conn.close()
            _LOGGER.error("Connection error: "
                          + str(response.status) + " " + response.reason)
            return False

        answer = response.read()
        conn.close()

        response_from_server = json.loads(answer)
        get_results_from(response_from_server)
        return True
    except (HTTPException, socket.error):
        _LOGGER.error("Error connecting to presto server at: " +
                      server + ":" + str(port))
        return False


def get_results_from(response_from_server):
    # TODO: Implement various getters/api after analyzing the response
    pass
