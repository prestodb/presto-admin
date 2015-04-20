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
from httplib import HTTPException
import json
import os
import socket

from mock import patch

from prestoadmin.prestoclient import URL_TIMEOUT_MS, PrestoClient
from prestoadmin.util.exception import InvalidArgumentError
from tests import utils


class TestPrestoClient(utils.BaseTestCase):
    def test_no_sql(self):
        client = PrestoClient("any_host", "any_user")
        self.assertRaisesRegexp(InvalidArgumentError,
                                "SQL query missing",
                                client.execute_query, "", )

    def test_no_server(self):
        client = PrestoClient("", "any_user")
        self.assertRaisesRegexp(InvalidArgumentError,
                                "Server IP missing",
                                client.execute_query, "any_sql")

    def test_no_user(self):
        client = PrestoClient("any_host", "")
        self.assertRaisesRegexp(InvalidArgumentError,
                                "Username missing",
                                client.execute_query, "any_sql")

    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_default_request_called(self, mock_conn):
        client = PrestoClient("any_host", "any_user")
        headers = {"X-Presto-Catalog": "hive", "X-Presto-Schema": "default",
                   "X-Presto-User": "any_user"}

        client.execute_query("any_sql")
        mock_conn.assert_called_with("any_host", 8080, False, URL_TIMEOUT_MS)
        mock_conn().request.assert_called_with("POST", "/v1/statement",
                                               "any_sql", headers)
        self.assertTrue(mock_conn().getresponse.called)

    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_connection_failed(self, mock_conn):
        client = PrestoClient("any_host", "any_user")
        client.execute_query("any_sql")

        self.assertTrue(mock_conn().close.called)
        self.assertFalse(client.execute_query("any_sql"))

    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_http_call_failed(self, mock_conn):
        client = PrestoClient("any_host", "any_user")
        mock_conn.side_effect = HTTPException("Error")
        self.assertFalse(client.execute_query("any_sql"))

        mock_conn.side_effect = socket.error("Error")
        self.assertFalse(client.execute_query("any_sql"))

    @patch.object(PrestoClient, 'get_response_from')
    @patch.object(PrestoClient, 'get_next_uri')
    def test_retrieve_rows(self, mock_uri, mock_get_from_uri):
        client = PrestoClient("any_host", "any_user")
        dir = os.path.abspath(os.path.dirname(__file__))

        with open(dir + '/files/valid_rest_response_level1.txt') as json_file:
            client.response_from_server = json.load(json_file)
        mock_get_from_uri.return_value = True
        mock_uri.side_effect = [
            "http://localhost:8080/v1/statement/2015_harih/2", ""
        ]

        self.assertEqual(client.get_rows(), [])
        self.assertEqual(client.next_uri,
                         "http://localhost:8080/v1/statement/2015_harih/2")

        with open(dir + '/files/valid_rest_response_level2.txt') as json_file:
            client.response_from_server = json.load(json_file)
        mock_uri.side_effect = [
            "http://localhost:8080/v1/statement/2015_harih/2", ""
        ]

        expected_row = [["7298eeff-e6b", "http://localhost:8080",
                         "presto-main:0.97-SNAPSHOT", True]]
        self.assertEqual(client.get_rows(), expected_row)
        self.assertEqual(client.next_uri, "")

    @patch.object(PrestoClient, 'get_response_from')
    @patch.object(PrestoClient, 'get_next_uri')
    def test_limit_rows(self, mock_uri, mock_get_from_uri):
        client = PrestoClient("any_host", "any_user")
        dir = os.path.abspath(os.path.dirname(__file__))
        with open(dir + '/files/valid_rest_response_level2.txt') as json_file:
            client.response_from_server = json.load(json_file)
        mock_get_from_uri.return_value = True
        mock_uri.side_effect = [
            "http://localhost:8080/v1/statement/2015_harih/2", ""
        ]

        self.assertEqual(client.get_rows(0), [])
