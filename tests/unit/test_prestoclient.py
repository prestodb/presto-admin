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

from httplib import HTTPException, HTTPConnection
import json
import os
import socket

from fabric.operations import _AttributeString
from mock import patch, PropertyMock

from prestoadmin.prestoclient import URL_TIMEOUT_MS, PrestoClient
from prestoadmin.util.exception import InvalidArgumentError
from tests.base_test_case import BaseTestCase


class TestPrestoClient(BaseTestCase):
    def test_no_sql(self):
        client = PrestoClient('any_host', 'any_user')
        self.assertRaisesRegexp(InvalidArgumentError,
                                "SQL query missing",
                                client.execute_query, "", )

    def test_no_server(self):
        client = PrestoClient("", 'any_user')
        self.assertRaisesRegexp(InvalidArgumentError,
                                "Server IP missing",
                                client.execute_query, "any_sql")

    def test_no_user(self):
        client = PrestoClient('any_host', "")
        self.assertRaisesRegexp(InvalidArgumentError,
                                "Username missing",
                                client.execute_query, "any_sql")

    @patch('prestoadmin.prestoclient.lookup_port')
    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_default_request_called(self, mock_conn, mock_port):
        mock_port.return_value = 8080
        client = PrestoClient('any_host', 'any_user')
        headers = {"X-Presto-Catalog": "hive", "X-Presto-Schema": "default",
                   "X-Presto-User": 'any_user'}

        client.execute_query("any_sql")
        mock_conn.assert_called_with('any_host', 8080, False, URL_TIMEOUT_MS)
        mock_conn().request.assert_called_with("POST", "/v1/statement",
                                               "any_sql", headers)
        self.assertTrue(mock_conn().getresponse.called)

    @patch('prestoadmin.prestoclient.lookup_port')
    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_connection_failed(self, mock_conn, mock_port):
        mock_port.return_value = 8080
        client = PrestoClient('any_host', 'any_user')
        client.execute_query("any_sql")

        self.assertTrue(mock_conn().close.called)
        self.assertFalse(client.execute_query("any_sql"))

    @patch('prestoadmin.prestoclient.lookup_port')
    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_http_call_failed(self, mock_conn, mock_port):
        mock_port.return_value = 8080
        client = PrestoClient('any_host', 'any_user')
        mock_conn.side_effect = HTTPException("Error")
        self.assertFalse(client.execute_query("any_sql"))

        mock_conn.side_effect = socket.error("Error")
        self.assertFalse(client.execute_query("any_sql"))

    @patch('prestoadmin.prestoclient.lookup_port')
    @patch.object(HTTPConnection, 'request')
    @patch.object(HTTPConnection, 'getresponse')
    def test_http_answer_valid(self, mock_response, mock_request, mock_port):
        mock_port.return_value = 8080
        client = PrestoClient('any_host', 'any_user')
        mock_response.return_value.read.return_value = '{}'
        type(mock_response.return_value).status = \
            PropertyMock(return_value=200)
        self.assertTrue(client.execute_query('any_sql'))

    @patch('prestoadmin.prestoclient.lookup_port')
    @patch.object(HTTPConnection, 'request')
    @patch.object(HTTPConnection, 'getresponse')
    def test_http_answer_not_json(self, mock_response,
                                  mock_request, mock_port):
        mock_port.return_value = 8080
        client = PrestoClient('any_host', 'any_user')
        mock_response.return_value.read.return_value = 'NOT JSON!'
        type(mock_response.return_value).status =\
            PropertyMock(return_value=200)
        self.assertRaisesRegexp(ValueError, 'No JSON object could be decoded',
                                client.execute_query, 'any_sql')

    @patch('prestoadmin.prestoclient.lookup_port')
    @patch.object(PrestoClient, 'get_response_from')
    @patch.object(PrestoClient, 'get_next_uri')
    def test_retrieve_rows(self, mock_uri, mock_get_from_uri, mock_port):
        mock_port.return_value = 8080
        client = PrestoClient('any_host', 'any_user')
        dir = os.path.abspath(os.path.dirname(__file__))

        with open(dir + '/resources/valid_rest_response_level1.txt') \
                as json_file:
            client.response_from_server = json.load(json_file)
        mock_get_from_uri.return_value = True
        mock_uri.side_effect = [
            "http://localhost:8080/v1/statement/2015_harih/2", ""
        ]

        self.assertEqual(client.get_rows(), [])
        self.assertEqual(client.next_uri,
                         "http://localhost:8080/v1/statement/2015_harih/2")

        with open(dir + '/resources/valid_rest_response_level2.txt') \
                as json_file:
            client.response_from_server = json.load(json_file)
        mock_uri.side_effect = [
            "http://localhost:8080/v1/statement/2015_harih/2", ""
        ]

        expected_row = [["uuid1", "http://localhost:8080", "presto-main:0.97",
                         True],
                        ["uuid2", "http://worker:8080", "presto-main:0.97",
                         False]]
        self.assertEqual(client.get_rows(), expected_row)
        self.assertEqual(client.next_uri, "")

    @patch('prestoadmin.prestoclient.lookup_port')
    @patch.object(PrestoClient, 'get_response_from')
    @patch.object(PrestoClient, 'get_next_uri')
    def test_append_rows(self, mock_uri, mock_get_from_uri, mock_port):
        mock_port.return_value = 8080
        client = PrestoClient('any_host', 'any_user')
        dir = os.path.abspath(os.path.dirname(__file__))

        with open(dir + '/resources/valid_rest_response_level2.txt') \
                as json_file:
            client.response_from_server = json.load(json_file)
        mock_get_from_uri.return_value = True
        mock_uri.side_effect = ["any_next_uri", "any_next_next_uri", "", ""]
        expected_row = [["uuid1", "http://localhost:8080", "presto-main:0.97",
                         True],
                        ["uuid2", "http://worker:8080", "presto-main:0.97",
                         False],
                        ["uuid1", "http://localhost:8080", "presto-main:0.97",
                         True],
                        ["uuid2", "http://worker:8080",  "presto-main:0.97",
                         False]]
        self.assertEqual(client.get_rows(), expected_row)

    @patch.object(PrestoClient, 'get_response_from')
    @patch.object(PrestoClient, 'get_next_uri')
    def test_limit_rows(self, mock_uri, mock_get_from_uri):
        client = PrestoClient('any_host', 'any_user')
        dir = os.path.abspath(os.path.dirname(__file__))
        with open(dir + '/resources/valid_rest_response_level2.txt') \
                as json_file:
            client.response_from_server = json.load(json_file)
        mock_get_from_uri.return_value = True
        mock_uri.side_effect = ["any_next_uri", ""]

        self.assertEqual(client.get_rows(0), [])

    @patch('prestoadmin.prestoclient.urlopen')
    @patch('httplib.HTTPResponse')
    def test_get_response(self, mock_resp, mock_urlopen):
        client = PrestoClient('any_host', 'any_user')
        mock_urlopen.return_value = mock_resp
        mock_resp.read.return_value = '{"message": "ok!"}'

        client.get_response_from('any_uri')
        self.assertEqual(client.response_from_server, {"message": "ok!"})

    @patch('prestoadmin.prestoclient.HTTPConnection')
    @patch('prestoadmin.util.remote_config_util.sudo')
    def test_execute_query_get_port(self, sudo_mock, conn_mock):
        client = PrestoClient('any_host', 'any_user')
        client.rows = ['hello']
        client.next_uri = 'hello'
        client.response_from_server = {'hello': 'hello'}
        sudo_mock.return_value = _AttributeString('http-server.http.port=8080')
        sudo_mock.return_value.failed = False
        sudo_mock.return_value.return_code = 0
        client.execute_query('select * from nation')
        self.assertEqual(client.port, 8080)
        self.assertEqual(client.rows, [])
        self.assertEqual(client.next_uri, '')
        self.assertEqual(client.response_from_server, {})
