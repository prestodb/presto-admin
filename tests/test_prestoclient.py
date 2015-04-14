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
import socket
from mock import patch
from prestoadmin.prestoclient import execute_query, URL_TIMEOUT_MS
from prestoadmin.util.exception import InvalidArgumentError
from tests import utils


class TestPrestoClient(utils.BaseTestCase):
    def test_no_sql(self):
        self.assertRaisesRegexp(InvalidArgumentError,
                                "SQL query missing",
                                execute_query, "", "any_host", "any_user")

    def test_no_server(self):
        self.assertRaisesRegexp(InvalidArgumentError,
                                "Server IP missing",
                                execute_query, "any_sql", "", "any_user")

    def test_no_user(self):
        self.assertRaisesRegexp(InvalidArgumentError,
                                "Username missing",
                                execute_query, "any_sql", "any_host", "")

    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_default_request_called(self, mock_conn):
        headers = {"X-Presto-Catalog": "hive", "X-Presto-Schema": "default",
                   "X-Presto-User": "any_user"}

        execute_query("any_sql", "any_host", "any_user")
        mock_conn.assert_called_with("any_host", 8080, False, URL_TIMEOUT_MS)
        mock_conn().request.assert_called_with("POST", "/v1/statement",
                                               "any_sql", headers)
        self.assertTrue(mock_conn().getresponse.called)

    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_connection_failed(self, mock_conn):
        execute_query("any_sql", "any_host", "any_user")

        self.assertTrue(mock_conn().close.called)
        self.assertFalse(execute_query("any_sql", "any_host", "any_user"))

    @patch('prestoadmin.prestoclient.HTTPConnection')
    def test_http_call_failed(self, mock_conn):
        mock_conn.side_effect = HTTPException("Error")
        self.assertFalse(execute_query("any_sql", "any_host", "any_user"))

        mock_conn.side_effect = socket.error("Error")
        self.assertFalse(execute_query("any_sql", "any_host", "any_user"))
