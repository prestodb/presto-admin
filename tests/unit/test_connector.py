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
tests for connector module
"""
import fabric.api
from fabric.operations import _AttributeString
from mock import patch

from prestoadmin import connector
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigurationError, \
    ConfigFileNotFoundError
from tests.unit.base_unit_case import BaseUnitCase


class TestConnector(BaseUnitCase):
    def setUp(self):
        super(TestConnector, self).setUp(capture_output=True)

    @patch('prestoadmin.connector.os.path.isfile')
    def test_add_not_exist(self, isfile_mock):
        isfile_mock.return_value = False
        self.assertRaisesRegexp(ConfigurationError,
                                'Configuration for connector dummy not found',
                                connector.add, 'dummy')

    @patch('prestoadmin.connector.validate')
    @patch('prestoadmin.connector.deploy_files')
    @patch('prestoadmin.connector.os.path.isfile')
    def test_add_exists(self, isfile_mock, deploy_mock, validate_mock):
        isfile_mock.return_value = True
        connector.add('tpch')
        filenames = ['tpch.properties']
        deploy_mock.assert_called_with(filenames,
                                       constants.CONNECTORS_DIR,
                                       constants.REMOTE_CATALOG_DIR)
        validate_mock.assert_called_with(filenames)

    @patch('prestoadmin.connector.deploy_files')
    @patch('prestoadmin.connector.os.path.isdir')
    @patch('prestoadmin.connector.os.listdir')
    @patch('prestoadmin.connector.validate')
    def test_add_all(self, mock_validate, listdir_mock, isdir_mock,
                     deploy_mock):
        catalogs = ['tpch.properties', 'another.properties']
        listdir_mock.return_value = catalogs
        connector.add()
        deploy_mock.assert_called_with(catalogs,
                                       constants.CONNECTORS_DIR,
                                       constants.REMOTE_CATALOG_DIR)

    @patch('prestoadmin.connector.deploy_files')
    @patch('prestoadmin.connector.os.path.isdir')
    def test_add_all_fails_if_dir_not_there(self, isdir_mock, deploy_mock):
        isdir_mock.return_value = False
        self.assertRaisesRegexp(ConfigFileNotFoundError,
                                r'Cannot add connectors because directory .+'
                                r' does not exist',
                                connector.add)
        self.assertFalse(deploy_mock.called)

    @patch('prestoadmin.connector.sudo')
    @patch('prestoadmin.connector.os.path.exists')
    @patch('prestoadmin.connector.os.remove')
    def test_remove(self, local_rm_mock, exists_mock, sudo_mock):
        script = ('if [ -f /etc/presto/catalog/tpch.properties ] ; '
                  'then rm /etc/presto/catalog/tpch.properties ; '
                  'else echo "Could not remove connector \'tpch\'. '
                  'No such file \'/etc/presto/catalog/tpch.properties\'"; fi')
        exists_mock.return_value = True
        fabric.api.env.host = 'localhost'
        connector.remove('tpch')
        sudo_mock.assert_called_with(script)
        local_rm_mock.assert_called_with(constants.CONNECTORS_DIR +
                                         '/tpch.properties')

    @patch('prestoadmin.connector.sudo')
    @patch('prestoadmin.connector.os.path.exists')
    def test_remove_failure(self, exists_mock, sudo_mock):
        exists_mock.return_value = False
        fabric.api.env.host = 'localhost'
        out = _AttributeString()
        out.succeeded = False
        sudo_mock.return_value = out
        self.assertRaisesRegexp(SystemExit,
                                '\\[localhost\\] Failed to remove '
                                'connector tpch.',
                                connector.remove,
                                'tpch')

    @patch('prestoadmin.connector.sudo')
    @patch('prestoadmin.connector.os.path.exists')
    def test_remove_no_such_file(self, exists_mock, sudo_mock):
        exists_mock.return_value = False
        fabric.api.env.host = 'localhost'
        error_msg = ('Could not remove connector tpch: No such file '
                     '/etc/opt/prestoadmin/connectors/tpch.properties')
        out = _AttributeString(error_msg)
        out.succeeded = True
        sudo_mock.return_value = out
        self.assertRaisesRegexp(SystemExit,
                                '\\[localhost\\] %s' % error_msg,
                                connector.remove,
                                'tpch')

    @patch('prestoadmin.connector.os.listdir')
    @patch('prestoadmin.connector.os.path.isdir')
    def test_warning_if_connector_dir_empty(self, isdir_mock, listdir_mock):
        isdir_mock.return_value = True
        listdir_mock.return_value = []
        connector.add()
        self.assertEqual('\nWarning: Directory %s is empty. No connectors will'
                         ' be deployed\n\n' % constants.CONNECTORS_DIR,
                         self.test_stderr.getvalue())

    @patch('prestoadmin.connector.os.listdir')
    @patch('prestoadmin.connector.os.path.isdir')
    def test_add_permission_denied(self, isdir_mock, listdir_mock):
        isdir_mock.return_value = True
        error_msg = ('Permission denied')
        listdir_mock.side_effect = OSError(13, error_msg)
        fabric.api.env.host = 'localhost'
        self.assertRaisesRegexp(SystemExit, '\[localhost\] %s' % error_msg,
                                connector.add)

    @patch('prestoadmin.connector.os.remove')
    @patch('prestoadmin.connector.remove_file')
    def test_remove_os_error(self, remove_file_mock, remove_mock):
        fabric.api.env.host = 'localhost'
        error = OSError(13, 'Permission denied')
        remove_mock.side_effect = error
        self.assertRaisesRegexp(OSError, 'Permission denied',
                                connector.remove, 'tpch')

    @patch('prestoadmin.connector.sudo')
    @patch('prestoadmin.connector.put')
    def test_deploy_files(self, put_mock, sudo_mock):
        local_dir = '/my/local/dir'
        remote_dir = '/my/remote/dir'
        connector.deploy_files(['a', 'b'], local_dir, remote_dir)
        sudo_mock.assert_called_with('mkdir -p %s' % remote_dir)
        put_mock.assert_any_call('/my/local/dir/a', remote_dir, use_sudo=True)
        put_mock.assert_any_call('/my/local/dir/b', remote_dir, use_sudo=True)

    @patch('prestoadmin.connector.os.path.isfile')
    @patch("__builtin__.open")
    def test_validate(self, open_mock, is_file_mock):
        is_file_mock.return_value = True
        file_obj = open_mock.return_value.__enter__.return_value
        file_obj.read.return_value = 'connector.noname=example'

        self.assertRaisesRegexp(ConfigurationError,
                                'Catalog configuration example.properties '
                                'does not contain connector.name',
                                connector.add, 'example')

    @patch('prestoadmin.connector.os.path.isfile')
    def test_validate_fail(self, is_file_mock):
        is_file_mock.return_value = True

        self.assertRaisesRegexp(
            SystemExit,
            'Error validating '
            '/etc/opt/prestoadmin/connectors/example.properties\n\n'
            'Underlying exception:\n    No such file or directory',
            connector.add, 'example')

    @patch('prestoadmin.connector.get')
    @patch('prestoadmin.connector.files.exists')
    @patch('prestoadmin.connector.ensure_directory_exists')
    @patch('prestoadmin.connector.os.path.exists')
    def test_gather_connectors(self, path_exists, ensure_dir_exists,
                               files_exists, get_mock):
        fabric.api.env.host = 'any_host'
        path_exists.return_value = False
        files_exists.return_value = True
        connector.gather_connectors('local_config_dir')
        get_mock.assert_called_once_with(
            constants.REMOTE_CATALOG_DIR, 'local_config_dir/any_host/catalog')

        # if remote catalog dir does not exist
        get_mock.reset_mock()
        files_exists.return_value = False
        results = connector.gather_connectors('local_config_dir')
        self.assertEqual([], results)
        self.assertFalse(get_mock.called)
