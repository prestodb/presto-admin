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
tests for catalog module
"""
import os

import fabric.api
from fabric.operations import _AttributeString
from mock import patch

from prestoadmin import catalog
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigurationError, \
    ConfigFileNotFoundError
from prestoadmin.standalone.config import PRESTO_STANDALONE_USER_GROUP
from prestoadmin.util.local_config_util import get_catalog_directory
from tests.unit.base_unit_case import BaseUnitCase


class TestCatalog(BaseUnitCase):
    def setUp(self):
        super(TestCatalog, self).setUp(capture_output=True)

    @patch('prestoadmin.catalog.os.path.isfile')
    def test_add_not_exist(self, isfile_mock):
        isfile_mock.return_value = False
        self.assertRaisesRegexp(ConfigurationError,
                                'Configuration for catalog dummy not found',
                                catalog.add, 'dummy')

    @patch('prestoadmin.catalog.validate')
    @patch('prestoadmin.catalog.deploy_files')
    @patch('prestoadmin.catalog.os.path.isfile')
    def test_add_exists(self, isfile_mock, deploy_mock, validate_mock):
        isfile_mock.return_value = True
        catalog.add('tpch')
        filenames = ['tpch.properties']
        deploy_mock.assert_called_with(filenames,
                                       get_catalog_directory(),
                                       constants.REMOTE_CATALOG_DIR,
                                       PRESTO_STANDALONE_USER_GROUP)
        validate_mock.assert_called_with(filenames)

    @patch('prestoadmin.catalog.deploy_files')
    @patch('prestoadmin.catalog.os.path.isdir')
    @patch('prestoadmin.catalog.os.listdir')
    @patch('prestoadmin.catalog.validate')
    def test_add_all(self, mock_validate, listdir_mock, isdir_mock,
                     deploy_mock):
        catalogs = ['tpch.properties', 'another.properties']
        listdir_mock.return_value = catalogs
        catalog.add()
        deploy_mock.assert_called_with(catalogs,
                                       get_catalog_directory(),
                                       constants.REMOTE_CATALOG_DIR,
                                       PRESTO_STANDALONE_USER_GROUP)

    @patch('prestoadmin.catalog.deploy_files')
    @patch('prestoadmin.catalog.os.path.isdir')
    def test_add_all_fails_if_dir_not_there(self, isdir_mock, deploy_mock):
        isdir_mock.return_value = False
        self.assertRaisesRegexp(ConfigFileNotFoundError,
                                r'Cannot add catalogs because directory .+'
                                r' does not exist',
                                catalog.add)
        self.assertFalse(deploy_mock.called)

    @patch('prestoadmin.catalog.sudo')
    @patch('prestoadmin.catalog.os.path.exists')
    @patch('prestoadmin.catalog.os.remove')
    def test_remove(self, local_rm_mock, exists_mock, sudo_mock):
        script = ('if [ -f /etc/presto/catalog/tpch.properties ] ; '
                  'then rm /etc/presto/catalog/tpch.properties ; '
                  'else echo "Could not remove catalog \'tpch\'. '
                  'No such file \'/etc/presto/catalog/tpch.properties\'"; fi')
        exists_mock.return_value = True
        fabric.api.env.host = 'localhost'
        catalog.remove('tpch')
        sudo_mock.assert_called_with(script)
        local_rm_mock.assert_called_with(get_catalog_directory() +
                                         '/tpch.properties')

    @patch('prestoadmin.catalog.sudo')
    @patch('prestoadmin.catalog.os.path.exists')
    def test_remove_failure(self, exists_mock, sudo_mock):
        exists_mock.return_value = False
        fabric.api.env.host = 'localhost'
        out = _AttributeString()
        out.succeeded = False
        sudo_mock.return_value = out
        self.assertRaisesRegexp(SystemExit,
                                '\\[localhost\\] Failed to remove catalog tpch.',
                                catalog.remove,
                                'tpch')

    @patch('prestoadmin.catalog.sudo')
    @patch('prestoadmin.catalog.os.path.exists')
    def test_remove_no_such_file(self, exists_mock, sudo_mock):
        exists_mock.return_value = False
        fabric.api.env.host = 'localhost'
        error_msg = ('Could not remove catalog tpch: No such file ' +
                     os.path.join(get_catalog_directory(), 'tpch.properties'))
        out = _AttributeString(error_msg)
        out.succeeded = True
        sudo_mock.return_value = out
        self.assertRaisesRegexp(SystemExit,
                                '\\[localhost\\] %s' % error_msg,
                                catalog.remove,
                                'tpch')

    @patch('prestoadmin.catalog.os.listdir')
    @patch('prestoadmin.catalog.os.path.isdir')
    def test_warning_if_connector_dir_empty(self, isdir_mock, listdir_mock):
        isdir_mock.return_value = True
        listdir_mock.return_value = []
        catalog.add()
        self.assertEqual('\nWarning: Directory %s is empty. No catalogs will'
                         ' be deployed\n\n' % get_catalog_directory(),
                         self.test_stderr.getvalue())

    @patch('prestoadmin.catalog.os.listdir')
    @patch('prestoadmin.catalog.os.path.isdir')
    def test_add_permission_denied(self, isdir_mock, listdir_mock):
        isdir_mock.return_value = True
        error_msg = ('Permission denied')
        listdir_mock.side_effect = OSError(13, error_msg)
        fabric.api.env.host = 'localhost'
        self.assertRaisesRegexp(SystemExit, '\[localhost\] %s' % error_msg,
                                catalog.add)

    @patch('prestoadmin.catalog.os.remove')
    @patch('prestoadmin.catalog.remove_file')
    def test_remove_os_error(self, remove_file_mock, remove_mock):
        fabric.api.env.host = 'localhost'
        error = OSError(13, 'Permission denied')
        remove_mock.side_effect = error
        self.assertRaisesRegexp(OSError, 'Permission denied',
                                catalog.remove, 'tpch')

    @patch('prestoadmin.catalog.secure_create_directory')
    @patch('prestoadmin.util.fabricapi.put')
    def test_deploy_files(self, put_mock, create_dir_mock):
        local_dir = '/my/local/dir'
        remote_dir = '/my/remote/dir'
        catalog.deploy_files(['a', 'b'], local_dir, remote_dir,
                             PRESTO_STANDALONE_USER_GROUP)
        create_dir_mock.assert_called_with(remote_dir, PRESTO_STANDALONE_USER_GROUP)
        put_mock.assert_any_call('/my/local/dir/a', remote_dir, use_sudo=True,
                                 mode=0600)
        put_mock.assert_any_call('/my/local/dir/b', remote_dir, use_sudo=True,
                                 mode=0600)

    @patch('prestoadmin.catalog.os.path.isfile')
    @patch("__builtin__.open")
    def test_validate(self, open_mock, is_file_mock):
        is_file_mock.return_value = True
        file_obj = open_mock.return_value.__enter__.return_value
        file_obj.read.return_value = 'connector.noname=example'

        self.assertRaisesRegexp(ConfigurationError,
                                'Catalog configuration example.properties '
                                'does not contain connector.name',
                                catalog.add, 'example')

    @patch('prestoadmin.catalog.os.path.isfile')
    def test_validate_fail(self, is_file_mock):
        is_file_mock.return_value = True

        self.assertRaisesRegexp(
            SystemExit,
            'Error validating ' + os.path.join(get_catalog_directory(), 'example.properties') + '\n\n'
            'Underlying exception:\n    No such file or directory',
            catalog.add, 'example')

    @patch('prestoadmin.catalog.get')
    @patch('prestoadmin.catalog.files.exists')
    @patch('prestoadmin.catalog.ensure_directory_exists')
    @patch('prestoadmin.catalog.os.path.exists')
    def test_gather_connectors(self, path_exists, ensure_dir_exists,
                               files_exists, get_mock):
        fabric.api.env.host = 'any_host'
        path_exists.return_value = False
        files_exists.return_value = True
        catalog.gather_catalogs('local_config_dir')
        get_mock.assert_called_once_with(
            constants.REMOTE_CATALOG_DIR, 'local_config_dir/any_host/catalog', use_sudo=True)

        # if remote catalog dir does not exist
        get_mock.reset_mock()
        files_exists.return_value = False
        results = catalog.gather_catalogs('local_config_dir')
        self.assertEqual([], results)
        self.assertFalse(get_mock.called)
