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
Tests the presto install
"""
import os
import tempfile

from fabric.api import env
from fabric.operations import _AttributeString
from mock import patch, call, MagicMock

from prestoadmin import server
from prestoadmin.prestoclient import PrestoClient
from prestoadmin.server import INIT_SCRIPTS
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigFileNotFoundError, \
    ConfigurationError
from prestoadmin.util.fabricapi import get_host_list
from prestoadmin.util.local_config_util import get_catalog_directory
from tests.unit.base_unit_case import BaseUnitCase, PRESTO_CONFIG


class TestInstall(BaseUnitCase):
    SERVER_FAIL_MSG = 'Could not verify server status for: failed_node1\n' \
                      'This could mean that the server failed to start or that there was no coordinator or worker up.' \
                      ' Please check ' \
                      + constants.DEFAULT_PRESTO_SERVER_LOG_FILE + ' and ' + \
                      constants.DEFAULT_PRESTO_LAUNCHER_LOG_FILE

    def setUp(self):
        self.remove_runs_once_flag(server.status)
        self.remove_runs_once_flag(server.install)
        self.maxDiff = None
        super(TestInstall, self).setUp(capture_output=True)

    @patch('prestoadmin.server.package.check_if_valid_rpm')
    def check_corrupt_rpm_removed_and_returns_none(self, mock_valid_rpm, is_absolute_path):
        mock_valid_rpm.side_effect = SystemExit('...Corrupted RPM...')
        fd = -1
        absolute_path_corrupt_rpm = None
        try:
            fd, absolute_path_corrupt_rpm = tempfile.mkstemp()
            if is_absolute_path:
                local_finder = server.LocalPrestoRpmFinder(absolute_path_corrupt_rpm)
            else:
                relative_path_corrupt_rpm = os.path.basename(absolute_path_corrupt_rpm)
                local_finder = server.LocalPrestoRpmFinder(relative_path_corrupt_rpm)
            self.assertTrue(local_finder.find_local_presto_rpm() is None)
            self.assertTrue(mock_valid_rpm.called)
        finally:
            os.close(fd)
            self.assertRaises(OSError, os.remove, absolute_path_corrupt_rpm)

    def test_check_corrupt_rpm_at_absolute_path_is_removed_and_returns_none(self):
        self.check_corrupt_rpm_removed_and_returns_none(is_absolute_path=True)

    def test_check_corrupt_rpm_at_relative_path_is_removed_and_returns_none(self):
        self.check_corrupt_rpm_removed_and_returns_none(is_absolute_path=False)

    @patch('prestoadmin.server.package.check_if_valid_rpm')
    def check_nonexistent_rpm_returns_none(self, mock_valid_rpm, is_absolute_path):
        mock_valid_rpm.side_effect = SystemExit('...File does not exist...')
        fd = -1
        absolute_path_nonexistent_rpm = None
        try:
            fd, absolute_path_nonexistent_rpm = tempfile.mkstemp()
            if is_absolute_path:
                local_finder = server.LocalPrestoRpmFinder(absolute_path_nonexistent_rpm)
            else:
                relative_path_nonexistent_rpm = os.path.basename(absolute_path_nonexistent_rpm)
                local_finder = server.LocalPrestoRpmFinder(relative_path_nonexistent_rpm)
        finally:
            os.close(fd)
            os.remove(absolute_path_nonexistent_rpm)
        self.assertTrue(local_finder.find_local_presto_rpm() is None)

    def test_check_nonexistent_rpm_at_absolute_path_returns_none(self):
        self.check_nonexistent_rpm_returns_none(is_absolute_path=True)

    def test_check_nonexistent_rpm_at_relative_path_returns_none(self):
        self.check_nonexistent_rpm_returns_none(is_absolute_path=False)

    @patch('prestoadmin.server.package.check_if_valid_rpm')
    def check_find_valid_rpm_returns_absolute_path(self, mock_valid_rpm, is_absolute_path):
        fd = -1
        absolute_path_valid_rpm = None
        try:
            fd, absolute_path_valid_rpm = tempfile.mkstemp()
            if is_absolute_path:
                local_finder = server.LocalPrestoRpmFinder(absolute_path_valid_rpm)
            else:
                relative_path_valid_rpm = os.path.basename(absolute_path_valid_rpm)
                local_finder = server.LocalPrestoRpmFinder(relative_path_valid_rpm)
            self.assertEqual(local_finder.find_local_presto_rpm(), absolute_path_valid_rpm)
            self.assertTrue(mock_valid_rpm.called)
        finally:
            os.close(fd)
            os.remove(absolute_path_valid_rpm)

    def test_check_find_valid_rpm_at_absolute_path_returns_absolute_path(self):
        self.check_find_valid_rpm_returns_absolute_path(is_absolute_path=True)

    def test_check_find_valid_rpm_at_relative_path_returns_absolute_path(self):
        self.check_find_valid_rpm_returns_absolute_path(is_absolute_path=False)

    @patch('prestoadmin.server.urllib2.urlopen')
    def check_content_length(self, mock_urlopen, is_header_present):
        url_response = MagicMock()
        if is_header_present:
            url_response.info.return_value = {'Content-Length': '123'}
        else:
            url_response.info.return_value = {}

        mock_urlopen.return_value = url_response
        url_handler = server.UrlHandler('https://www.google.com')
        if is_header_present:
            self.assertEqual(url_handler.get_content_length(), 123)
        else:
            self.assertTrue(url_handler.get_content_length() is None)

    def test_get_content_length_returns_content_length(self):
        self.check_content_length(is_header_present=True)

    def test_get_content_length_missing_header_returns_none(self):
        self.check_content_length(is_header_present=False)

    @patch('prestoadmin.server.urllib2.urlopen')
    def check_download_file_name(self, mock_urlopen, is_header_present, is_version_present):
        url_response = MagicMock()
        if is_header_present:
            url_response.info.return_value = {'Content-Disposition': 'attachment; filename="test.txt"'}
        else:
            url_response.info.return_value = {}
        mock_urlopen.return_value = url_response
        url_handler = server.UrlHandler('https://www.google.com')
        if is_header_present:
            self.assertEqual(url_handler.get_download_file_name(), 'test.txt')
        else:
            if is_version_present:
                self.assertEqual(url_handler.get_download_file_name('0.148'), 'presto-server-rpm-0.148.rpm')
            else:
                self.assertEqual(url_handler.get_download_file_name(), server.DEFAULT_RPM_NAME)

    def test_get_download_file_name_without_version_returns_header_file_name(self):
        self.check_download_file_name(is_header_present=True, is_version_present=False)

    def test_get_download_file_name_with_version_returns_header_file_name(self):
        self.check_download_file_name(is_header_present=True, is_version_present=True)

    def test_get_download_file_name_not_in_header_without_version_returns_default_name(self):
        self.check_download_file_name(is_header_present=False, is_version_present=False)

    def test_get_download_file_name_not_in_header_with_version_returns_default_name(self):
        self.check_download_file_name(is_header_present=False, is_version_present=True)

    @patch('prestoadmin.server.UrlHandler')
    def test_download_rpm(self, mock_url_handler):
        instance_url_handler = mock_url_handler.return_value
        instance_url_handler.read_block.side_effect = ['abc', 'def', None]
        instance_url_handler.get_content_length.return_value = 6
        fd = -1
        absolute_path_valid_rpm = None
        try:
            fd, absolute_path_valid_rpm = tempfile.mkstemp()
            instance_url_handler.get_download_file_name.return_value = os.path.basename(absolute_path_valid_rpm)
            downloader = server.PrestoRpmDownloader(instance_url_handler)
            downloader.download_rpm('0.148')
            instance_url_handler.get_download_file_name.assert_called_with('0.148')
            with open(absolute_path_valid_rpm) as download_file:
                self.assertEqual(download_file.read(), 'abcdef')
        finally:
            os.close(fd)
            os.remove(absolute_path_valid_rpm)

    def check_version(self, version, expect_valid):
        rpm_fetcher = server.PrestoRpmFetcher(version)
        is_valid_version = rpm_fetcher.check_valid_version()
        if expect_valid:
            self.assertTrue(is_valid_version)
        else:
            self.assertFalse(is_valid_version)

    def test_check_version_empty_string_fails(self):
        self.check_version('', False)

    def test_check_version_major_succeeds(self):
        self.check_version('1', True)

    def test_check_version_major_extra_period_fails(self):
        self.check_version('1.', False)

    def test_check_version_minor_succeeds(self):
        self.check_version('1.2', True)

    def test_check_version_minor_extra_period_fails(self):
        self.check_version('1.2.', False)

    def test_check_version_patch_succeeds(self):
        self.check_version('1.2.3', True)

    def test_check_version_patch_extra_period_fails(self):
        self.check_version('1.2.3.', False)

    def test_check_version_multiple_numbers_succeeds(self):
        self.check_version('111.222.333', True)

    def test_check_version_with_dashes_fails(self):
        self.check_version('1-2-3', False)

    def test_check_version_extra_fields_fails(self):
        self.check_version('1.2.3.4', False)

    @staticmethod
    def set_up_specifier_find_and_download_mocks(mock_download_rpm, mock_find_local, rpm_path, location=None):
        if location == 'local':
            mock_find_local.return_value = rpm_path
        elif location == 'download':
            mock_download_rpm.return_value = rpm_path
            mock_find_local.return_value = None
        elif location == 'none':
            mock_download_rpm.return_value = None
            mock_find_local.return_value = None
        else:
            exit('Cannot mock because of invalid location: %s' % location)

    def call_and_assert_install_with_rpm_specifier(self, mock_download_rpm, mock_check_rpm, mock_execute, location,
                                                   rpm_specifier, rpm_path):
        if location == 'local' or location == 'download':
            server.install(rpm_specifier)
            if location == 'local':
                mock_download_rpm.assert_not_called()
            else:
                self.assertTrue(mock_download_rpm.called)
            mock_check_rpm.assert_called_with(rpm_path)
            mock_execute.assert_called_with(server.deploy_install_configure,
                                            rpm_path, hosts=get_host_list())
        elif location == 'none':
            self.assertRaises(SystemExit, server.install, rpm_specifier)
            mock_check_rpm.assert_not_called()
            mock_execute.assert_not_called()
        else:
            exit('Cannot assert because of invalid location: %s' % location)

    @patch('prestoadmin.server.execute')
    @patch('prestoadmin.server.package.check_if_valid_rpm')
    @patch('prestoadmin.server.LocalPrestoRpmFinder.find_local_presto_rpm')
    @patch('prestoadmin.server.PrestoRpmDownloader.download_rpm')
    def check_rpm_specifier_with_location(self, mock_download_rpm, mock_find_local,
                                          mock_check_rpm, mock_execute, rpm_specifier, location=None):
        # This function should not mock the UrlHandler class so that urls will be opened
        # This checks that the urls that the installer tries to reach are still valid
        rpm_path = '/path/to/download_or_found/rpm'
        TestInstall.set_up_specifier_find_and_download_mocks(mock_download_rpm, mock_find_local, rpm_path, location)
        self.call_and_assert_install_with_rpm_specifier(mock_download_rpm, mock_check_rpm, mock_execute, location,
                                                        rpm_specifier, rpm_path)

    def test_specifier_as_latest_download(self):
        self.check_rpm_specifier_with_location(rpm_specifier='latest', location='download')

    def test_specifier_as_latest_found_locally(self):
        self.check_rpm_specifier_with_location(rpm_specifier='latest', location='local')

    def test_specifier_as_latest_not_located(self):
        self.check_rpm_specifier_with_location(rpm_specifier='latest', location='none')

    def test_specifier_as_url_download(self):
        self.check_rpm_specifier_with_location(rpm_specifier='http://search.maven.org/remotecontent?filepath=com/'
                                                             'facebook/presto/presto-server-rpm/0.148/'
                                                             'presto-server-rpm-0.148.rpm',
                                               location='download')

    def test_specifier_as_url_found_locally(self):
        self.check_rpm_specifier_with_location(rpm_specifier='http://search.maven.org/remotecontent?filepath=com/'
                                                             'facebook/presto/presto-server-rpm/0.148/'
                                                             'presto-server-rpm-0.148.rpm',
                                               location='local')

    def test_specifier_as_url_not_located(self):
        self.check_rpm_specifier_with_location(rpm_specifier='http://search.maven.org/remotecontent?filepath=com/'
                                                             'facebook/presto/presto-server-rpm/0.148/'
                                                             'presto-server-rpm-0.148.rpm',
                                               location='none')

    def test_specifier_as_version_download(self):
        self.check_rpm_specifier_with_location(rpm_specifier='0.144.6', location='download')

    def test_specifier_as_version_found_locally(self):
        self.check_rpm_specifier_with_location(rpm_specifier='0.144.6', location='local')

    def test_specifier_as_version_not_located(self):
        self.check_rpm_specifier_with_location(rpm_specifier='0.144.6', location='none')

    def test_specifier_as_local_path_without_file_scheme_found_locally(self):
        self.check_rpm_specifier_with_location(rpm_specifier='/path/to/rpm', location='local')

    def test_specifier_as_local_path_without_file_scheme_not_located(self):
        self.check_rpm_specifier_with_location(rpm_specifier='/path/to/rpm', location='none')

    def test_specifier_as_local_path_with_file_scheme_found_locally(self):
        self.check_rpm_specifier_with_location(rpm_specifier='file:///path/to/rpm', location='local')

    def test_specifier_as_local_path_with_file_scheme_not_located(self):
        self.check_rpm_specifier_with_location(rpm_specifier='file:///path/to/rpm', location='none')

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.package.deploy_install')
    @patch('prestoadmin.server.update_configs')
    def test_deploy_install_configure(self, mock_update, mock_install,
                                      mock_sudo):
        rpm_specifier = "/any/path/rpm"
        mock_sudo.side_effect = self.mock_fail_then_succeed()

        server.deploy_install_configure(rpm_specifier)
        mock_install.assert_called_with(rpm_specifier)
        self.assertTrue(mock_update.called)
        mock_sudo.assert_called_with('getent passwd presto', quiet=True)

    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.package.is_rpm_installed')
    @patch('prestoadmin.package.rpm_uninstall')
    def test_uninstall_is_called(self, mock_package_rpm_uninstall, mock_package_is_rpm_installed, mock_version_check):
        env.host = "any_host"
        mock_package_is_rpm_installed.side_effect = [False, True]

        server.uninstall()

        mock_version_check.assert_called_with()
        mock_package_is_rpm_installed.assert_called_with('presto-server')
        mock_package_rpm_uninstall.assert_called_with('presto-server')
        self.assertTrue(mock_package_is_rpm_installed.call_count == 2)
        self.assertTrue(mock_package_rpm_uninstall.call_count == 1)

    @patch('prestoadmin.util.presto_config.PrestoConfig.coordinator_config',
           return_value=PRESTO_CONFIG)
    @patch('prestoadmin.util.remote_config_util.lookup_in_config')
    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.query_server_for_status')
    @patch('prestoadmin.server.warn')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_start_fail(self, mock_port_in_use,
                               mock_version_check, mock_warn,
                               mock_query_for_status, mock_sudo, mock_run, mock_config,
                               mock_presto_config):
        mock_query_for_status.return_value = False
        env.host = "failed_node1"
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 0
        mock_config.return_value = None
        server.start()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        mock_warn.assert_called_with(self.SERVER_FAIL_MSG)

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_start(self, mock_port_in_use, mock_version_check,
                          mock_check_status, mock_sudo):
        env.host = 'good_node'
        mock_version_check.return_value = ''
        mock_check_status.return_value = True
        mock_port_in_use.return_value = 0
        server.start()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        self.assertEqual('Waiting to make sure we can connect to the Presto '
                         'server on good_node, please wait. This check will '
                         'time out after 2 minutes if the server does not '
                         'respond.\nServer started successfully on: '
                         'good_node\n', self.test_stdout.getvalue())

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_start_bad_presto_version(self, mock_port_in_use,
                                             mock_version_check, mock_sudo):
        env.host = "good_node"
        mock_version_check.return_value = 'Presto not installed'
        server.start()
        mock_version_check.assert_called_with()
        self.assertEqual(False, mock_sudo.called)

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_start_port_in_use(self, mock_port_in_use,
                                      mock_version_check, mock_sudo):
        env.host = "good_node"
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 1
        server.start()
        mock_version_check.assert_called_with()
        mock_port_in_use.assert_called_with('good_node')
        self.assertEqual(False, mock_sudo.called)

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_status_for_control_commands')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_restart_port_in_use(self, mock_port_in_use,
                                        mock_version_check, mock_check_status,
                                        mock_sudo):
        env.host = "good_node"
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 1
        server.restart()
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' stop')
        mock_version_check.assert_called_with()
        self.assertEqual(False, mock_check_status.called)

    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    @patch('prestoadmin.server.sudo')
    def test_server_stop(self, mock_sudo, mock_port_in_use,
                         mock_version_check):
        mock_version_check.return_value = ''
        server.stop()
        mock_version_check.assert_called_with()
        self.assertEqual(False, mock_port_in_use.called)
        mock_sudo.assert_called_with('set -m; ' + INIT_SCRIPTS + ' stop')

    @patch('prestoadmin.util.remote_config_util.lookup_in_config')
    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.warn')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_restart_fail(self, mock_port_in_use, mock_version_check,
                                 mock_warn, mock_status, mock_sudo,
                                 mock_config):
        mock_status.return_value = False
        mock_config.return_value = None
        env.host = "failed_node1"
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 0
        server.restart()
        mock_sudo.assert_any_call('set -m; ' + INIT_SCRIPTS + ' stop')
        mock_sudo.assert_any_call('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()

        mock_warn.assert_called_with(self.SERVER_FAIL_MSG)

    @patch('prestoadmin.util.remote_config_util.lookup_port')
    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.check_server_status')
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.is_port_in_use')
    def test_server_restart(self, mock_port_in_use, mock_version_check,
                            mock_status, mock_sudo, mock_lookup_host):
        mock_status.return_value = True
        env.host = 'good_node'
        mock_version_check.return_value = ''
        mock_port_in_use.return_value = 0
        server.restart()
        mock_sudo.assert_any_call('set -m; ' + INIT_SCRIPTS + ' stop')
        mock_sudo.assert_any_call('set -m; ' + INIT_SCRIPTS + ' start')
        mock_version_check.assert_called_with()
        self.assertEqual('Waiting to make sure we can connect to the Presto '
                         'server on good_node, please wait. This check will '
                         'time out after 2 minutes if the server does not '
                         'respond.\nServer started successfully on: '
                         'good_node\n', self.test_stdout.getvalue())

    @patch('prestoadmin.server.catalog')
    @patch('prestoadmin.server.configure_cmds.deploy')
    @patch('prestoadmin.server.os.path.exists')
    @patch('prestoadmin.server.os.makedirs')
    @patch('prestoadmin.server.util.filesystem.os.fdopen')
    @patch('prestoadmin.server.util.filesystem.os.open')
    def test_update_config(self, mock_open, mock_fdopen, mock_makedir,
                           mock_path_exists, mock_config, mock_connector):
        e = ConfigFileNotFoundError(
            message='problems', config_path='config_path')
        mock_connector.add.side_effect = e
        mock_path_exists.side_effect = [False, False]

        server.update_configs()

        mock_config.assert_called_with()
        mock_makedir.assert_called_with(get_catalog_directory())
        mock_open.assert_called_with(os.path.join(get_catalog_directory(),
                                                  'tpch.properties'),
                                     os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        file_manager = mock_fdopen.return_value.__enter__.return_value
        file_manager.write.assert_called_with("connector.name=tpch")

    @patch('prestoadmin.util.presto_config.PrestoConfig.coordinator_config',
           return_value=PRESTO_CONFIG)
    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.lookup_string_config')
    @patch.object(PrestoClient, 'run_sql')
    def test_check_success_status(self, mock_run_sql, string_config_mock, mock_run, mock_presto_config):
        env.roledefs = {
            'coordinator': ['Node1'],
            'worker': ['Node1', 'Node2', 'Node3', 'Node4'],
            'all': ['Node1', 'Node2', 'Node3', 'Node4']
        }
        env.hosts = env.roledefs['all']
        env.host = 'Node1'
        string_config_mock.return_value = 'Node1'
        mock_run_sql.return_value = [['Node2', 'some stuff'], ['Node1', 'some other stuff']]
        self.assertEqual(server.check_server_status(), True)

    @patch('prestoadmin.util.presto_config.PrestoConfig.coordinator_config',
           return_value=PRESTO_CONFIG)
    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.lookup_string_config')
    @patch('prestoadmin.server.query_server_for_status')
    def test_check_success_fail(self, mock_query_for_status, string_config_mock, mock_run,
                                mock_presto_config):
        env.roledefs = {
            'coordinator': ['Node1'],
            'worker': ['Node1', 'Node2', 'Node3', 'Node4'],
            'all': ['Node1', 'Node2', 'Node3', 'Node4']
        }
        env.hosts = env.roledefs['all']
        env.host = 'Node1'
        string_config_mock.return_value = 'Node1'
        mock_query_for_status.return_value = False
        self.assertEqual(server.check_server_status(), False)

    @patch('prestoadmin.util.presto_config.PrestoConfig.coordinator_config',
           return_value=PRESTO_CONFIG)
    @patch('prestoadmin.server.execute')
    @patch('prestoadmin.server.get_presto_version')
    @patch('prestoadmin.server.presto_installed')
    @patch.object(PrestoClient, 'run_sql')
    def test_status_from_each_node(
            self, mock_run_sql, mock_presto_installed, mock_get_presto_version, mock_execute, mock_presto_config):
        env.roledefs = {
            'coordinator': ['Node1'],
            'worker': ['Node1', 'Node2', 'Node3', 'Node4'],
            'all': ['Node1', 'Node2', 'Node3', 'Node4']
        }
        env.hosts = env.roledefs['all']

        mock_get_presto_version.return_value = '0.97-SNAPSHOT'
        mock_run_sql.side_effect = [
            [['select * from system.runtime.nodes']],
            [['hive'], ['system'], ['tpch']],
            [['http://active/statement', 'presto-main:0.97-SNAPSHOT', True]],
            [['http://inactive/stmt', 'presto-main:0.99-SNAPSHOT', False]],
            [[]],
            [['http://servrdown/statement', 'any', True]]
        ]
        mock_execute.side_effect = [{
            'Node1': ('IP1', True, ''),
            'Node2': ('IP2', True, ''),
            'Node3': ('IP3', True, ''),
            'Node4': Exception('Timed out trying to connect to Node4')
        }]
        env.host = 'Node1'
        server.status()

        expected = self.read_file_output('/resources/server_status_out.txt')
        self.assertEqual(
            expected.splitlines(),
            self.test_stdout.getvalue().splitlines()
        )

    @patch('prestoadmin.util.presto_config.PrestoConfig.coordinator_config',
           return_value=PRESTO_CONFIG)
    @patch('prestoadmin.server.check_presto_version')
    @patch('prestoadmin.server.service')
    @patch('prestoadmin.server.get_ext_ip_of_node')
    def test_collect_node_information(self, mock_ext_ip, mock_service,
                                      mock_version, mock_presto_config):
        env.roledefs = {
            'coordinator': ['Node1'],
            'all': ['Node1']
        }
        mock_ext_ip.side_effect = ['IP1', 'IP3', 'IP4']
        mock_service.side_effect = [True, False, Exception('Not running')]
        mock_version.side_effect = ['', 'Presto not installed', '', '']

        self.assertEqual(('IP1', True, ''), server.collect_node_information())
        self.assertEqual(('Unknown', False, 'Presto not installed'),
                         server.collect_node_information())
        self.assertEqual(('IP3', False, ''), server.collect_node_information())
        self.assertEqual(('IP4', False, ''),
                         server.collect_node_information())

    @patch('prestoadmin.server.sudo')
    def test_get_external_ip(self, mock_nodeuuid):
        client_mock = MagicMock(PrestoClient)
        client_mock.run_sql.return_value = [['IP']]
        self.assertEqual(server.get_ext_ip_of_node(client_mock), 'IP')

    @patch('prestoadmin.server.sudo')
    @patch('prestoadmin.server.warn')
    def test_warn_external_ip(self, mock_warn, mock_nodeuuid):
        env.host = 'node'
        client_mock = MagicMock(PrestoClient)
        client_mock.run_sql.return_value = [['IP1'], ['IP2']]
        server.get_ext_ip_of_node(client_mock)
        mock_warn.assert_called_with("More than one external ip found for "
                                     "node. There could be multiple nodes "
                                     "associated with the same node.id")

    def read_file_output(self, filename):
        dir = os.path.abspath(os.path.dirname(__file__))
        result_file = open(dir + filename, 'r')
        file_content = "".join(result_file.readlines())
        result_file.close()
        return file_content

    @patch('prestoadmin.util.presto_config.PrestoConfig.coordinator_config',
           return_value=PRESTO_CONFIG)
    @patch.object(PrestoClient, 'run_sql')
    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.warn')
    def test_warning_presto_version_not_installed(self, mock_warn, mock_run,
                                                  mock_run_sql, mock_presto_config):
        env.host = 'node1'
        env.roledefs['coordinator'] = ['node1']
        env.roledefs['worker'] = ['node1']
        env.roledefs['all'] = ['node1']
        env.hosts = env.roledefs['all']
        output = _AttributeString('package presto is not installed')
        output.succeeded = False
        mock_run.return_value = output
        env.host = 'node1'
        server.collect_node_information()
        installation_warning = 'Presto is not installed.'
        mock_warn.assert_called_with(installation_warning)

    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.lookup_port')
    @patch('prestoadmin.server.error')
    def test_fail_if_port_is_in_use(self, mock_error, mock_port, mock_run):
        mock_port.return_value = 1010
        env.host = 'any_host'
        mock_run.return_value = 'some_string'
        server.is_port_in_use(env.host)
        mock_error.assert_called_with('Server failed to start on any_host. '
                                      'Port 1010 already in use')

    @patch('prestoadmin.server.run')
    @patch('prestoadmin.server.lookup_port')
    @patch('prestoadmin.server.warn')
    def test_no_warn_if_port_free(self, mock_warn, mock_port, mock_run):
        mock_port.return_value = 1010
        env.host = 'any_host'
        mock_run.return_value = ''
        server.is_port_in_use(env.host)
        self.assertEqual(False, mock_warn.called)

    @patch('prestoadmin.server.lookup_port')
    @patch('prestoadmin.server.warn')
    def test_no_warn_if_port_lookup_fail(self, mock_warn, mock_port):
        e = ConfigurationError()
        mock_port.side_effect = e
        env.host = 'any_host'
        self.assertFalse(server.is_port_in_use(env.host))
        self.assertEqual(False, mock_warn.called)

    @patch('prestoadmin.server.run')
    def test_multiple_version_rpms(self, mock_run):
        output1 = _AttributeString('package presto is not installed')
        output1.succeeded = False
        output2 = _AttributeString('presto-server-rpm-0.115t-1.x86_64')
        output2.succeeded = True
        output3 = _AttributeString('Presto is not installed.')
        output3.succeeded = False
        output4 = _AttributeString('0.111.SNAPSHOT')
        output4.succeeded = True

        mock_run.side_effect = [output1, output2, output3, output4]

        expected = server.check_presto_version()
        mock_run.assert_has_calls([
            call('rpm -q presto'),
            call('rpm -q presto-server-rpm')
        ])
        self.assertEqual(expected, '')

    def mock_fail_then_succeed(self):
        output1 = _AttributeString()
        output1.succeeded = False
        output2 = _AttributeString()
        output2.succeeded = True
        return [output1, output2]
