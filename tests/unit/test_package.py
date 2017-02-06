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

from fabric.state import env
from fabric.operations import _AttributeString
from mock import patch
from prestoadmin import package
from prestoadmin.util import constants
from tests.unit.base_unit_case import BaseUnitCase


class TestPackage(BaseUnitCase):

    @patch('prestoadmin.package.os.path.isfile')
    @patch('prestoadmin.package.sudo')
    @patch('prestoadmin.package.put')
    def test_deploy_is_called(self, mock_put, mock_sudo, mock_isfile):
        env.host = 'any_host'
        mock_isfile.return_value = True
        package.deploy('/any/path/rpm')
        mock_sudo.assert_called_with('mkdir -p ' +
                                     constants.REMOTE_PACKAGES_PATH)
        mock_put.assert_called_with('/any/path/rpm',
                                    constants.REMOTE_PACKAGES_PATH,
                                    use_sudo=True)

    @patch('prestoadmin.package.sudo')
    def test_rpm_install(self, mock_sudo):
        env.host = 'any_host'
        env.nodeps = False
        package.rpm_install('test.rpm')
        mock_sudo.assert_called_with('rpm -i '
                                     '/opt/prestoadmin/packages/test.rpm')

    @patch('prestoadmin.package.sudo')
    def test_rpm_install_nodeps(self, mock_sudo):
        env.host = 'any_host'
        env.nodeps = True
        package.rpm_install('test.rpm')
        mock_sudo.assert_called_with('rpm -i --nodeps '
                                     '/opt/prestoadmin/packages/test.rpm')

    @patch('prestoadmin.package._rpm_upgrade')
    @patch('prestoadmin.package.sudo')
    def test_rpm_upgrade(self, mock_sudo, mock_rpm_upgrade):
        env.host = 'any_host'
        env.nodeps = False
        mock_sudo.return_value = _AttributeString('test_package_name')
        mock_sudo.return_value.succeeded = True
        package.rpm_upgrade('test.rpm')

        mock_sudo.assert_any_call('rpm -qp --queryformat \'%{NAME}\' '
                                  '/opt/prestoadmin/packages/test.rpm',
                                  quiet=True)

        mock_rpm_upgrade.assert_any_call('/opt/prestoadmin/packages/test.rpm')

    @patch('prestoadmin.package.rpm_install')
    @patch('prestoadmin.package.deploy')
    @patch('prestoadmin.package.check_if_valid_rpm')
    def test_install(self, mock_chksum, mock_deploy, mock_install):
        env.host = 'any_host'
        self.remove_runs_once_flag(package.install)
        package.install('/any/path/rpm')
        mock_chksum.assert_called_with('/any/path/rpm')
        mock_deploy.assert_called_with('/any/path/rpm')
        mock_install.assert_called_with('rpm')

    @patch('prestoadmin.package.local')
    @patch('prestoadmin.package.abort')
    def test_check_rpm_checksum(self, mock_abort, mock_local):
        mock_local.return_value = lambda: None
        setattr(mock_local.return_value, 'stderr', '')
        setattr(mock_local.return_value, 'stdout', 'sha1 MD5 NOT OK')
        package.check_if_valid_rpm('/any/path/rpm')

        mock_local.assert_called_with('rpm -K --nosignature /any/path/rpm',
                                      capture=True)
        mock_abort.assert_called_with('Corrupted RPM. '
                                      'Try downloading the RPM again.')

    @patch('prestoadmin.package.local')
    @patch('prestoadmin.package.abort')
    def test_check_rpm_checksum_err(self, mock_abort, mock_local):
        mock_local.return_value = lambda: None
        setattr(mock_local.return_value, 'stderr', 'Not an rpm package')
        setattr(mock_local.return_value, 'stdout', '')
        package.check_if_valid_rpm('/any/path/rpm')

        mock_local.assert_called_with('rpm -K --nosignature /any/path/rpm',
                                      capture=True)
        mock_abort.assert_called_with('Not an rpm package')

    @patch('prestoadmin.package.os.path.isfile')
    @patch('prestoadmin.package.sudo')
    @patch('prestoadmin.package.put')
    def test_deploy_with_fallback_location(self, mock_put, mock_sudo, mock_isfile):
        env.host = 'any_host'
        mock_isfile.return_value = True
        package.deploy('/any/path/rpm')
        mock_put.return_value = lambda: None
        setattr(mock_put.return_value, 'succeeded', False)
        package.deploy('/any/path/rpm')
        mock_put.assert_called_with('/any/path/rpm',
                                    constants.REMOTE_PACKAGES_PATH,
                                    use_sudo=True,
                                    temp_dir='/tmp')

    @patch('prestoadmin.package.os.path.isfile')
    def test_deploy_invalid_local_path(self, mock_isfile):
        mock_isfile.return_value = False
        invalid_path = '/invalid/path'
        self.assertRaisesRegexp(SystemExit, 'RPM file not found at %s' % invalid_path, package.deploy, invalid_path)

    @patch('prestoadmin.package.uninstall')
    def test_uninstall(self, mock_uninstall):
        env.host = 'any_host'
        env.nodeps = False
        self.remove_runs_once_flag(package.uninstall)

        package.uninstall('any_rpm')

        mock_uninstall.assert_called_once_with('any_rpm')

    @patch('prestoadmin.package.sudo')
    def test_rpm_uninstall(self, mock_sudo):
        env.host = 'any_host'
        env.nodeps = False

        package.rpm_uninstall('anyrpm')

        mock_sudo.assert_called_with('rpm -e anyrpm')

    @patch('prestoadmin.package.sudo')
    def test_rpm_uninstall_nodeps(self, mock_sudo):
        env.host = 'any_host'
        env.nodeps = True

        package.rpm_uninstall('anyrpm')

        mock_sudo.assert_called_with('rpm -e --nodeps anyrpm')

    @patch('prestoadmin.package.is_rpm_installed')
    def test_rpm_uninstall_non_existing(self, mock_is_rpm_installed):
        env.host = 'any_host'
        env.force = False
        mock_is_rpm_installed.return_value = False

        try:
            package.rpm_uninstall('anyrpm')
            self.fail('expected exception to be raised here')
        except SystemExit, e:
            self.assertEqual(e.message, '[any_host] Package is not installed: anyrpm')

    @patch('prestoadmin.package.is_rpm_installed')
    @patch('prestoadmin.package.sudo')
    def test_rpm_uninstall_non_existing_with_force(self, mock_sudo, mock_is_rpm_installed):
        env.host = 'any_host'
        env.force = True
        env.nodeps = False
        mock_is_rpm_installed.return_value = False

        package.rpm_uninstall('anyrpm')

        self.assertTrue(mock_sudo.call_count == 0)
