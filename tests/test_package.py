from mock import patch
from prestoadmin import package
from prestoadmin.util import constants
from tests import utils


class TestPackage(utils.BaseTestCase):

    @patch('prestoadmin.package.sudo')
    @patch('prestoadmin.package.put')
    def test_deploy_is_called(self, mock_put, mock_sudo):
        package.deploy("/any/path/rpm")
        mock_sudo.assert_called_with("mkdir -p " +
                                     constants.REMOTE_PACKAGES_PATH)
        mock_put.assert_called_with("/any/path/rpm",
                                    constants.REMOTE_PACKAGES_PATH,
                                    use_sudo=True)

    @patch('prestoadmin.package.sudo')
    def test_rpm_install(self, mock_sudo):
        package.rpm_install("test.rpm")
        mock_sudo.assert_called_with("rpm -i "
                                     "/opt/presto-admin/packages/test.rpm")

    @patch('prestoadmin.package.rpm_install')
    @patch('prestoadmin.package.deploy')
    def test_install(self, mock_deploy, mock_install):
        self.remove_runs_once_flag(package.install)
        package.install("/any/path/rpm")
        mock_deploy.assert_called_with("/any/path/rpm")
        mock_install.assert_called_with("rpm")

    @patch('prestoadmin.package.sudo')
    @patch('prestoadmin.package.put')
    def test_deploy_with_fallback_location(self, mock_put, mock_sudo):
        package.deploy("/any/path/rpm")
        mock_put.return_value = lambda: None
        setattr(mock_put.return_value, 'succeeded', False)
        package.deploy("/any/path/rpm")
        mock_put.assert_called_with("/any/path/rpm",
                                    constants.REMOTE_PACKAGES_PATH,
                                    use_sudo=True,
                                    temp_dir='/tmp')
