import errno
from mock import patch
from prestoadmin.util import filesystem
from tests.utils import BaseTestCase


class TestFilesystem(BaseTestCase):
    @patch('prestoadmin.util.filesystem.os.fdopen')
    @patch('prestoadmin.util.filesystem.os.open')
    @patch('prestoadmin.util.filesystem.os.makedirs')
    def test_write_file_exits(self, makedirs_mock, open_mock, fdopen_mock):
        makedirs_mock.side_effect = OSError(errno.EEXIST, 'message')
        open_mock.side_effect = OSError(errno.EEXIST, 'message')
        filesystem.write_to_file_if_not_exists('content', 'path/to/anyfile')
        self.assertFalse(fdopen_mock.called)

    @patch('prestoadmin.util.filesystem.os.makedirs')
    def test_write_file_error_in_dirs(self, makedirs_mock):
        makedirs_mock.side_effect = OSError(errno.EACCES, 'message')
        self.assertRaisesRegexp(OSError, 'message',
                                filesystem.write_to_file_if_not_exists,
                                'content', 'path/to/anyfile')

    @patch('prestoadmin.util.filesystem.os.makedirs')
    @patch('prestoadmin.util.filesystem.os.open')
    def test_write_file_error_in_files(self, open_mock, makedirs_mock):
        open_mock.side_effect = OSError(errno.EACCES, 'message')
        self.assertRaisesRegexp(OSError, 'message',
                                filesystem.write_to_file_if_not_exists,
                                'content', 'path/to/anyfile')
