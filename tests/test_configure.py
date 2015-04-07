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
Tests setting the presto configure
"""
from mock import patch

from prestoadmin import configure
import utils


class TestConfigure(utils.BaseTestCase):
    def test_output_format_dict(self):
        conf = {'a': 'b', 'c': 'd'}
        self.assertEqual(configure.output_format(conf),
                         "a=b\nc=d")

    def test_output_format_list(self):
        self.assertEqual(configure.output_format(['a', 'b']),
                         'a\nb')

    def test_output_format_string(self):
        conf = "A string"
        self.assertEqual(configure.output_format(conf), conf)

    def test_output_format_int(self):
        conf = 1
        self.assertEqual(configure.output_format(conf), str(conf))

    @patch('prestoadmin.configure.configure')
    @patch('prestoadmin.configure.util.get_coordinator_role')
    @patch('prestoadmin.configure.env')
    def test_worker_is_coordinator(self, env_mock, coord_mock, configure_mock):
        env_mock.host = "my.host"
        coord_mock.return_value = ["my.host"]
        configure.workers()
        assert not configure_mock.called

    @patch('prestoadmin.configure.configure')
    @patch('prestoadmin.configure.util.get_coordinator_role')
    @patch('prestoadmin.configure.env')
    def test_worker_not_coordinator(self, env_mock, coord_mock,
                                    configure_mock):
        env_mock.host = "my.host1"
        coord_mock.return_value = ["my.host2"]
        configure.workers()
        assert configure_mock.called

    @patch('prestoadmin.configure.configure')
    @patch('prestoadmin.configure.coord.get_conf')
    def test_coordinator(self, coord_mock, configure_mock):
        coord_mock.return_value = {}
        configure.coordinator()
        assert configure_mock.called

    @patch('__builtin__.open')
    @patch('prestoadmin.configure.os.listdir')
    @patch('prestoadmin.configure.sudo')
    @patch('prestoadmin.configure.put')
    @patch('prestoadmin.configure.files.append')
    def test_deploy(self, append_mock, put_mock, sudo_mock,
                    listdir_mock, open_mock):
        listdir_mock.return_value = ["jvm.config", "node.properties"]
        file_manager = open_mock.return_value.__enter__.return_value
        file_manager.read.return_value = ("key=value")
        configure.deploy("/my/local/dir", "/my/remote/dir")

        command = (
            "if ! ( grep -q 'node.id' /my/remote/dir/node.properties ); "
            "then "
            "uuid=$(uuidgen); "
            "echo node.id=$uuid >> /my/remote/dir/node.properties;"
            "fi; "
            "sed -i '/node.id/!d' /my/remote/dir/node.properties; ")
        sudo_mock.assert_called_with(command)
        put_mock.assert_called_with("/my/local/dir/jvm.config",
                                    "/my/remote/dir/jvm.config", True)
        append_mock.assert_called_with("/my/remote/dir/node.properties",
                                       "key=value", True)

    @patch('prestoadmin.configure.config.write')
    def test_write_to_tmp(self, write_mock):
        conf = {"file1": {"k1": "v1", "k2": "v2"},
                "file2": ["i1", "i2", "i3"]}
        conf_dir = "/conf"
        configure.write_conf_to_tmp(conf, conf_dir)
        write_mock.assert_any_call("k1=v1\nk2=v2", "/conf/file1")
        write_mock.assert_any_call("i1\ni2\ni3", "/conf/file2")
