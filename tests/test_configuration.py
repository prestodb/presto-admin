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

from prestoadmin import configuration as config
import os
import utils


class TestConfiguration(utils.BaseTestCase):
    def test_file_does_not_exist(self):
        self.assertRaisesRegexp(IOError,
                                "No such file or directory",
                                config.get_conf_from_file,
                                ("does/not/exist/conf.json"))

    def test_invalid_json(self):
        self.assertRaisesRegexp(ValueError,
                                "Expecting , delimiter: line 3 column 3 "
                                "\(char 19\)",
                                config.get_conf_from_file,
                                (os.path.dirname(__file__) +
                                 "/files/invalid_json_conf.json"))

    def test_fill_defaults_no_missing(self):
        orig = {"key1": "val1", "key2": "val2", "key3": "val3"}
        defaults = {"key1": "default1", "key2": "default2"}
        filled = orig.copy()
        config.fill_defaults(filled, defaults)
        self.assertEqual(filled, orig)

    def test_fill_defaults(self):
        orig = {"key1": "val1",  "key3": "val3"}
        defaults = {"key1": "default1", "key2": "default2"}
        filled = orig.copy()
        config.fill_defaults(filled, defaults)
        self.assertEqual(filled,
                         {"key1": "val1", "key2": "default2", "key3": "val3"})
