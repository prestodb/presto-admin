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

from prestoadmin.util.exception import ExceptionWithCause, \
    ConfigFileNotFoundError

import pickle
import re
from unittest import TestCase


class ExceptionTest(TestCase):

    def test_exception_with_cause(self):
        pass
        try:
            try:
                raise ValueError('invalid parameter!')
            except:
                raise ExceptionWithCause('outer exception')
        except ExceptionWithCause as e:
            self.assertEqual(str(e), 'outer exception')
            m = re.match(
                r'Traceback \(most recent call last\):\n  File ".*", line \d+,'
                ' in test_exception_with_cause\n    raise ValueError\('
                '\'invalid parameter!\'\)\nValueError: invalid parameter!\n',
                e.inner_exception
            )
            self.assertTrue(m is not None)
        else:
            self.fail('ExceptionWithCause should have been raised')

    def test_can_pickle_ConfigFileNotFound(self):
        config_path = '/usa/georgia/macon'
        message = 'I woke up this morning, I had them Statesboro Blues'
        e = ConfigFileNotFoundError(config_path=config_path, message=message)

        ps = pickle.dumps(e, pickle.HIGHEST_PROTOCOL)
        a = pickle.loads(ps)

        self.assertEquals(message, a.message)
        self.assertEquals(config_path, a.config_path)
