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
Module for validating functionality in BaseTestCase.
"""

from base_unit_case import BaseUnitCase


class TestBaseTestCase(BaseUnitCase):
    def testLazyPass(self):
        self.assertLazyMessage(
            lambda: self.fail("shouldn't be called"), self.assertEqual, 1, 1)

    def testLazyFail(self):
        a = 2
        e = 1

        self.assertRaisesRegexp(
            AssertionError, 'asdfasdfasdf 2 1', self.assertLazyMessage,
            lambda: 'asdfasdfasdf %d %d' % (a, e), self.assertEqual, a, e)
