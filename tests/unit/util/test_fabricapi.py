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
Tests the utility
"""
from fabric.api import env

from mock import Mock

from prestoadmin.util import fabricapi

from tests.base_test_case import BaseTestCase


class TestFabricapi(BaseTestCase):
    def test_get_host_with_exclude(self):
        env.hosts = ['a', 'b', 'bad']
        env.exclude_hosts = ['bad']
        self.assertEqual(fabricapi.get_host_list(), ['a', 'b'])

    TEST_ROLEDEFS = {
        'coordinator': ['coordinator'],
        'worker': ['worker0', 'worker1', 'worker2']
        }

    def test_by_role_coordinator(self):
        env.roledefs = self.TEST_ROLEDEFS

        callback = Mock()

        fabricapi.by_role_coordinator('worker0', callback)
        self.assertFalse(callback.called, 'coordinator callback called for ' +
                         'worker')
        fabricapi.by_role_coordinator('coordinator', callback)
        callback.assert_any_call()

    def test_by_role_worker(self):
        env.roledefs = self.TEST_ROLEDEFS

        callback = Mock()

        fabricapi.by_role_worker('coordinator', callback)
        self.assertFalse(callback.called, 'worker callback called for ' +
                         'coordinator')
        fabricapi.by_role_worker('worker0', callback)
        callback.assert_any_call()

    def assert_is_worker(self, roledefs):
        def check(*args, **kwargs):
            self.assertTrue(env.host in roledefs.get('worker'))
        return check

    def assert_is_coordinator(self, roledefs):
        def check(*args, **kwargs):
            self.assertTrue(env.host in roledefs.get('coordinator'))
        return check

    def test_by_rolename_worker(self):
        callback = Mock()
        callback.side_effect = self.assert_is_worker(self.TEST_ROLEDEFS)
        env.roledefs = self.TEST_ROLEDEFS

        env.host = 'coordinator'
        fabricapi.by_rolename(env.host, 'worker', callback)
        self.assertFalse(callback.called)

        env.host = 'worker0'
        fabricapi.by_rolename(env.host, 'worker', callback)
        self.assertTrue(callback.called)

    def test_by_rolename_coordinator(self):
        callback = Mock()
        callback.side_effect = self.assert_is_coordinator(self.TEST_ROLEDEFS)
        env.roledefs = self.TEST_ROLEDEFS

        env.host = 'worker0'
        fabricapi.by_rolename(env.host, 'coordinator', callback)
        self.assertFalse(callback.called)

        env.host = 'coordinator'
        fabricapi.by_rolename(env.host, 'coordinator', callback)
        self.assertTrue(callback.called)

    def test_by_rolename_all(self):
        callback = Mock()
        env.roledefs = self.TEST_ROLEDEFS

        env.host = 'worker0'
        fabricapi.by_rolename(env.host, None, callback)
        self.assertTrue(callback.called)

        callback.reset_mock()

        env.host = 'coordinator'
        fabricapi.by_rolename(env.host, None, callback)
        self.assertTrue(callback.called)
