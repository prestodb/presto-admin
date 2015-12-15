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

from json import loads

from prestoadmin.util.json_tranform import as_key, transform, purge

from tests.unit.base_unit_case import BaseUnitCase


class TestJsonTransform(BaseUnitCase):
    def run_it(self, expected, json_string, transformations):
        initial = loads(json_string)
        transformed = transform(initial, transformations)
        self.assertEqual(expected, transformed)

    def test_as_key(self):
        # Test both uses: join a list as used internally, and join args as used
        # by somebody creating a dict of transformations
        self.assertEqual('foo/bar', as_key(['foo', 'bar']))
        self.assertEqual('foo/bar', as_key('foo', 'bar'))

    def test_empty(self):
        json = '{}'
        expected = {}

        self.run_it(expected, json, {})

    def test_simple(self):
        json = '{ "string":"hello", "number":2179 }'
        expected = {
            'string': 'hello',
            'number': 2179
        }

        self.run_it(expected, json, {})

    def test_change_top_level_value(self):
        json = '{ "string":"hello", "number":2179 }'
        transformations = {
            as_key('number'): lambda path, value: 2181
        }

        expected = {
            'string': 'hello',
            'number': 2181
        }

        self.run_it(expected, json, transformations)

    def test_purge_top_level_value(self):
        json = '{ "string":"hello", "number":2179 }'
        transformations = {
            as_key('number'): purge
        }

        expected = {
            'string': 'hello',
        }

        self.run_it(expected, json, transformations)

    def test_change_top_level_list(self):
        json = '{ "string":"hello", "list":[1, 2, 3, 4] }'
        transformations = {
            as_key('list'): lambda path, value: [v + 1 for v in value]
        }

        expected = {
            'string': 'hello',
            'list': [2, 3, 4, 5]
        }

        self.run_it(expected, json, transformations)

    def test_purge_list(self):
        json = '{ "string":"hello", "list":[1, 2, 3, 4] }'
        transformations = {
            as_key('list'): purge
        }

        expected = {
            'string': 'hello'
        }

        self.run_it(expected, json, transformations)

    def test_change_child_object_value(self):
        json = '{ "string":"hello", "object":{"k1":"value", "k2":2179}}'
        transformations = {
            as_key('object', 'k2'): lambda path, value: 2181
        }

        expected = {
            'string': 'hello',
            'object': {
                'k1': 'value',
                'k2': 2181
            }
        }

        self.run_it(expected, json, transformations)

    def test_purge_child_object_value(self):
        json = '{ "string":"hello", "object":{"k1":"value", "k2":2179}}'
        transformations = {
            as_key('object', 'k2'): purge
        }

        expected = {
            'string': 'hello',
            'object': {
                'k1': 'value',
            }
        }

        self.run_it(expected, json, transformations)

    def test_purge_top_level_obj(self):
        json = '{ "string":"hello", "object":{"k1":"value", "k2":2179}}'
        transformations = {
            as_key('object'): purge
        }

        expected = {
            'string': 'hello',
        }

        self.run_it(expected, json, transformations)
