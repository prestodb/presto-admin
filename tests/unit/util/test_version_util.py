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
Tests for version ranges
"""

from prestoadmin.util.version_util import VersionRange, VersionRangeList, \
    strip_tag, split_version

from tests.unit.base_unit_case import BaseUnitCase


class TestVersionRange(BaseUnitCase):
    def test_pad_tuple_bad_len(self):
        self.assertRaises(AssertionError, VersionRange.pad_tuple, (1, 2), 0, 0)
        self.assertRaises(AssertionError, VersionRange.pad_tuple, (1, 2), 1, 0)

    def test_pad_tuple(self):
        self.assertEquals((1, 2, 0, 0), VersionRange.pad_tuple((1, 2), 4, 0))

    def test_invalid_range(self):
        # Empty intervals, min == max
        self.assertRaises(AssertionError, VersionRange, (1, 0), (1, 0))
        self.assertRaises(AssertionError, VersionRange, (1, 0), (1, ))
        self.assertRaises(AssertionError, VersionRange, (1, ), (1, 0))

        # Empty interval max > min
        self.assertRaises(AssertionError, VersionRange, (2, 0), (1, 0))

        # Bare integers for min, max disallowed
        self.assertRaises(AssertionError, VersionRange, (0), (2,))
        self.assertRaises(AssertionError, VersionRange, (1,), (2))

    def test_contains(self):
        vr = VersionRange((2171, 0), (2179, 0))
        self.assertNotIn(('2170', '9'), vr)
        self.assertNotIn((2170, 9, 2, 718, 28, 18284, 590, 4523, 536), vr)
        self.assertIn((2171, 0, 0), vr)
        self.assertIn([2171, 1], vr)
        self.assertIn(('2175',), vr)
        self.assertIn((2178, 3, 1, 4, 1, 59, 26535, 89793), vr)
        self.assertNotIn([2179], vr)
        self.assertNotIn((2179, 1), vr)

    def test_strip_td(self):
        self.assertEquals((0, 123), VersionRange.strip_td_suffix((0, '123t')))
        self.assertEquals((0, 123, 1, 0), VersionRange.strip_td_suffix((0, 123, 't', 1, 0)))

    def test_contains_teradata(self):
        vr = VersionRange((0,), (0, 128))
        self.assertIn((0, '115t'), vr)
        self.assertIn(('0', '115t'), vr)
        self.assertIn((0, 125, 't', 0, 1), vr)


class TestVersionRangeSet(BaseUnitCase):
    def test_0_length_list(self):
        vl = VersionRangeList()
        self.assertRaises(KeyError, vl.for_version, (1, 0))

    def test_1_length_list(self):
        vl = VersionRangeList(
            VersionRange((0,), (1, 0)))
        self.assertIsNone(vl.for_version((0, 5)))

    def test_valid_2_length_list(self):
        vl = VersionRangeList(
            VersionRange((0,), (1, 0), '0'),
            VersionRange((1, 0), (2, 0), '1'))
        self.assertEqual('0', vl.for_version((0, 5)))
        self.assertEqual('1', vl.for_version((1, 5)))

    def test_discontinuous_2_length_list(self):
        self.assertRaises(
            AssertionError, VersionRangeList,
            VersionRange((0,), (1, 0)), VersionRange((1, 1), (2, 0)))

    def test_bad_order_2_length_list(self):
        self.assertRaises(
            AssertionError, VersionRangeList,
            VersionRange((1, 0), (2, 0)), VersionRange((0,), (1, 0)))

    def test_overlapping_2_length_list(self):
        self.assertRaises(
            AssertionError, VersionRangeList,
            VersionRange((0,), (1, 0)), VersionRange((0, 9), (2, 0)))


class TestVersionUtils(BaseUnitCase):
    def test_all_numeric(self):
        self.assertEqual((1, 2), strip_tag(('1', '2')))
        self.assertEqual((1, 2), strip_tag(['1', '2']))

    def test_trailing_non_numeric(self):
        self.assertEqual(
            (1, 2), strip_tag(('1', '2', 'THREE', 'FOUR')))
        self.assertEqual(
            (1, 2), strip_tag(['1', '2', 'THR']))

    def test_ancient_tags(self):
        # Teradata and non-Teradata versions
        self.assertEqual(
            (0, '97t'), strip_tag(('0', '97t', 'SNAPSHOT')))
        self.assertEqual(
            (0, 99), strip_tag(('0', '99', 'SNAPSHOT')))

    def test_non_trailing_non_numeric(self):
        self.assertEqual(
            (1, 3, 't', 4, 't'), strip_tag(('1', 'TWO', '3', 't', '4', 't')))

    def test_no_numeric(self):
        self.assertEqual(
            (), strip_tag(('ONE', 'TWO', 'THREE'))
        )

    def test_split(self):
        self.assertEqual(['1', '2', '3'], split_version(' \t 1.2.3  \t '))
        self.assertEqual(['0', '115t'], split_version('0.115t'))
        self.assertEqual(['0', '115t', 'SNAPSHOT'], split_version('0.115t-SNAPSHOT'))

    def test_old_teradata_version(self):
        self.assertEqual(
            (0, '115t'), strip_tag(('0', '115t')))
        self.assertEqual(
            (0, '123t'), strip_tag(('0', '123t', 'SNAPSHOT')))

    def test_new_teradata_version(self):
        self.assertEqual(
            (0, 148, 't'), strip_tag(('0', '148', 't'))
        )
        self.assertEqual(
            (0, 148, 't', 0, 1), strip_tag(('0', '148', 't', '0', '1'))
        )
        self.assertEqual(
            (0, 148, 't'), strip_tag(('0', '148', 'snapshot', 't', 'snapshot'))
        )
