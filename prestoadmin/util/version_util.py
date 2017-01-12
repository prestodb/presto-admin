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
Stuff to handle version ranges.
"""

import re

TD_VERSION = re.compile(r'^\d+t$')


def split_version(version_string):
    # We split on '.' and '-' because ancient tagged versions had the tag
    # delimited by a '-'
    return re.split('\.|-', version_string.strip())


def get_int_or_t(x):
    try:
        return int(x)
    except ValueError as e:
        if x is 't':
            return x
        if x[-1] is 't':
            int(x[:-1])
            return x
        raise e


def is_int_or_t(x):
    try:
        get_int_or_t(x)
        return True
    except ValueError:
        return False


def strip_tag(version):
    """
    Strip any parts of the version that are not numeric components or t's
    We leave the 't' on numeric components if it's present.
    ['1', '2', 'THREE'] -> (1, 2)
    ['1', 'TWO', '3'] -> (1, 3)
    ['0', '115t', 'SNAPSHOT'] -> (0, '115t')
    ['ZERO', '123t'] -> (123t)
    ['0', '148', 't'] => (0, 148, 't')
    ['0', '148', 't', 0, 1] => (0, 148, 't', 0, 1)
    ['0', '148', 't', 0, 1, 'SNAPSHOT'] => (0, 148, 't', 0, 1)
    ['0', '162', 'SNAPSHOT', 't', 'SNAPSHOT'] => (0, 162, 't')

    This checks the components of the version from least to most significant.

    :param version: something that can be sliced
    :return: a tuple containing only integer components or the letter t
    """

    result = list(version[:])
    result = [get_int_or_t(x) for x in result if is_int_or_t(x)]
    return tuple(result)


class VersionRange(object):
    """
    Represents a range of version numbers [min_version, max_version).
    The interval is right-open so that you can construct a numerically
    continuous list of versions like so:
    l = [VersionRange((0, 0), (0, 5)), VersionRange((0, 5), (1, 0))]
    and for all versions v where 0.0 <= v < 1.0 is contained in exactly one
    VersionRange in l.

    Continuity between version ranges can be checked using is_continuous.

    VersionRanges understand how to check if a Teradata version is contained
    in a VersionRange, but do no special handling to accomodate Teradata
    versions in their internal min_version and max_version members. I.e.,
    creating a VersionRange with a Teradata version will work, but __contains__
    will not work correctly. We don't currently need this, and hope not to.

    Note that the right-open interval representation of a version range does
    NOT allow the creation of a VersionRange that contains exactly one version.

    Note that empty intervals cannot be constructed as the serve no useful
    purpose. Specifically, we assert that min_version < max_version in the
    constructor.
    """

    def __init__(self, min_version, max_version, versioned_thing=None):
        # not pythonic, but bare ints screw things up.
        assert isinstance(min_version, tuple)
        assert isinstance(max_version, tuple)
        l = max(len(min_version), len(max_version))
        min_pad = VersionRange.pad_tuple(min_version, l, 0)
        max_pad = VersionRange.pad_tuple(max_version, l, 0)
        assert min_pad < max_pad
        self.min_version = min_version
        self.max_version = max_version
        self.versioned_thing = versioned_thing

    def __str__(self):
        return '[%s, %s) -> %s' % (
            '.'.join([str(c) for c in self.min_version]),
            '.'.join([str(c) for c in self.max_version]),
            self.versioned_thing)

    @staticmethod
    def strip_td_suffix(version):
        new_version = ()
        for component in version:
            if TD_VERSION.match(str(component)):
                new_last = component[:-1]
                new_version += (int(new_last),)
            elif component is not 't':
                new_version += (int(component),)

        return new_version

    @staticmethod
    def pad_tuple(tup, length, pad):
        assert len(tup) <= length
        result = list(tup)
        while len(result) < length:
            result.append(pad)
        return tuple(result)

    def zero_pad(self, other):
        """
        Pad out min_version, max_version, and other with zeroes to the length
        of the longest of the three. This allows subsequent comparisons to work
        as expected when tuples are of unequal length.
        Returns a tuple of tuples padded out to the same length
        """
        l = max(len(self.min_version), len(self.max_version), len(other))
        return (self.pad_tuple(self.min_version, l, 0),
                self.pad_tuple(self.max_version, l, 0),
                self.pad_tuple(other, l, 0))

    def __contains__(self, other):
        other = self.strip_td_suffix(other)
        other = tuple([int(component) for component in other])

        min_pad, max_pad, o_pad = self.zero_pad(other)
        return min_pad <= o_pad and o_pad < max_pad

    def is_continuous(self, next):
        min_pad, max_pad, next_min_pad = self.zero_pad(next.min_version)
        return max_pad == next_min_pad


class VersionRangeList(object):
    """
    A VersionRangeList is a list of continuous, non-overlapping VersionRanges.
    This is guaranteed by calling VersionRange.is_continuous on all pairs of
    VersionRanges vr[i], vr[i + 1] in the list, which ensures that the list is
    both sorted in order of ascending version and that the interval
    [vr[0].min_version, vr[n].max_version) has no discontinuities.
    """

    def __init__(self, *range_list):
        if len(range_list) >= 2:
            for i in range(0, len(range_list) - 1):
                assert range_list[i].is_continuous(range_list[i + 1])

        self.range_list = range_list

    def __str__(self):
        return '\n'.join([str(vr) for vr in self.range_list])

    def for_version(self, version):
        for range in self.range_list:
            if version in range:
                return range.versioned_thing
        raise KeyError(version)
