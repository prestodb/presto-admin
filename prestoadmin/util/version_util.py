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
ANCIENT_TAGGED_VERSION = re.compile(r'^\d+t?-[A-Z]+$')


def split_version(version_string):
    return version_string.strip().split('.')


# New versions of the Teradata RPM are of the form 0.148.t.x.y, so the 't'
# can be not attached to the preceding version number, and we want to
# preserve it.
def intOrT(x):
    try:
        return int(x)
    except ValueError as e:
        if x is 't':
            return x
        raise e


def strip_tag(version):
    """
    Strip trailing non-numeric components from a version leaving the Teradata
    't' on the final version component if it's present.
    ['1', '2', 'THREE'] -> (1, 2)
    ['1', 'TWO', '3'] -> raises a ValueError
    ['0', '115t', 'SNAPSHOT'] -> (0, '115t')
    ['ZERO', '123t'] -> raises a ValueError
    ['0', '148', 't'] => (0, 148, 't')
    ['0', '148', 't', 0, 1] => (0, 148, 't', 0, 1)
    ['0', '148', 't', 0, 1, 'SNAPSHOT'] => (0, 148, 't', 0, 1)

    This checks the components of the version from least to most significant.
    Tags are only allowed at the least significant place in a version number,
    i.e. as the right-most component.

    Anything that can't be parsed as an integer that isn't in the right-most
    position is considered an error.

    :param version: something that can be sliced
    :return: a tuple containing only integer components, except for possibly
             the last one, which will be a string iff it's an integer followed
             by the letter 't'
    """
    is_teradata = False
    is_ancient = False

    result = list(version[:])
    while True:
        try:
            rightmost = result[-1]
            intOrT(rightmost)
            # Once we find the right-most/least significant component that
            # can be represented as an int (doesn't raise a ValueError), break
            # out of the loop.
            break
        except ValueError:
            # Ancient tagged versions had the tag delimited by a - rather than
            # a ., spilt on -, and take the left-most token. The pattern
            # ensures that the component consists of numbers followed by a tag.
            # Once we've matched the pattern, we know the left-most token can
            # be converted by the int() function, and we're done removing
            # components.
            if ANCIENT_TAGGED_VERSION.match(rightmost):
                is_ancient = True
                result[-1] = rightmost.split('-')[0]

            # Do this second, and get the right-most component by index to get
            # the updated value for an ancient tagged version. If the pattern
            # matches, we know that except for the trailing t, the remainder of
            # the last component is a number int can parse.
            if TD_VERSION.match(result[-1]):
                is_teradata = True
                break

            # Non-teradata ancient tag. See above. We know this component is
            # numeric and we should break out of the loop and check the
            # components to the left of it.
            if is_ancient:
                break
            result = result[:-1]
        except IndexError:
            # If every component of the version has been removed because it's
            # non-numeric, we'll try to slice [][-1], and get an IndexError.
            # In that case, we've started with something that wasn't a version.
            raise ValueError(
                '%s does not contain any numeric version information' %
                (version,))

    # Verify that every component left of the right-most int() parseable
    # component is parseable by int(). For Teradata versions, preserve the
    # Teradata 't' on the final component.
    if is_teradata:
        result = [int(x) for x in result[:-1]] + [result[-1]]
    else:
        result = [intOrT(x) for x in result]

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
