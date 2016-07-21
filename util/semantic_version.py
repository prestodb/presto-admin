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
Module for parsing and processing semantic versions
"""
class SemanticVersion(object):
    def __init__(self, version):
        self.version = version
        version_fields = self.version.split('.')
        if len(version_fields) > 3:
            exit('Version %s has more than 3 fields' % self.version)
        self.major_version = self._get_version_field_value(version_fields, 0)
        self.minor_version = self._get_version_field_value(version_fields, 1)
        self.patch_version = self._get_version_field_value(version_fields, 2)

    def _get_version_field_value(self, version_fields, index):
        try:
            return int(version_fields[index])
        except IndexError:
            # The field value was omitted for the version
            return 0
        except ValueError:
            exit('Version %s has a non-numeric field' % self.version)

    def __lt__(self, other):
        if self.major_version == other.major_version:
            if self.minor_version == other.minor_version:
                return self.patch_version < other.patch_version
            else:
                return self.minor_version < other.minor_version
        else:
            return self.major_version < other.major_version

    def __eq__(self, other):
        return self.major_version == other.major_version and \
               self.minor_version == other.minor_version and \
               self.patch_version == other.patch_veresion

    def __str__(self):
        return self.version

    @staticmethod
    def _bump_version(version_field):
        return str(int(version_field) + 1)

    def _get_acceptable_major_version_bumps(self):
        acceptable_major = self._bump_version(self.major_version)
        return [acceptable_major,
                acceptable_major + '.0',
                acceptable_major + '.0.0']

    def _get_acceptable_minor_version_bumps(self):
        acceptable_minor = self._bump_version(self.minor_version)
        return [str(self.major_version) + '.' + acceptable_minor,
                str(self.major_version) + '.' + acceptable_minor + '.0']

    def _get_acceptable_patch_version_bumps(self):
        acceptable_patch = self._bump_version(self.patch_version)
        return [str(self.major_version) + '.' + str(self.minor_version) + '.' + acceptable_patch]

    def get_acceptable_version_bumps(self):
        """
        This functions takes as input strings major, minor, and patch which should be
        the corresponding semvar fields for a release. It returns a list of strings, which
        contains all acceptable versions. For each field bump, lower fields may be omitted
        or 0s. For instance, bumping 0.1.2's major version can result in 1, 1.0, or 1.0.0.
        """
        major_bumps = self._get_acceptable_major_version_bumps()
        minor_bumps = self._get_acceptable_minor_version_bumps()
        patch_bumps = self._get_acceptable_patch_version_bumps()
        return major_bumps + minor_bumps + patch_bumps
